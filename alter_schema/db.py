import abc
import dataclasses
import logging
from multiprocessing import Process, Queue
import random
import time

from sqlalchemy import create_engine, MetaData, Table, Column, select, and_, func, text
from sqlalchemy.engine.base import Connection
from sqlalchemy.dialects.mysql import insert

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)


log = logging.getLogger("db-copy")

BATCH_SIZE = 10000

def make_copy_table_name(table: str):
    return table + "_new"

def make_old_table_name(table: str):
    return table + "old"

def clone_table(metadata: MetaData, table: Table, copy_table_name: str):
    columns = []
    for column in table.columns:
        new_column = Column(column.name, column.type, primary_key=column.primary_key, autoincrement=column.autoincrement)
        columns.append(new_column)

    copy_table = Table(copy_table_name, metadata, *columns)
    return copy_table


def swap_tables(conn: Connection, table: Table, copy_table: Table):
    conn.execute(text(f"RENAME TABLE {table.name} TO {table.name}_old, {copy_table.name} TO {table.name}", bind=conn))


def first(xs):
    for x in xs:
        return x


def scalar(xs):
    result = first(xs)
    return result[0] if result else result


@dataclasses.dataclass
class DBConfig:
    user: str
    password: str
    host: str
    database: str
    port: str
    table: str

    @property
    def uri(self):
        return f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.database}"

    @property
    def as_dict(self):
        return dataclasses.asdict(self)

    @property
    def copy_table(self):
        return make_copy_table_name(self.table)

    @property
    def old_table(self):
        return make_old_table_name(self.table)

    @classmethod
    def from_args(cls, args):
        return cls(
            args.user,
            args.password,
            args.host,
            args.database,
            args.port,
            args.table,
        )


class TablePageIterator:

    def __init__(self, conn: Connection, table: Table, batch_size=BATCH_SIZE) -> None:
        self.conn = conn
        self.table = table
        self.batch_size = batch_size
        self.last_page = None
        self.exhaused = False

        stmt = select(func.count("*")).select_from(self.table)
        self.count = scalar(self.conn.execute(stmt))
        log.info("table %s has %d total rows, %s pages", self.table.name, self.count, self.count / self.batch_size)

    def __iter__(self):
        return self

    def __next__(self):
        if self.exhaused:
            raise StopIteration()

        pk_cols = self.table.primary_key.columns

        stmt = select(pk_cols)
        if self.last_page:
            clauses = [col >= val for col, val in zip(pk_cols, self.last_page)]
            stmt = stmt.where(and_(*clauses))

        stmt = stmt.limit(1).offset(self.batch_size).order_by(*[col.asc() for col in pk_cols.values()])
        next_page = first(self.conn.execute(stmt))
        if next_page is None:
            self.exhaused = True

        rv = (self.last_page, next_page)
        self.last_page = next_page
        return rv


class CopyWorker(Process):
    def __init__(self, config: DBConfig, request_queue: Queue, completion_queue: Queue):
        super(CopyWorker, self).__init__()
        self.config = config
        self.request_queue = request_queue
        self.completion_queue = completion_queue

    def run(self) -> None:
        engine = create_engine(self.config.uri)
        metadata = MetaData()

        running = True
        with engine.connect() as conn:
            table = Table(self.config.table, metadata, autoload_with=engine)
            copy_table = Table(self.config.copy_table, metadata, autoload_with=engine)

            while running:
                page = self.request_queue.get()
                if not page:
                    running = False
                    break

                lower, upper = page
                lower_clause = and_(*[col >= val for col, val in zip(table.primary_key.columns, lower)]) if lower else None
                upper_clause = and_(*[col < val for col, val in zip(table.primary_key.columns, upper)]) if upper else None

                page_select = select(table).where(lower_clause, upper_clause)
                page_insert = insert(copy_table).from_select(copy_table.columns, page_select)

                values = {col.key: table.c[col.key] for col in table.c}
                on_duplicate_key_update = page_insert.on_duplicate_key_update(
                    **values
                )
                conn.execute(on_duplicate_key_update)

                # log.info("copying page %s", page)
                self.completion_queue.put(page)


class Monitor(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def attach(self):
        pass

    @abc.abstractmethod
    def detach(self):
        pass


class TriggerMonitor(Monitor):

    INSERT_TRIGGER_SQL = """
    CREATE TRIGGER schema_alter_monitor_insert AFTER INSERT ON {table_name}
      FOR EACH ROW
      BEGIN
        REPLACE INTO {table_name} ({cols}) VALUES ({new_values});
      END
    """

    UPDATE_TRIGGER_SQL = """
    CREATE TRIGGER schema_alter_monitor_update AFTER UPDATE ON {table_name}
      FOR EACH ROW
      BEGIN
        REPLACE INTO {table_name} ({cols}) VALUES ({new_values});
      END
    """

    DELETE_TRIGGER_SQL = """
    CREATE TRIGGER schema_alter_monitor_delete AFTER DELETE ON {table_name}
      FOR EACH ROW
        DELETE FROM {table_name} WHERE {pk_where}
    """

    TRIGGER_NAMES = [
        "schema_alter_monitor_insert",
        "schema_alter_monitor_update",
        "schema_alter_monitor_delete",
    ]

    def __init__(self, config: DBConfig, conn: Connection, table: Table):
        self.config = config
        self.conn = conn
        self.table = table

    def _build(self, template: str):
        cols = [col.name for col in self.table.c.values()]
        new_values = ["NEW." + col.name for col in self.table.c.values()]
        pk_where = [col.name + " = OLD." + col.name for col in self.table.primary_key.columns.values()]

        return template.format(
            table_name=self.table.name,
            cols=", ".join(cols),
            new_values=", ".join(new_values),
            pk_where=" AND ".join(pk_where),
        )

    def attach(self):
        insert_trigger = self._build(self.INSERT_TRIGGER_SQL)
        update_trigger = self._build(self.UPDATE_TRIGGER_SQL)
        delete_trigger = self._build(self.DELETE_TRIGGER_SQL)

        for trigger in [insert_trigger, update_trigger, delete_trigger]:
            self.conn.execute(text(trigger))

    def detach(self):
        for trigger_name in self.TRIGGER_NAMES:
            self.conn.execute(text("DROP TRIGGER " + trigger_name))


class ReplicationWorker(Process):
    def __init__(self, config: DBConfig):
        super(ReplicationWorker, self).__init__()
        self.config = config

    def run(self):

        engine = create_engine(self.config.uri)
        metadata = MetaData()

        running = True
        with engine.connect() as conn:
            table = Table(self.config.table, metadata, autoload_with=engine)
            copy_table = Table(self.config.copy_table, metadata, autoload_with=engine)

            server_id = random.randint(0, 1_000_000)
            stream = BinLogStreamReader(
                connection_settings={
                    "host": self.config.host,
                    "port": int(self.config.port),
                    "user": self.config.user,
                    "passwd": self.config.password,
                },
                server_id=server_id,
                blocking=True,
                only_events=[
                    UpdateRowsEvent,
                    WriteRowsEvent,
                    DeleteRowsEvent,
                ],
                only_tables=[
                    self.config.table,
                ],
                skip_to_timestamp=time.time(),
            )

            for event in stream:
                pass

            stream.close()


class ReplicationMonitor(Monitor):

    def __init__(self, config: DBConfig):
        self.worker = ReplicationWorker(config)

    def attach(self):
        self.worker.start()

    def detach(self):
        self.worker.terminate()
        self.worker.close()
