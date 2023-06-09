import abc
import dataclasses
import logging
import math
import multiprocessing
import threading
import random
import time
import queue

from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    select,
    and_,
    func,
    text,
    delete,
    tuple_,
)
from sqlalchemy.engine.base import Connection
from sqlalchemy.dialects.mysql import insert

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent
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
    return table + "_old"


def clone_table(metadata: MetaData, table: Table, copy_table_name: str):
    columns = []
    for column in table.columns:
        new_column = Column(
            column.name,
            column.type,
            primary_key=column.primary_key,
            autoincrement=column.autoincrement,
        )
        columns.append(new_column)

    copy_table = Table(copy_table_name, metadata, *columns)
    return copy_table


def swap_tables(conn: Connection, table: Table, copy_table: Table, comment=None):
    sql = f"RENAME TABLE {table.name} TO {table.name}_old, {copy_table.name} TO {table.name}"
    if comment:
        sql += " -- " + comment
    conn.execute(text(sql))


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
        return (
            f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.database}"
        )

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
        self.pages = int(math.ceil(self.count / self.batch_size))
        log.info(
            "table %s has %d total rows, %s pages",
            self.table.name,
            self.count,
            self.pages,
        )

    def __iter__(self):
        return self

    def __next__(self):
        if self.exhaused:
            raise StopIteration()

        pk_cols = self.table.primary_key.columns

        base_query = (
            select(pk_cols)
            .order_by(*[col.asc() for col in pk_cols.values()])
            .limit(self.batch_size)
        )
        if self.last_page:
            clauses = [col >= val for col, val in zip(pk_cols, self.last_page)]
            base_query = base_query.where(and_(*clauses))

        base_query = base_query.subquery()

        cols = [
            func.count("*").label("count"),
        ]
        cols.extend([func.min(col).label("min_" + col.name) for col in base_query.c])
        cols.extend([func.max(col).label("max_" + col.name) for col in base_query.c])

        query = select(*cols)
        result = first(self.conn.execute(query))

        lower = [getattr(result, "min_" + col.name) for col in base_query.c]
        upper = [getattr(result, "max_" + col.name) for col in base_query.c]

        if result.count < self.batch_size:
            self.exhaused = True

        self.last_page = upper

        return (lower, upper)


class CopyWorker(multiprocessing.Process):
    def __init__(
        self,
        config: DBConfig,
        request_queue: multiprocessing.Queue,
        completion_queue: multiprocessing.Queue,
    ):
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
                lower_clause = (
                    and_(
                        *[
                            col >= val
                            for col, val in zip(table.primary_key.columns, lower)
                        ]
                    )
                    if lower
                    else None
                )
                upper_clause = (
                    and_(
                        *[
                            col <= val
                            for col, val in zip(table.primary_key.columns, upper)
                        ]
                    )
                    if upper
                    else None
                )

                page_select = select(table).where(lower_clause, upper_clause)
                page_insert = insert(copy_table).from_select(
                    copy_table.columns, page_select
                )

                values = {col.key: table.c[col.key] for col in table.c}
                on_duplicate_key_update = page_insert.on_duplicate_key_update(**values)
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
        pk_where = [
            col.name + " = OLD." + col.name
            for col in self.table.primary_key.columns.values()
        ]

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

        log.info("triggers attached")

    def detach(self):
        for trigger_name in self.TRIGGER_NAMES:
            self.conn.execute(text("DROP TRIGGER " + trigger_name))
        log.info("triggers detached")


def replication_reader(event, stream: BinLogStreamReader, q: queue.Queue):
    while not event.is_set():
        try:
            ev = stream.fetchone()
            q.put(ev)
        except Exception:
            log.exception("error during replication reader")
            break


class ReplicationWorker(multiprocessing.Process):

    READ_TIMEOUT = 1

    def __init__(self, config: DBConfig, event):
        super(ReplicationWorker, self).__init__()
        self.config = config
        self.event = event

    def run(self):

        engine = create_engine(self.config.uri)
        metadata = MetaData()

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
                only_tables=[
                    self.config.table,
                ],
                only_events=[
                    QueryEvent,
                    UpdateRowsEvent,
                    WriteRowsEvent,
                    DeleteRowsEvent,
                ],
                skip_to_timestamp=time.time(),
            )

            reader_queue = queue.Queue()
            reader_thread = threading.Thread(
                target=replication_reader,
                args=(self.event, stream, reader_queue),
                daemon=True,
            )
            reader_thread.start()

            # FIXME this seems like it could drop events, make sure to drain
            while not self.event.is_set():
                try:
                    event = reader_queue.get(timeout=self.READ_TIMEOUT)
                except queue.Empty:
                    continue

                if type(event) == WriteRowsEvent:
                    self.handle_write_event(conn, copy_table, event)
                elif type(event) == UpdateRowsEvent:
                    self.handle_update_event(conn, copy_table, event)
                elif type(event) == DeleteRowsEvent:
                    self.handle_delete_event(conn, copy_table, event)

            stream.close()
            # FIXME this blocks :(
            # reader_thread.join()
            log.info("replication stream closed")

    def handle_write_event(self, conn: Connection, table: Table, event: WriteRowsEvent):
        values = [row["values"] for row in event.rows]
        query = insert(table).values(values)
        conn.execute(query)

    def handle_update_event(
        self, conn: Connection, table: Table, event: UpdateRowsEvent
    ):
        event.dump()

    def handle_delete_event(
        self, conn: Connection, table: Table, event: DeleteRowsEvent
    ):
        def make_value(row):
            return {
                column.name: row["values"][column.name]
                for column in table.primary_key.columns
            }

        values = [make_value(row) for row in event.rows]
        query = delete(table).where(tuple_(*table.primary_key.columns).in_(values))
        conn.execute(query)


class ReplicationMonitor(Monitor):
    def __init__(self, config: DBConfig):
        self.event = multiprocessing.Event()
        self.worker = ReplicationWorker(config, self.event)

    def attach(self):
        self.worker.start()
        log.info("replication attached")

    def detach(self):
        self.event.set()
        self.worker.join()
        log.info("replication detached")
