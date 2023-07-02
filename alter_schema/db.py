import abc
import enum
import dataclasses
import logging
import math
import multiprocessing
import multiprocessing.synchronize
import threading
import random
import time
import queue
from typing import Optional

import retry

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
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.engine.base import Connection
from sqlalchemy.dialects.mysql import insert

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)
from sqlalchemy.sql.ddl import DDL


log = logging.getLogger()

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


@retry.retry(exceptions=(OperationalError,), tries=3, delay=2)
def swap_tables(conn: Connection, table: Table, copy_table: Table):
    sql = f"RENAME TABLE {table.name} TO {table.name}_old, {copy_table.name} TO {table.name};"
    conn.execute(text("SET lock_wait_timeout=5"))
    conn.execute(DDL(sql))


def first(xs):
    for x in xs:
        return x


def scalar(xs):
    result = first(xs)
    return result[0] if result else result


class MonitorTypes(enum.Enum):
    TRIGGER = "trigger"
    REPLICATION = "replication"


@dataclasses.dataclass
class DBConfig:
    user: str
    password: str
    host: str
    database: str
    port: str
    table: str
    alter: list[str] = dataclasses.field(default_factory=list)
    yes: bool = False
    keep_old_table: bool = False
    monitor: Optional[MonitorTypes] = None

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
            alter=args.alter,
            yes=args.yes,
            keep_old_table=args.keep_old_table,
        )


@dataclasses.dataclass
class TablePage:
    count: int
    lower: tuple
    upper: tuple


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

        lower = tuple(getattr(result, "min_" + col.name) for col in base_query.c)
        upper = tuple(getattr(result, "max_" + col.name) for col in base_query.c)

        if result.count < self.batch_size:
            self.exhaused = True

        self.last_page = upper

        return TablePage(result.count, lower, upper)


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
        engine = create_engine(self.config.uri, isolation_level="READ COMMITTED")
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

                lower_clause = (
                    and_(
                        *[
                            col >= val
                            for col, val in zip(table.primary_key.columns, page.lower)
                        ]
                    )
                    if page.lower
                    else None
                )
                upper_clause = (
                    and_(
                        *[
                            col <= val
                            for col, val in zip(table.primary_key.columns, page.upper)
                        ]
                    )
                    if page.upper
                    else None
                )

                base_query = select(table).where(lower_clause, upper_clause)
                query = (
                    insert(copy_table)
                    .prefix_with("IGNORE")
                    .from_select(base_query.c, base_query)
                )

                conn.execute(query)
                conn.commit()

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
        REPLACE INTO {copy_table_name} ({cols}) VALUES ({new_values});
      END
    """

    UPDATE_TRIGGER_SQL = """
    CREATE TRIGGER schema_alter_monitor_update AFTER UPDATE ON {table_name}
      FOR EACH ROW
      BEGIN
        REPLACE INTO {copy_table_name} ({cols}) VALUES ({new_values});
      END
    """

    DELETE_TRIGGER_SQL = """
    CREATE TRIGGER schema_alter_monitor_delete AFTER DELETE ON {table_name}
      FOR EACH ROW
        DELETE FROM {copy_table_name} WHERE {pk_where}
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
            table_name=self.config.table,
            copy_table_name=self.config.copy_table,
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


def replication_reader(
    event: multiprocessing.synchronize.Event, config: DBConfig, q: queue.Queue
):

    server_id = random.randint(0, 1_000_000)
    stream = BinLogStreamReader(
        connection_settings={
            "host": config.host,
            "port": int(config.port),
            "user": config.user,
            "passwd": config.password,
        },
        server_id=server_id,
        blocking=True,
        skip_to_timestamp=time.time(),
    )

    while not event.is_set():
        try:
            ev = stream.fetchone()
            q.put(ev)
        except Exception:
            log.exception("error during replication reader")
            break

    stream.close()


class ReplicationWorker(multiprocessing.Process):

    READ_TIMEOUT = 0.1

    def __init__(self, config: DBConfig, event: multiprocessing.synchronize.Event):
        super(ReplicationWorker, self).__init__()
        self.config = config
        self.event = event

    def run(self):

        engine = create_engine(self.config.uri, isolation_level="READ COMMITTED")
        metadata = MetaData()

        switched = False
        with engine.connect() as conn:
            table = Table(self.config.table, metadata, autoload_with=engine)
            copy_table = Table(self.config.copy_table, metadata, autoload_with=engine)

            def _target_table():
                return table if switched else copy_table

            reader_queue = queue.Queue()
            reader_thread = threading.Thread(
                target=replication_reader,
                args=(self.event, self.config, reader_queue),
            )
            reader_thread.start()

            while True:
                try:
                    event = reader_queue.get(timeout=self.READ_TIMEOUT)
                except queue.Empty:
                    if self.event.is_set():
                        break
                    continue

                try:
                    self.handle_event(conn, _target_table(), event)
                except ProgrammingError as e:
                    log.debug("checking for table switch")
                    if str(e.orig.args[0]) == "1146":
                        switched = True
                        log.info("table switches")
                        time.sleep(0.02)
                        self.handle_event(conn, _target_table(), event)

            reader_thread.join()
            log.info("replication stream closed")

    def handle_event(self, conn: Connection, table: Table, event):
        if type(event) == WriteRowsEvent:
            self.handle_write_event(conn, table, event)
        elif type(event) == UpdateRowsEvent:
            self.handle_update_event(conn, table, event)
        elif type(event) == DeleteRowsEvent:
            self.handle_delete_event(conn, table, event)

    def handle_write_event(self, conn: Connection, table: Table, event: WriteRowsEvent):
        if not event.rows:
            return
        values = [row["values"] for row in event.rows]
        query = insert(table).prefix_with("IGNORE").values(values)
        result = conn.execute(query)
        conn.commit()
        log.info("inserted %d rows", result.rowcount)

    def handle_update_event(
        self, conn: Connection, table: Table, event: UpdateRowsEvent
    ):
        event.dump()

    def handle_delete_event(
        self, conn: Connection, table: Table, event: DeleteRowsEvent
    ):
        if not event.rows:
            return

        def make_value(row):
            return {
                column.name: row["values"][column.name]
                for column in table.primary_key.columns
            }

        values = [make_value(row) for row in event.rows]
        query = delete(table).where(tuple_(*table.primary_key.columns).in_(values))
        result = conn.execute(query)
        conn.commit()
        log.info("deleted %d rows", result.rowcount)


class ReplicationMonitor(Monitor):
    def __init__(self, config: DBConfig):
        self.event = multiprocessing.Event()
        self.worker = ReplicationWorker(config, self.event)

    def attach(self):
        self.worker.start()
        log.info("replication attached")

    def detach(self):
        log.info("waiting for replication to detach...")
        self.event.set()
        self.worker.join()
        log.info("replication detached")
