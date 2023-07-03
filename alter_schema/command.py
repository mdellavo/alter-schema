import abc
import argparse
import getpass
import logging
import threading
from argparse import Namespace
from multiprocessing import Queue
from typing import Optional

import tqdm
from sqlalchemy import DDL, MetaData, Table, create_engine
from sqlalchemy.exc import NoSuchTableError
from tqdm.contrib.logging import logging_redirect_tqdm

from .db import (
    CopyWorker,
    DBConfig,
    MonitorTypes,
    ReplicationMonitor,
    TablePageIterator,
    TriggerMonitor,
    clone_table,
    swap_tables,
)

WORKERS = 3

log = logging.getLogger("alter-schema")


def confirm(prompt: str):
    line = None
    while not line:
        line = input(prompt) or "Y"
        if line:
            return line.strip().upper() == "Y"


class Args(Namespace):
    host: str
    user: str
    password: Optional[str]
    port: int
    database: str
    table: str
    yes: bool
    alter: list[str]
    yes: bool
    keep_old_table: bool
    monitor: Optional[MonitorTypes]
    progress: bool


COMMANDS = {}


class Command(metaclass=abc.ABCMeta):
    NAME: str

    def __init_subclass__(cls, **kwargs):
        COMMANDS[cls.NAME] = cls

    def run(self, args: Args) -> int:
        # noinspection PyBroadException
        try:
            rv = self.main(args)
        except Exception:
            log.exception("main raised an unhandled exception")
            rv = 1

        return rv

    @abc.abstractmethod
    def main(self, args: Args) -> int:
        pass


class BasicCommand(Command):
    NAME = "basic"

    def __init__(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format="[%(asctime)s] %(levelname)s - %(message)s",
        )

    def on_table_count(self, count):
        log.info("%d rows to copy", count)

    def on_page_copied(self, page):
        log.info("page copied: %s", page)

    def on_copy_complete(self):
        log.info("copy complete")

    def main(self, args: Args):
        config = DBConfig.from_args(args)
        log.info("connecting to %s", config.uri)

        engine = create_engine(config.uri, isolation_level="READ COMMITTED")
        metadata = MetaData()

        with engine.connect() as conn:

            try:
                table = Table(args.table, metadata, autoload_with=conn)
            except NoSuchTableError:
                log.error("Table %s does not exist", args.table)
                return 1

            log.info("primary key %s", table.primary_key)

            copy_table = clone_table(metadata, table, config.copy_table)
            copy_table.drop(conn, checkfirst=True)
            copy_table.create(conn, checkfirst=True)

            log.info("Created copy table %s", config.copy_table)

            if args.monitor:
                monitor_type = MonitorTypes(args.monitor)
            else:
                monitor_type = MonitorTypes.REPLICATION
            if monitor_type == MonitorTypes.TRIGGER:
                monitor = TriggerMonitor(config, conn, table)
            else:
                monitor = ReplicationMonitor(config)

            log.info("using %s monitor", args.monitor)

            monitor.attach()

            table_iterator = TablePageIterator(conn, table)
            self.on_table_count(table_iterator.count)

            request_queue = Queue()
            completion_queue = Queue()
            workers = [CopyWorker(config, request_queue, completion_queue) for _ in range(WORKERS)]
            for worker in workers:
                worker.start()

            def on_completion():
                while True:
                    page = completion_queue.get()
                    if not page:
                        break

                    self.on_page_copied(page)

            completion_thread = threading.Thread(target=on_completion, daemon=True)
            completion_thread.start()

            for i, page in enumerate(table_iterator):
                request_queue.put(page)

            for _ in workers:
                request_queue.put(None)

            for worker in workers:
                worker.join()

            completion_queue.put(None)
            completion_thread.join()

            self.on_copy_complete()

            for alter in config.alter:
                sql = f"ALTER TABLE {config.copy_table} " + alter
                log.info("APPLYING: %s", sql)
                conn.execute(DDL(sql))

            # Swap table
            if config.yes or confirm("Swap table? [Y/n] "):
                old_table = Table(config.old_table, metadata)
                old_table.drop(engine, checkfirst=True)
                swap_tables(conn, table, copy_table)
                log.info(
                    "swapped %s to %s, %s to %s",
                    config.table,
                    config.old_table,
                    config.copy_table,
                    config.table,
                )
                if not config.keep_old_table:
                    old_table.drop(engine, checkfirst=True)
                    log.info("dropped old table %s", config.old_table)

            monitor.detach()

        return 0


class FancyCommand(BasicCommand):
    NAME = "fancy"

    def __init__(self):
        super(FancyCommand, self).__init__()
        self.pbar = None

    def on_table_count(self, count):
        super().on_table_count(count)
        self.pbar = tqdm.tqdm(total=count, unit="rows")

    def on_page_copied(self, page):
        super(FancyCommand, self).on_page_copied(page)

        if self.pbar:
            self.pbar.update(page.count)

    def on_copy_complete(self):
        super(FancyCommand, self).on_copy_complete()
        if self.pbar:
            self.pbar.close()

    def run(self, args):
        with logging_redirect_tqdm():
            return super(FancyCommand, self).run(args)


def parse_args(args: list[str]) -> Args:
    parser = argparse.ArgumentParser(prog="alter-schema")
    parser.add_argument("-H", "--host", required=True)
    parser.add_argument("-u", "--user", default="root")
    parser.add_argument("-p", "--password", default=None, nargs="?")
    parser.add_argument("-P", "--port", default=3306)
    parser.add_argument("-d", "--database", required=True)
    parser.add_argument("-t", "--table", required=True)
    parser.add_argument("-y", "--yes", action="store_true")
    parser.add_argument("-a", "--alter", action="append", default=[])
    parser.add_argument("--keep-old-table", action="store_true")
    parser.add_argument("--progress", action="store_true", dest="progress", default=True)
    parser.add_argument("--no-progress", action="store_false", dest="progress")
    parser.add_argument(
        "-m",
        "--monitor",
        choices=[monitor.value for monitor in MonitorTypes],
        default=MonitorTypes.REPLICATION.value,
    )
    args = parser.parse_args(args=args)

    if args.password is None:
        args.password = getpass.getpass()

    return args


def get_command(args: Args) -> Command:
    if args.progress:
        return FancyCommand()
    return BasicCommand()
