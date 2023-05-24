#!/bin/which python

import argparse
import getpass
import logging
from multiprocessing import Queue
import sys
import threading

from sqlalchemy import create_engine, MetaData, Table, Column, select, func, and_
from sqlalchemy.engine.base import Connection

import tqdm

from .db import CopyWorker

BATCH_SIZE = 5000
WORKERS = 10

log = logging.getLogger("alter-schema")


def uri_from_args(args):
    return f"mysql+pymysql://{args.user}:{args.password}@{args.host}/{args.database}"


def parse_args():
    parser = argparse.ArgumentParser(
        prog="alter-schema"
    )
    parser.add_argument("-H", "--host", required=True)
    parser.add_argument("-u", "--user", default="root")
    parser.add_argument("-p", "--password", default=None,  nargs='?')
    parser.add_argument("-P", "--port", default=3306)
    parser.add_argument("-d", "--database", required=True)
    parser.add_argument("-t", "--table", required=True)
    args = parser.parse_args()

    if args.password is None:
        args.password = getpass.getpass()

    return args


def first(xs):
    for x in xs:
        return x


def scalar(xs):
    result = first(xs)
    return result[0] if result else result


class TablePageIterator:

    def __init__(self, conn: Connection, table: Table, batch_size=BATCH_SIZE) -> None:
        self.conn = conn
        self.table = table
        self.batch_size = batch_size

        self.last_page = None

        stmt = select(func.count("*")).select_from(self.table)
        self.count = scalar(self.conn.execute(stmt))
        log.info("table %s has %d total rows", self.table.name, self.count)

    def __iter__(self):
        return self

    def __next__(self):
        pk_cols = self.table.primary_key.columns

        stmt = select(pk_cols)
        if self.last_page:
            clauses = [col > val for col, val in zip(pk_cols, self.last_page)]
            stmt = stmt.where(and_(*clauses))

        stmt = stmt.limit(1).offset(self.batch_size)

        next_page = first(self.conn.execute(stmt))
        if next_page is None:
            raise StopIteration()

        rv = (self.last_page, next_page)
        self.last_page = next_page
        return rv


def main():

    logging.basicConfig(level=logging.DEBUG)

    args = parse_args()
    uri = uri_from_args(args)
    log.info("connecting to %s", uri)

    engine = create_engine(uri)
    metadata = MetaData()

    request_queue = Queue()
    completion_queue = Queue()

    copy_table_name = args.table + "_new"
    workers = [CopyWorker(uri, args.table, copy_table_name, request_queue, completion_queue) for _ in range(WORKERS)]
    for worker in workers:
        worker.start()

    with engine.connect() as conn:
        table = Table(args.table, metadata, autoload_with=engine)
        log.info("primary key %s", table.primary_key)

        columns = []
        for column in table.columns:
            new_column = Column(column.name, column.type, primary_key=column.primary_key, autoincrement=column.autoincrement)
            columns.append(new_column)

        copy_table = Table(copy_table_name, metadata, *columns)

        copy_table.drop(engine, checkfirst=True)
        copy_table.create(engine, checkfirst=True)

        log.info("Created copy table %s", copy_table_name)

        table_iterator = TablePageIterator(conn, table)
        total = table_iterator.count / table_iterator.batch_size

        pbar = tqdm.tqdm(total=total)

        def on_completion():
            running = True
            while running:
                page = completion_queue.get()
                if not page:
                    running = False
                    break

                pbar.update(1)

        completion_thread = threading.Thread(target=on_completion, daemon=True)
        completion_thread.start()

        for page in table_iterator:
            request_queue.put(page)

    for _ in workers:
        request_queue.put(None)
    for worker in workers:
        worker.join()

    completion_queue.put(None)
    completion_thread.join()

    # Swap table
    line = None
    while not line:
        line = input("Swap table? [Y/n]").strip() or "Y"
        if line == "Y":
            print("swap")
        if line:
            break

    return 0


if __name__ == "__main__":
    rv = main()
    sys.exit(rv)
