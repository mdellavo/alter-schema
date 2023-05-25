#!/bin/which python

import argparse
import getpass
import logging
from multiprocessing import Queue
import sys
import threading

from sqlalchemy import create_engine, MetaData, Table

import tqdm

from .db import CopyWorker, TablePageIterator, clone_table, swap_tables

WORKERS = 3

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


def confirm(prompt):
    line = None
    while not line:
        line = input(prompt) or "Y"
        if line:
            return line.strip().upper() == "Y"


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
    old_table_name = args.table + "_old"

    workers = [CopyWorker(uri, args.table, copy_table_name, request_queue, completion_queue) for _ in range(WORKERS)]
    for worker in workers:
        worker.start()

    with engine.connect() as conn:
        table = Table(args.table, metadata, autoload_with=conn)
        log.info("primary key %s", table.primary_key)

        copy_table = clone_table(metadata, table, copy_table_name)
        copy_table.drop(conn, checkfirst=True)
        copy_table.create(conn, checkfirst=True)

        log.info("Created copy table %s", copy_table_name)

        table_iterator = TablePageIterator(conn, table)

        pbar = tqdm.tqdm(total=table_iterator.count, unit="rows")

        def on_completion():
            running = True
            while running:
                page = completion_queue.get()
                if not page:
                    running = False
                    break

                pbar.update(table_iterator.batch_size)

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

        pbar.close()

        # Swap table
        if confirm("Swap table? [Y/n] "):
            old_table = Table(old_table_name, metadata)
            old_table.drop(engine, checkfirst=True)

            swap_tables(conn, table, copy_table)

            old_table.drop(engine, checkfirst=True)

    return 0


if __name__ == "__main__":
    rv = main()
    sys.exit(rv)
