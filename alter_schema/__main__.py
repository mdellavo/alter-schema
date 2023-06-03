#!/bin/which python

import argparse

import getpass
import logging
from multiprocessing import Queue
import sys
import threading
import uuid

from sqlalchemy import create_engine, MetaData, Table, text

import tqdm

from .db import (
    DBConfig,
    CopyWorker,
    TablePageIterator,
    TriggerMonitor,
    ReplicationMonitor,
    clone_table,
    swap_tables,
)

WORKERS = 3

log = logging.getLogger("alter-schema")


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

    config = DBConfig.from_args(args)
    log.info("connecting to %s", config.uri)

    engine = create_engine(config.uri)
    metadata = MetaData()

    request_queue = Queue()
    completion_queue = Queue()
    workers = [CopyWorker(config, request_queue, completion_queue) for _ in range(WORKERS)]
    for worker in workers:
        worker.start()

    with engine.connect() as conn:
        table = Table(args.table, metadata, autoload_with=conn)
        log.info("primary key %s", table.primary_key)

        monitor = ReplicationMonitor(config)
        monitor.attach()

        copy_table = clone_table(metadata, table, config.copy_table)
        copy_table.drop(conn, checkfirst=True)
        copy_table.create(conn, checkfirst=True)

        log.info("Created copy table %s", config.copy_table)

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
            #print("page", i, page, page[1][0] - page[0][0] if page[0] is not None and page[1] is not None else "")
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
            old_table = Table(config.old_table, metadata)
            old_table.drop(engine, checkfirst=True)
            swap_tables(conn, table, copy_table)
            old_table.drop(engine, checkfirst=True)

        monitor.detach()

    return 0


if __name__ == "__main__":
    rv = main()
    sys.exit(rv)
