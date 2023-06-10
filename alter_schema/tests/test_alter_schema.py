import datetime
from queue import Queue
import random
import threading
import time
from unittest import mock
import uuid

import pymysql
import pytest

from alter_schema.command import BasicCommand
from alter_schema.db import DBConfig

from sqlalchemy import (
    Table,
    MetaData,
    Column,
    Integer,
    String,
    DateTime,
    insert,
    select,
    create_engine,
    inspect,
)


pytest_plugins = ["docker_compose"]


class Config(DBConfig):
    @property
    def run_args(self):
        return [
            "-H",
            self.host,
            "-u",
            self.user,
            "-p",
            self.password,
            "-d",
            self.database,
            "-t",
            self.table,
        ]


@pytest.fixture(scope="function")
def test_db(request, module_scoped_container_getter):

    while True:
        try:
            connection = pymysql.connect(
                host="localhost",
                user="root",
                password="root",
                database="test",
            )
            connection.close()
            break
        except pymysql.Error:
            time.sleep(1)

    table_name = request.function.__name__

    config = Config(
        user="root",
        password="root",
        host="localhost",
        database="test",
        port="3306",
        table=table_name,
    )

    return config


def test_no_such_table(test_db):

    metadata = MetaData()
    table = Table(test_db.table, metadata, Column("id", Integer, primary_key=True))

    engine = create_engine(test_db.uri)
    with engine.connect() as conn:
        inspector = inspect(conn)
        assert not inspector.has_table(table.name)

    rv = BasicCommand().run(test_db.run_args)
    assert rv == 1


@mock.patch("alter_schema.command.confirm")
def test_e2e(mock_confirm, test_db):

    NUM_ROWS = 100

    metadata = MetaData()
    table = Table(
        test_db.table,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("num", Integer),
        Column("data", String(100)),
        Column("timestamp", DateTime),
    )

    copy_table = Table(
        test_db.copy_table, metadata, Column("id", Integer, primary_key=True)
    )

    old_table = Table(
        test_db.old_table, metadata, Column("id", Integer, primary_key=True)
    )

    def build_row():
        return {
            "num": random.randint(0, 1_000_000),
            "data": uuid.uuid4().hex,
            "timestamp": datetime.datetime.fromtimestamp(random.randrange(0, 2**32)),
        }

    values = [build_row() for _ in range(NUM_ROWS)]

    engine = create_engine(test_db.uri)
    with engine.connect() as conn:
        metadata.drop_all(bind=conn)
        metadata.create_all(bind=conn)

        inspector = inspect(conn)
        assert inspector.has_table(table.name)
        assert inspector.has_table(old_table.name)
        assert inspector.has_table(copy_table.name)

        result = conn.execute(insert(table), values)
        assert result.rowcount == NUM_ROWS

        before_rows = [row._mapping for row in conn.execute(select(table))]
        assert len(before_rows) == NUM_ROWS
        conn.commit()

    event = threading.Event()
    update_queue = Queue()

    def updater():
        with create_engine(test_db.uri).connect() as conn:
            table = Table(test_db.table, MetaData(), autoload_with=conn)

            while not event.is_set():
                values = build_row()
                result = conn.execute(insert(table), values)
                conn.commit()

                for pk_col, pk_val in zip(
                    table.primary_key, result.inserted_primary_key
                ):
                    values[pk_col.name] = pk_val

                update_queue.put(values)
                time.sleep(0.01)

    updater_thread = threading.Thread(target=updater)
    updater_thread.start()

    rv = BasicCommand().run(test_db.run_args)

    event.set()
    updater_thread.join()

    assert rv == 0
    assert mock_confirm.called

    update_rows = []
    while not update_queue.empty():
        row = update_queue.get()
        update_rows.append(row)

    with engine.connect() as conn:
        after_rows = [row._mapping for row in conn.execute(select(table))]

        after_ids = {row["id"] for row in after_rows}
        before_ids = {row["id"] for row in before_rows}
        update_ids = {row["id"] for row in update_rows}

        print("diff", after_ids - before_ids)
        print("update", update_ids)

        assert after_ids.issuperset(before_ids)
        assert after_ids.issuperset(update_ids)
        assert after_ids - before_ids == update_ids

        inspector = inspect(conn)
        assert inspector.has_table(table.name)
        assert not inspector.has_table(old_table.name)
        assert not inspector.has_table(copy_table.name)
