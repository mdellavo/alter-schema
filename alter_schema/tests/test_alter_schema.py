import datetime
import random
import threading
import time
import uuid
from queue import Queue

import pymysql
import pytest
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, create_engine, insert, inspect, select

from alter_schema import get_command, parse_args
from alter_schema.db import DBConfig, MonitorTypes

pytest_plugins = ["docker_compose"]


class Config(DBConfig):
    @property
    def run_args(self):
        rv = [
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
            "-m",
            self.monitor,
            "-y",
            "--keep-old-table",
            "--no-progress",
        ]

        for alter in self.alter:
            rv.extend(
                [
                    "-a",
                    alter,
                ]
            )

        return rv


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
        keep_old_table=True,
        yes=True,
        alter=[
            "ADD COLUMN foo INT",
            "ADD COLUMN bar VARCHAR(100)",
            "ADD COLUMN baz DATETIME",
        ],
        monitor=MonitorTypes.REPLICATION.value,
    )

    return config


def test_no_such_table(test_db):

    metadata = MetaData()
    table = Table(test_db.table, metadata, Column("id", Integer, primary_key=True))

    engine = create_engine(test_db.uri, isolation_level="READ COMMITTED")
    with engine.connect() as conn:
        inspector = inspect(conn)
        assert not inspector.has_table(table.name)

    args = parse_args(test_db.run_args)
    command = get_command(args)
    rv = command.run(test_db)
    assert rv == 1


def _test_e2e(test_db):

    NUM_ROWS = 100_000

    metadata = MetaData()
    table = Table(
        test_db.table,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("num", Integer),
        Column("data", String(100)),
        Column("timestamp", DateTime),
    )

    copy_table = Table(test_db.copy_table, metadata, Column("id", Integer, primary_key=True))

    old_table = Table(test_db.old_table, metadata, Column("id", Integer, primary_key=True))

    def build_row():
        return {
            "num": random.randint(0, 1_000_000),
            "data": uuid.uuid4().hex,
            "timestamp": datetime.datetime.fromtimestamp(random.randrange(0, 2**32)),
        }

    values = [build_row() for _ in range(NUM_ROWS)]

    engine = create_engine(test_db.uri, isolation_level="READ COMMITTED")
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
        while not event.is_set():
            with create_engine(test_db.uri, isolation_level="READ COMMITTED").connect() as conn:
                table = Table(test_db.table, MetaData(), autoload_with=conn)
                values = build_row()
                result = conn.execute(insert(table), values)
                assert result.rowcount == 1
                conn.commit()

                for pk_col, pk_val in zip(table.primary_key, result.inserted_primary_key):
                    values[pk_col.name] = pk_val

                update_queue.put(values)
            time.sleep(0.01)

    updater_thread = threading.Thread(target=updater)
    updater_thread.start()

    args = parse_args(test_db.run_args)
    command = get_command(args)
    rv = command.run(args)

    event.set()
    updater_thread.join()

    assert rv == 0

    update_rows = []
    while not update_queue.empty():
        row = update_queue.get()
        update_rows.append(row)

    with engine.connect() as conn:
        after_rows = [row._mapping for row in conn.execute(select(table))]

        after_ids = {row["id"] for row in after_rows}
        before_ids = {row["id"] for row in before_rows}
        update_ids = {row["id"] for row in update_rows}

        print("len before=", len(before_ids))
        print("len after=", len(after_ids))
        print("len update=", len(update_ids))

        print("missing", update_ids - after_ids)

        assert after_ids.issuperset(before_ids)
        assert after_ids.issuperset(update_ids)
        assert after_ids - before_ids == update_ids

        inspector = inspect(conn)
        assert inspector.has_table(table.name)
        assert inspector.has_table(old_table.name)
        assert not inspector.has_table(copy_table.name)

        columns = inspector.get_columns(table.name)
        print(columns)
        col_map = {column["name"]: column for column in columns}
        new_cols = ("foo", "bar", "baz")
        for col_name in new_cols:
            assert col_name in col_map


def test_e2e_replication(test_db):
    test_db.monitor = MonitorTypes.REPLICATION.value
    _test_e2e(test_db)


def test_e2e_trigger(test_db):
    test_db.monitor = MonitorTypes.TRIGGER.value
    _test_e2e(test_db)
