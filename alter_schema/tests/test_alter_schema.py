import datetime
import random
from unittest import mock
import uuid
import time

import pytest
import pymysql

from alter_schema.db import DBConfig
from alter_schema.command import Command

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
    inspect
)


pytest_plugins = ["docker_compose"]

class Config(DBConfig):
    @property
    def run_args(self):
        return [
            "-H", self.host,
            "-u", self.user,
            "-p", self.password,
            "-d", self.database,
            "-t", self.table,
        ]



@pytest.fixture(scope="function")
def test_db(request, module_scoped_container_getter):

    while True:
        try:
            connection = pymysql.connect(
                host='localhost',
                user='test',
                password='test',
                database='test',
            )
            connection.close()
            break
        except pymysql.Error:
            time.sleep(1)

    table_name = request.function.__name__

    config = Config(
        user="test",
        password="test",
        host="localhost",
        database="test",
        port="3306",
        table=table_name,
    )

    return config


def test_no_such_table(test_db):

    metadata = MetaData()
    table = Table(test_db.table, metadata,
                  Column("id", Integer, primary_key=True))

    engine = create_engine(test_db.uri)
    with engine.connect() as conn:
        inspector = inspect(conn)
        assert not inspector.has_table(table.name)

    rv = Command().run(test_db.run_args)
    assert rv == 1


@mock.patch("alter_schema.command.confirm")
def test_e2e(mock_confirm, test_db):

    NUM_ROWS = 100_000

    metadata = MetaData()
    table = Table(test_db.table, metadata,
                  Column("id", Integer, primary_key=True),
                  Column("num", Integer),
                  Column("data", String(100)),
                  Column("timestamp", DateTime))

    copy_table = Table(test_db.copy_table, metadata,
                       Column("id", Integer, primary_key=True))

    old_table = Table(test_db.old_table, metadata,
                      Column("id", Integer, primary_key=True))

    def build_row():
        return {
            "num": random.randint(0, 1_000_000),
            "data": uuid.uuid4().hex,
            "timestamp": datetime.datetime.fromtimestamp(random.randrange(0, 2**32))
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

        conn.execute(insert(table), values)

        before_rows = [row._mapping for row in conn.execute(select(table))]
        assert len(before_rows) == NUM_ROWS

    rv = Command().run(test_db.run_args)
    assert rv == 0
    assert mock_confirm.called

    with engine.connect() as conn:
        after_rows = [row._mapping for row in conn.execute(select(table))]
        assert len(after_rows) == len(before_rows)
        assert after_rows == before_rows

        inspector = inspect(conn)
        assert inspector.has_table(table.name)
        assert not inspector.has_table(old_table.name)
        assert not inspector.has_table(copy_table.name)
