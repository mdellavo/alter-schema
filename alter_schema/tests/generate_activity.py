#!/usr/bin/env python3

import datetime
import random
import time
import uuid

from sqlalchemy import create_engine, Table, MetaData, insert

URI = "mysql+pymysql://test:test@localhost/test"

engine = create_engine(URI)
metadata = MetaData()


def build_row():
    return {
        "num": random.randint(0, 1_000_000),
        "data": uuid.uuid4().hex,
        "timestamp": datetime.datetime.fromtimestamp(random.randrange(0, 2**32))
    }


with engine.connect() as conn:

    table = Table("test_data", metadata, autoload_with=conn)

    while True:
        time.sleep(.01)
        values = [build_row() for _ in range(random.randint(0, 100))]
        conn.execute(insert(table), values)
        print("inserted", len(values), "events")
