#!/usr/bin/env python3

import datetime
import uuid
import random

from sqlalchemy import create_engine, MetaData, Table, insert

URI = "mysql+pymysql://test:test@localhost/test"

def build_row():
    return {
        "num": random.randint(0, 1_000_000),
        "data": uuid.uuid4().hex,
        "timestamp": datetime.datetime.fromtimestamp(random.randrange(0, 2**32))
    }


values = [build_row() for _ in range(1_000_000)]

engine = create_engine(URI)
metadata = MetaData()
with engine.connect() as conn:
    table = Table("test_data", metadata, autoload_with=conn)
    conn.execute(insert(table), values)
