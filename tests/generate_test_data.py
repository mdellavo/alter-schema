#!/usr/bin/env python3

import datetime
import uuid
import random

from sqlalchemy import create_engine, text

URI = "mysql+pymysql://test:test@localhost/test"

engine = create_engine(URI)

def build_row():
    return {
        "num": random.randint(0, 1_000_000),
        "data": uuid.uuid4().hex,
        "timestamp": datetime.datetime.fromtimestamp(random.randrange(0, 2**32))
    }


values = [build_row() for _ in range(100_000)]


with engine.connect() as conn:
    conn.execute(text("USE test"))
    conn.execute(text("INSERT INTO test_data(num, data, timestamp) VALUES (:num, :data, :timestamp)"), values)
