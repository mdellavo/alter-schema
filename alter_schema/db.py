import logging
from multiprocessing import Process, Queue

from sqlalchemy import create_engine, MetaData, Table, select, insert, and_

log = logging.getLogger("db-copy")


class CopyWorker(Process):
    def __init__(self, uri: str, table_name: str, copy_table_name: str, request_queue: Queue, completion_queue: Queue):
        super(CopyWorker, self).__init__()
        self.uri = uri
        self.table_name = table_name
        self.copy_table_name = copy_table_name
        self.request_queue = request_queue
        self.completion_queue = completion_queue

    def run(self) -> None:
        logging.basicConfig(level=logging.DEBUG)

        engine = create_engine(self.uri)
        metadata = MetaData()

        running = True
        with engine.connect() as conn:
            table = Table(self.table_name, metadata, autoload_with=engine)
            copy_table = Table(self.copy_table_name, metadata, autoload_with=engine)

            while running:
                page = self.request_queue.get()
                if not page:
                    running = False
                    break

                lower, upper = page
                lower_clause = and_(*[col >= val for col, val in zip(table.primary_key.columns, lower)]) if lower else None
                upper_clause = and_(*[col < val for col, val in zip(table.primary_key.columns, upper)]) if upper else None

                page_select = select(table).where(lower_clause, upper_clause)
                page_insert = insert(copy_table).from_select(copy_table.columns, page_select)
                conn.execute(page_insert)

                # log.info("copying page %s", page)
                self.completion_queue.put(page)
