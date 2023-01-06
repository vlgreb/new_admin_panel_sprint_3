import logging
from datetime import datetime
from typing import Iterator

from sql import FILMWORKS_QUERY

BATCH_SIZE = 100


class PGExtractor:

    def __init__(self, cursor):
        self.cursor = cursor

    def extract_movies(self, date_last_modified: datetime) -> Iterator:
        """Extract data from Postgres"""
        self.cursor.execute(FILMWORKS_QUERY, (date_last_modified,) * 3)
        while rows := self.cursor.fetchmany(BATCH_SIZE):
            yield rows
