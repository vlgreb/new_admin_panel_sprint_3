from helpers import backoff

from elasticsearch.helpers import bulk

from models import ESFilmworkData


class ElasticsearchLoader:
    """A class to get data and load in Elasticsearch."""

    def __init__(self, connection) -> None:
        self.connection = connection

    @backoff()
    def load_data_to_elastic(self, data: list[ESFilmworkData]) -> None:
        documents = [
            {"_index": "movies", "_id": row.id, "_source": row.json()}
            for row in data
        ]
        bulk(self.connection, documents)
