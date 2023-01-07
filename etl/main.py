import json
import logging
import os
from time import sleep

import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from psycopg2.extras import DictCursor
from redis import Redis
from elasticsearch import Elasticsearch

from config import DSL
from extract import PGExtractor
from load import ElasticsearchLoader
from state import RedisStorage, State
from transform import DataTransform
from helpers import backoff

load_dotenv()

INTERVAL = 1


@backoff()
def connect_postgres():
    return psycopg2.connect(**DSL, cursor_factory=DictCursor)


@backoff()
def connect_redis_storage():
    host_redis = os.environ.get('REDIS_HOST')
    redis_adapter = Redis(host=host_redis,
                          charset="utf-8",
                          decode_responses=True)
    return RedisStorage(redis_adapter=redis_adapter)


@backoff()
def es_conn():
    return Elasticsearch(hosts=os.environ.get("ELASTIC_HOST"))


@backoff()
def create_index(es_client):
    if not es_client.indices.exists(index="movies"):
        schema = open("elastic_schema.json", "r")
        data = json.loads(schema.read())
        es_client.indices.create(index='movies', body=data)


if __name__ == '__main__':

    while True:

        with connect_postgres() as pg_conn:
            storage = connect_redis_storage()
            state = State(storage)
            last_modified = state.get_state("modified")
            date_last_modified = last_modified if last_modified else datetime.min
            cursor = pg_conn.cursor()

            extractor = PGExtractor(cursor)
            transformer = DataTransform()
            film_generator = extractor.extract_movies(date_last_modified)
            es = es_conn()
            create_index(es)
            loader = ElasticsearchLoader(es)
            for films in film_generator:
                transform_data = transformer.validate_and_transform_data(films)
                loader.load_data_to_elastic(transform_data)
                state.set_state("modified", datetime.now().isoformat())

        pg_conn.close()
        sleep(INTERVAL)
