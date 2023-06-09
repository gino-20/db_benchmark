from abc import ABC, abstractmethod
import contextlib
import psycopg2
from psycopg2.extras import execute_batch
from random import choice
import time
import elasticsearch
from elasticsearch.helpers import bulk
import pymongo
import numpy
import clickhouse_connect

from config import pg_config, elk_url, elk_index, mongo_url, clickhouse_dsl


class Benchmark(ABC):
    @abstractmethod
    def __init__(self, data):
        self.data = data
        self.item = choice(self.data)
        self.data.remove(self.item)

    @abstractmethod
    def _timer(func):
        def timer_wrapper(self):
            start_time = time.perf_counter()
            func(self)
            end_time = time.perf_counter()
            total_time = end_time - start_time
            print(f'Measure of {func.__name__} Took {total_time:.4f} seconds')
        return timer_wrapper

    @abstractmethod
    def write_one(self):
        pass

    @abstractmethod
    def write_many(self):
        pass

    @abstractmethod
    def read_one(self):
        pass

    @abstractmethod
    def clean(self):
        pass


class PG_benchmark(Benchmark):
    def __init__(self, data):
        super().__init__(data)
        print('Testing Postgres')
        self.PAGE_SIZE = 5000
        with contextlib.closing(psycopg2.connect(**pg_config)) as conn, conn.cursor() as cur:
            create_table = "create table IF NOT EXISTS test (id uuid, name varchar(256), email varchar(256));"
            cur.execute(create_table)
            conn.commit()
        self.write_one()
        self.write_many()
        self.read_one()
        self.clean()

    def _timer(func):
        def timer_wrapper(self):
            start_time = time.perf_counter()
            func(self)
            end_time = time.perf_counter()
            total_time = end_time - start_time
            print(f'Measure of {func.__name__} Took {total_time:.4f} seconds')
        return timer_wrapper

    @_timer
    def write_one(self):
        with contextlib.closing(psycopg2.connect(**pg_config)) as conn, conn.cursor() as cur:
            query = 'INSERT INTO test (id, name, email) VALUES (%s, %s, %s)'
            cur.execute(query, (str(self.item.id), self.item.name, self.item.email))
            conn.commit()

    @_timer
    def write_many(self):
        with contextlib.closing(psycopg2.connect(**pg_config)) as conn, conn.cursor() as cur:
            data_set = [(str(item.id), item.name, item.email) for item in self.data]
            query = 'INSERT INTO test (id, name, email) VALUES (%s, %s, %s)'
            execute_batch(cur, query, data_set, page_size=self.PAGE_SIZE)
            conn.commit()

    @_timer
    def read_one(self):
        with contextlib.closing(psycopg2.connect(**pg_config)) as conn, conn.cursor() as cur:
            query = f"select * from test where id = '{self.item.id}'"
            cur.execute(query)
            cur.fetchone()

    def clean(self):
        with contextlib.closing(psycopg2.connect(**pg_config)) as conn, conn.cursor() as cur:
            query = 'drop table test;'
            cur.execute(query)


class ELK_benchmark(Benchmark):
    def __init__(self, data):
        super().__init__(data)
        print('Testing ELK')
        es = elasticsearch.Elasticsearch(elk_url)
        if not es.indices.exists('test'):
            es.indices.create('test', elk_index)
        self.write_one()
        self.write_many()
        self.read_one()
        self.clean()

    def _timer(func):
        def timer_wrapper(self):
            start_time = time.perf_counter()
            func(self)
            end_time = time.perf_counter()
            total_time = end_time - start_time
            print(f'Measure of {func.__name__} Took {total_time:.4f} seconds')

        return timer_wrapper

    def elk_iterator(self, data_set):
        """Prepare ELK data chunks to write to the Elastic DB"""
        for data in data_set:
            yield {
                '_index': 'test',
                '_id': data.id,
                'id': data.id,
                'name': data.name,
                'email': data.email
            }

    @_timer
    def write_many(self):
        es = elasticsearch.Elasticsearch(elk_url)
        bulk(es, self.elk_iterator(self.data), ignore=[400, 404])

    @_timer
    def write_one(self):
        es = elasticsearch.Elasticsearch(elk_url)
        data = self.item
        document = {
                'id': data.id,
                'name': data.name,
                'email': data.email
            }
        es.index(index='test', id=data.id, body=document)

    @_timer
    def read_one(self):
        es = elasticsearch.Elasticsearch(elk_url)
        es.get(index='test', id=self.item.id)

    def clean(self):
        es = elasticsearch.Elasticsearch(elk_url)
        es.indices.delete(index='test', ignore=[400, 404])

class Mongo_benchmark(Benchmark):
    def __init__(self, data):
        super().__init__(data)
        print('Testing Mongo')
        self.write_one()
        self.data = [dict(item) for item in data]
        self.write_many()
        self.read_one()
        self.clean()

    def _timer(func):
        def timer_wrapper(self):
            start_time = time.perf_counter()
            func(self)
            end_time = time.perf_counter()
            total_time = end_time - start_time
            print(f'Measure of {func.__name__} Took {total_time:.4f} seconds')

        return timer_wrapper

    @_timer
    def write_one(self):
        mng = pymongo.MongoClient(mongo_url, uuidRepresentation='standard')
        mng_db = mng["test"]
        mng_col = mng_db["test"]
        mng_col.insert_one(dict(self.item))
    @_timer
    def write_many(self):
        mng = pymongo.MongoClient(mongo_url, uuidRepresentation='standard')
        mng_db = mng["test"]
        mng_col = mng_db["test"]
        mng_col.insert_many(self.data)

    @_timer
    def read_one(self):
        mng = pymongo.MongoClient(mongo_url, uuidRepresentation='standard')
        mng_db = mng["test"]
        mng_col = mng_db["test"]
        mng_col.find({'id': self.item.id})

    def clean(self):
        mng = pymongo.MongoClient(mongo_url, uuidRepresentation='standard')
        mng_db = mng["test"]
        mng_col = mng_db["test"]
        mng_col.drop()


class Clickhouse_benchmark(Benchmark):
    def __init__(self, data):
        super().__init__(data)
        print('Testing Clickhouse')
        self.write_one()
        self.write_many()
        self.read_one()
        self.clean()

    def _timer(func):
        def timer_wrapper(self):
            start_time = time.perf_counter()
            func(self)
            end_time = time.perf_counter()
            total_time = end_time - start_time
            print(f'Measure of {func.__name__} Took {total_time:.4f} seconds')

        return timer_wrapper

    @_timer
    def write_one(self):
        cl = clickhouse_connect.get_client(**clickhouse_dsl)
        data = [list(dict(self.item).values())]
        cl.command('CREATE TABLE IF NOT EXISTS test (id UUID, name String, email String) ENGINE MergeTree ORDER BY id')
        cl.insert('test', data, column_names=['id', 'name', 'email'])

    @_timer
    def write_many(self):
        cl = clickhouse_connect.get_client(**clickhouse_dsl)
        cl.command('CREATE TABLE IF NOT EXISTS test (id UUID, name String, email String) ENGINE MergeTree ORDER BY id')
        data_list = [[item.id, item.name, item.email] for item in self.data]
        cl.insert('test', data_list, column_names=['id', 'name', 'email'])

    @_timer
    def read_one(self):
        cl = clickhouse_connect.get_client(**clickhouse_dsl)
        cl.query(f'SELECT COLUMNS("id") FROM test')

    def clean(self):
        cl = clickhouse_connect.get_client(**clickhouse_dsl)
        cl.command('DROP TABLE test')
