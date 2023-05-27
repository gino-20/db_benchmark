import contextlib
import time
from abc import ABC, abstractmethod
from multiprocessing import Pool
from random import choice, sample

import clickhouse_connect
import elasticsearch
import psycopg2
import pymongo
from elasticsearch.helpers import bulk
from psycopg2.extras import execute_batch

from config import (clickhouse_dsl, elk_index, elk_url, mongo_url,
                    number_of_reads, number_of_threads, pg_config)


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
            print(f"Measure of {func.__name__} Took {total_time:.4f} seconds")

        return timer_wrapper

    @abstractmethod
    def test_iterator(self, mp=True):
        db_read_sp = 0
        db_read_mp = 0
        self.write_many()
        self.write_one()
        print("Starting read tests...")
        print("Single process multiple read test...")
        start_time = time.perf_counter()
        for _ in range(number_of_reads):
            self.read_one()
        end_time = time.perf_counter()
        db_read_sp = (end_time - start_time) / number_of_reads
        if mp:
            print("Multiprocess multiple read test...")
            temp_dataset = sample(self.data, number_of_reads)
            id_list = [one_id.user_id for one_id in temp_dataset]
            start_time = time.perf_counter()
            with Pool(processes=number_of_threads) as p:
                p.map(self.read_one, id_list)
            end_time = time.perf_counter()
            db_read_mp = (end_time - start_time) / number_of_reads
        self.clean()
        print(
            f"\nDatabase read results:\n\t Single process: "
            f"{db_read_sp:.4f}\t Multi process: {db_read_mp:.4f}"
        )

    @abstractmethod
    def write_one(self):
        pass

    @abstractmethod
    def write_many(self):
        pass

    @abstractmethod
    def read_one(self, search_id: int):
        pass

    @abstractmethod
    def clean(self):
        pass


class PG_benchmark(Benchmark):
    def __init__(self, data):
        super().__init__(data)
        print("Testing Postgres")
        self.PAGE_SIZE = 5000
        psycopg2.extras.register_uuid()
        with contextlib.closing(
            psycopg2.connect(**pg_config)
        ) as conn, conn.cursor() as cur:
            create_table = (
                "create table IF NOT EXISTS test ("
                "user_id uuid, "
                "likes uuid[],"
                "dislikes uuid[],"
                "bookmarks uuid[],"
                "score float(1));"
            )
            cur.execute(create_table)
            conn.commit()
            self.test_iterator()

    def _timer(func):
        def timer_wrapper(self):
            start_time = time.perf_counter()
            func(self)
            end_time = time.perf_counter()
            total_time = end_time - start_time
            print(f"Measure of {func.__name__} Took {total_time:.4f} seconds")

        return timer_wrapper

    def test_iterator(self):
        super().test_iterator()

    @_timer
    def write_one(self):
        with contextlib.closing(
            psycopg2.connect(**pg_config)
        ) as conn, conn.cursor() as cur:
            query = "INSERT INTO test " \
                    "(user_id, likes, dislikes, bookmarks, score) " \
                    "VALUES (%s, %s, %s, %s, %s)"
            cur.execute(
                query,
                (
                    self.item.user_id,
                    self.item.likes,
                    self.item.dislikes,
                    self.item.bookmarks,
                    self.item.score,
                ),
            )
            conn.commit()

    @_timer
    def write_many(self):
        print("Populating DB with the whole dataset...")
        with contextlib.closing(
            psycopg2.connect(**pg_config)
        ) as conn, conn.cursor() as cur:
            data_set = [
                (item.user_id, item.likes, item.dislikes,
                 item.bookmarks, item.score)
                for item in self.data
            ]
            query = "INSERT INTO test " \
                    "(user_id, likes, dislikes, bookmarks, score) " \
                    "VALUES (%s, %s, %s, %s, %s)"
            execute_batch(cur, query, data_set, page_size=self.PAGE_SIZE)
            conn.commit()

    def read_one(self, search_id=0):
        if search_id == 0:
            search_id = self.item.user_id
        with contextlib.closing(
            psycopg2.connect(**pg_config)
        ) as conn, conn.cursor() as cur:
            query = f"select * from test where user_id = '{search_id}'"
            cur.execute(query)
            cur.fetchone()

    def clean(self):
        with contextlib.closing(
            psycopg2.connect(**pg_config)
        ) as conn, conn.cursor() as cur:
            query = "drop table test;"
            cur.execute(query)


class ELK_benchmark(Benchmark):
    def __init__(self, data):
        super().__init__(data)
        print("Testing ELK")
        es = elasticsearch.Elasticsearch(elk_url)
        if not es.indices.exists("test"):
            es.indices.create("test", elk_index)
        self.test_iterator()

    def test_iterator(self):
        super().test_iterator()

    def _timer(func):
        def timer_wrapper(self):
            start_time = time.perf_counter()
            func(self)
            end_time = time.perf_counter()
            total_time = end_time - start_time
            print(f"Measure of {func.__name__} Took {total_time:.4f} seconds")

        return timer_wrapper

    def elk_iterator(self, data_set):
        """Prepare ELK data chunks to write to the Elastic DB."""
        for data in data_set:
            yield {
                "_index": "test",
                "_id": data.user_id,
                "user_id": data.user_id,
                "likes": data.likes,
                "dislikes": data.dislikes,
                "score": data.score,
            }

    @_timer
    def write_many(self):
        print("Populating DB with the whole dataset...")
        es = elasticsearch.Elasticsearch(
            elk_url, timeout=30, max_retries=10, retry_on_timeout=True
        )
        bulk(es, self.elk_iterator(self.data), ignore=[400, 404])

    @_timer
    def write_one(self):
        es = elasticsearch.Elasticsearch(elk_url)
        data = self.item
        document = {
            "user_id": data.user_id,
            "likes": data.likes,
            "dislikes": data.dislikes,
            "bookmarks": data.bookmarks,
            "score": data.score,
        }
        es.index(index="test", id=data.user_id, body=document)

    def read_one(self, search_id=0):
        if search_id == 0:
            search_id = self.item.user_id
        es = elasticsearch.Elasticsearch(elk_url)
        es.get(index="test", id=search_id)

    def clean(self):
        es = elasticsearch.Elasticsearch(elk_url)
        es.indices.delete(index="test", ignore=[400, 404])


class Mongo_benchmark(Benchmark):
    def __init__(self, data):
        super().__init__(data)
        print("Testing Mongo")
        self.test_iterator()

    def test_iterator(self):
        super().test_iterator()

    def _timer(func):
        def timer_wrapper(self):
            start_time = time.perf_counter()
            func(self)
            end_time = time.perf_counter()
            total_time = end_time - start_time
            print(f"Measure of {func.__name__} Took {total_time:.4f} seconds")

        return timer_wrapper

    @_timer
    def write_one(self):
        mng = pymongo.MongoClient(mongo_url, uuidRepresentation="standard")
        mng_db = mng["test"]
        mng_col = mng_db["test"]
        mng_col.insert_one(dict(self.item))

    @_timer
    def write_many(self):
        data = [dict(item) for item in self.data]
        print("Populating DB with the whole dataset...")
        mng = pymongo.MongoClient(mongo_url, uuidRepresentation="standard")
        mng_db = mng["test"]
        mng_col = mng_db["test"]
        mng_col.insert_many(data)

    def read_one(self, search_id=0):
        if search_id == 0:
            search_id = self.item.user_id
        mng = pymongo.MongoClient(mongo_url, uuidRepresentation="standard")
        mng_db = mng["test"]
        mng_col = mng_db["test"]
        mng_col.find({"id": search_id})

    def clean(self):
        mng = pymongo.MongoClient(mongo_url, uuidRepresentation="standard")
        mng_db = mng["test"]
        mng_col = mng_db["test"]
        mng_col.drop()


class Clickhouse_benchmark(Benchmark):
    def __init__(self, data):
        super().__init__(data)
        print("Testing Clickhouse")
        self.test_iterator()

    def test_iterator(self):
        super().test_iterator(False)

    def _timer(func):
        def timer_wrapper(self):
            start_time = time.perf_counter()
            func(self)
            end_time = time.perf_counter()
            total_time = end_time - start_time
            print(f"Measure of {func.__name__} Took {total_time:.4f} seconds")

        return timer_wrapper

    @_timer
    def write_one(self):
        cl = clickhouse_connect.get_client(**clickhouse_dsl)
        data = [list(dict(self.item).values())]
        cl.command(
            "CREATE TABLE IF NOT EXISTS test "
            "(user_id UUID, likes Array(UUID), "
            "dislikes Array(UUID), bookmarks Array(UUID), "
            "score Float32) "
            "ENGINE MergeTree ORDER BY user_id"
        )
        cl.insert(
            "test",
            data,
            column_names=[
                "user_id", "likes", "dislikes", "bookmarks", "score"
            ],
        )

    @_timer
    def write_many(self):
        print("Populating DB with the whole dataset...")
        cl = clickhouse_connect.get_client(**clickhouse_dsl)
        cl.command(
            "CREATE TABLE IF NOT EXISTS test "
            "(user_id UUID, likes Array(UUID), "
            "dislikes Array(UUID), bookmarks Array(UUID), "
            "score Float32) "
            "ENGINE MergeTree ORDER BY user_id"
        )
        data_list = [
            [item.user_id, item.likes, item.dislikes,
             item.bookmarks, item.score]
            for item in self.data
        ]
        cl.insert(
            "test",
            data_list,
            column_names=["user_id", "likes", "dislikes",
                          "bookmarks", "score"],
        )

    def read_one(self, search_id=0):
        cl = clickhouse_connect.get_client(**clickhouse_dsl)
        cl.query('SELECT COLUMNS("user_id") FROM test')

    def clean(self):
        cl = clickhouse_connect.get_client(**clickhouse_dsl)
        cl.command("DROP TABLE test")
