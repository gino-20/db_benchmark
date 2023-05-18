from abc import ABC, abstractmethod
from pydantic import BaseModel
import contextlib
import uuid
from faker import Faker
import psycopg2
from psycopg2.extras import execute_batch
from random import choice

from config import pg_config

class Benchmark(ABC):
    @abstractmethod
    def __init__(self, data):
        self.data = data

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
    def __init__(self):
        super().__init__()
        self.PAGE_SIZE=5000
        with contextlib.closing(psycopg2.connect(**pg_config)) as conn, conn.cursor() as cur:
            create_table = "create table IF NOT EXISTS test (id uuid, name varchar(256), email varchar(256));"
            cur.execute(create_table)
            conn.commit()
        self.item = choice(self.data)
        self.data.remove(self.item)
        self.write_one()
        self.write_many()
        self.read_one()
        self.clean()

    def write_one(self):
        with contextlib.closing(psycopg2.connect(**pg_config)) as conn, conn.cursor() as cur:
            query = 'INSERT INTO test (id, name, email) VALUES (%s, %s, %s)'
            cur.execute(query, (self.item.id, self.item.name, self.item.email))
            conn.commit()

    def write_many(self):
        with contextlib.closing(psycopg2.connect(**pg_config)) as conn, conn.cursor() as cur:
            data_set = [(str(item.id), item.name, item.email) for item in self.data]
            query = 'INSERT INTO test (id, name, email) VALUES (%s, %s, %s)'
            execute_batch(cur, query, data_set, page_size=self.PAGE_SIZE)
            conn.commit()

    def read_one(self):
        with contextlib.closing(psycopg2.connect(**pg_config)) as conn, conn.cursor() as cur:
            query = f"select * from test where id = '{self.item.id}'"
            cur.execute(query)
            cur.fetchone()

    def clean(self):
        with contextlib.closing(psycopg2.connect(**pg_config)) as conn, conn.cursor() as cur:
            query = 'drop table test;'
            cur.execute(query)
