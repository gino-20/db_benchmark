from pydantic import BaseModel
import contextlib
import uuid
from faker import Faker
import psycopg2
from psycopg2.extras import execute_batch

dsn = {
    'dbname': 'test',
    'user': 'test',
    'password': 'mysecretpassword',
    'host': 'localhost',
    'port': 5432,
}

PAGE_SIZE = 5000

class Test(BaseModel):
    id: uuid.UUID
    name: str
    email: str


def data_generator() -> list:
    fake = Faker()
    data_list = [Test(id=uuid.uuid4(), name=fake.name(), email=fake.email()) for _ in range(1000)]
    return data_list


def pg_tester(data):
    with contextlib.closing(psycopg2.connect(**dsn)) as conn, conn.cursor() as cur:
        create_table = "create table IF NOT EXISTS test (id uuid, name varchar(256), email varchar(256));"
        cur.execute(create_table)
        conn.commit()

        data_set = [(str(item.id), item.name, item.email) for item in data]
        query = 'INSERT INTO test (id, name, email) VALUES (%s, %s, %s)'
        execute_batch(cur, query, data_set, page_size=PAGE_SIZE)
        conn.commit()
