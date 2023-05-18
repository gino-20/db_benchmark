from pydantic import BaseModel
import uuid
from faker import Faker
from tqdm import tqdm


from config import data_range
from classes import PG_benchmark, ELK_benchmark, Mongo_benchmark, Clickhouse_benchmark

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
    data_list = [Test(id=uuid.uuid4(), name=fake.name(), email=fake.email()) for _ in tqdm(range(data_range))]
    return data_list


if __name__ == '__main__':
    print(f'Benchmarking DBs with the dataset of {data_range} items\n')
    ds = data_generator()
    PG_benchmark(ds)

    ELK_benchmark(ds)

    Mongo_benchmark(ds)

    Clickhouse_benchmark(ds)
