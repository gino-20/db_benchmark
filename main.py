from pydantic import BaseModel
from typing import List
import uuid
from faker import Faker
from tqdm import tqdm
from random import randint, random

from classes import PG_benchmark, ELK_benchmark, Mongo_benchmark, Clickhouse_benchmark

from config import numer_of_subfields, number_of_reads, number_of_threads

dsn = {
    'dbname': 'test',
    'user': 'test',
    'password': 'mysecretpassword',
    'host': 'localhost',
    'port': 5432,
}

PAGE_SIZE = 5000


class Test(BaseModel):
    user_id: uuid.UUID
    likes: List[uuid.UUID] = []
    dislikes: List[uuid.UUID] = []
    bookmarks: List[uuid.UUID] = []
    score: float = 0


def data_generator() -> list:
    fake = Faker()
    # The data structure is made with the idea, that everything is based on the id of user
    data_list = [
        Test(user_id=uuid.uuid4(),
             likes=[uuid.uuid4() for _ in range(randint(0, numer_of_subfields))],
             dislikes=[uuid.uuid4() for _ in range(randint(0, numer_of_subfields))],
             bookmarks=[uuid.uuid4() for _ in range(randint(0, numer_of_subfields))],
             score=round(random()*10, 1))
        for _ in tqdm(range(data_range))
    ]
    return data_list


if __name__ == '__main__':
    try:
        data_range = int(input('Enter desired dataset size (default is 100): '))
    except ValueError:
        print('Only integer value supported, using default')
        data_range = 100
    print(f'\nBenchmarking DBs with the dataset of '
          f'{data_range} items, '
          f'{numer_of_subfields} subfields, '
          f'{number_of_threads} threads\n')
    ds = data_generator()

    PG_benchmark(ds)

    ELK_benchmark(ds)

    Mongo_benchmark(ds)

    Clickhouse_benchmark(ds)
