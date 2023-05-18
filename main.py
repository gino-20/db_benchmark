from pydantic import BaseModel
import uuid
from faker import Faker
import psycopg2


class Test(BaseModel):
    id: uuid.UUID
    name: str
    email: str


def data_generator() -> list:
    fake = Faker()
    data_list = [Test(id=uuid.uuid4(), name=fake.name(), email=fake.email()) for i in range(1000)]
    return data_list

def pg_tester():
    conn = psycopg2.connect("dbname=test user=test password=mysecretpassword host=localhost")