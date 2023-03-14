import os
import pymongo
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from sql_alchemy_classes import Timetables, Stops
from sqlalchemy.exc import IntegrityError
from typing import Collection


# temporary solution
MONGO_HOST = os.getenv('MONGO_HOST', default='localhost')
MONGO_USER = os.getenv('MONGO_USER', default='root')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', default='pass12345')
MONGO_PORT = os.getenv('MONGO_PORT', default='27017')
MONGO_URL = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
POSTGRES_URL = "postgresql://my_user:password123@localhost:5432/WarsawTransportState"


def _get_mongo_url() -> str:
    return f'{MONGO_URL}.?authSource=admin'


def get_mongo_collection(collection_name: str) -> Collection:
    endpoint = _get_mongo_url()
    client = pymongo.MongoClient(endpoint)
    db = client['WarsawPublicTransport']
    mongo_collection = db[collection_name]
    return mongo_collection


def create_postgres_session(postgres_url: str):
    source_engine = create_engine(postgres_url, echo=True)
    return sessionmaker(source_engine)


def clean_stops_unit(mongo_collection: Collection) -> None:
    # units which start with 'R'
    query = {"unit": {"$regex": "^R"}}
    x = mongo_collection.delete_many(query)
    print(x.deleted_count, " documents deleted.")


def send_stops_to_postgres() -> None:
    stops_collection = get_mongo_collection('Stops')
    postgres_session = create_postgres_session(POSTGRES_URL)
    clean_stops_unit(stops_collection)

    with postgres_session() as session:
        # exclude columns: _id, street_id, valid_from
        for x in stops_collection.find({}, {"_id": 0, "street_id": 0, "valid_from": 0}):
            print(x)
            stops = Stops(id=int(x["unit"] + x["post"]), full_name=x["unit_name"],
                          stop_longitude=x["longitude"], stop_latitude=x["latitude"],
                          street=x["direction"], unit=x["unit"], post=x["post"], is_depot=1,
                          created_at=datetime.now())
            session.add(stops)
            try:
                session.commit()
            except IntegrityError:
                print("1st try: An IntegrityError has occurred")
                # do not commit this row
                session.rollback()
                continue


if __name__ == "__main__":
    send_stops_to_postgres()
