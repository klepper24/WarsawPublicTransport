from typing import Collection

import pymongo
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from sql_alchemy_classes import Timetables, Stops
from sqlalchemy.exc import IntegrityError

import settings


def _get_mongo_url() -> str:
    return f'{settings.MONGO_URL}.?authSource=admin'


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


def send_stops_to_postgres() -> [int, int]:
    stops_collection = get_mongo_collection('Stops')
    postgres_session = create_postgres_session(settings.POSTGRES_URL)
    clean_stops_unit(stops_collection)

    with postgres_session() as session:
        rollbacked_documents = 0
        committed_documents = 0
        for x in stops_collection.find({}):
            stop = Stops(id=int(x["unit"] + x["post"]), full_name=x["unit_name"],
                         stop_longitude=x["longitude"], stop_latitude=x["latitude"],
                         street=x["direction"], unit=x["unit"], post=x["post"], is_depot=0,
                         created_at=datetime.now())
            session.add(stop)
            try:
                session.commit()
                committed_documents += 1
            except IntegrityError:
                print("1st try: An IntegrityError has occurred")
                # do not commit this row
                session.rollback()
                rollbacked_documents += 1
                continue
    return committed_documents, rollbacked_documents


if __name__ == "__main__":
    send_stops_to_postgres()

