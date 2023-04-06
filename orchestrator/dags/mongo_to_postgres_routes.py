from datetime import datetime
import os
from typing import Collection

import pymongo
from sqlalchemy import create_engine
from sql_alchemy_classes import Routes
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

import settings


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


def send_routes_to_postgres() -> None:
    routes_collection = get_mongo_collection('Routes')
    agg_routes = routes_collection.aggregate(
    [
        {
            $unset: ["_id", "number_of_routes", "routes.stops"]
        },
        {
            $unwind: "$routes"
        }
    ])
    postgres_session = create_postgres_session(settings.POSTGRES_URL)

    with postgres_session() as session:
        session.execute('''TRUNCATE TABLE routes''')
        session.commit()
        id_nr = 1
        for doc in agg_routes:
            print(x)
            route = Routes(id=id_nr, line_name=doc["line_nr"] + doc["routes"]["route_nr"]
                          line_nr=doc["line_nr"], name=doc["routes"]["route_nr"],
                          stops_cnt=doc["routes"]["number_of_stops"],
                          created_at=datetime.now())
            session.add(route)
            try:
                session.commit()
            except IntegrityError:
                print("1st try: An IntegrityError has occurred")
                # do not commit this row
                session.rollback()
                continue
            finally:
                id_nr += 1


if __name__ == "__main__":
    send_routes_to_postgres()