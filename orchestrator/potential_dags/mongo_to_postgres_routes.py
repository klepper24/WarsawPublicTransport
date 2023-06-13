from datetime import datetime
from typing import Collection

import pymongo
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

import settings
from sql_alchemy_classes import Routes


def _get_mongo_url() -> str:
    print(settings.MONGO_URL)
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


def map_routes_to_postgres_object(routes: object, enum_start: int) -> object:
    for id_nr, doc in enumerate(routes, start=1+enum_start):
        route = Routes(id=id_nr, line_name=doc["line_nr"] + doc["routes"]["route_nr"],
                       line_nr=doc["line_nr"], name=doc["routes"]["route_nr"],
                       stops_cnt=doc["routes"]["number_of_stops"],
                       created_at=datetime.now())
        yield route


def send_routes_to_postgres() -> None:
    postgres_session = create_postgres_session(settings.POSTGRES_URL)
    with postgres_session() as session:
        session.execute(text('TRUNCATE TABLE dbo.routes CASCADE'))
        session.commit()
    limit = 10
    offset = 0
    routes_collection = get_mongo_collection('Routes')
    while True:
        pipeline = [
            {"$unset": ["_id", "number_of_routes", "routes.stops"]},
            {"$unwind": "$routes"},
            {"$skip": offset},
            {"$limit": limit}
        ]
        agg_routes = routes_collection.aggregate(pipeline)
        mapped_routes = list(map_routes_to_postgres_object(agg_routes, offset))
        with postgres_session() as session:
            try:
                session.bulk_save_objects(mapped_routes)
                session.commit()
            except IntegrityError:
                print("1st try: An IntegrityError has occurred")
                # do not commit this row
                session.rollback()
                for route in mapped_routes:
                    session.add(route)
                    try:
                        session.commit()
                    except IntegrityError:
                        print("2nd try: An IntegrityError has occurred")
                        # do not commit this row
                        session.rollback()
        if len(mapped_routes) < limit:
            break
        offset = offset + limit


if __name__ == "__main__":
    send_routes_to_postgres()
