from datetime import datetime
from typing import Collection

import pymongo
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

import settings
from sql_alchemy_classes import RouteVariants, Stops, Routes


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


def map_route_variants_to_postgres_object(postgres_session: object, route_variants: object, enum_start: int) -> object:
    for id_nr, doc in enumerate(route_variants, start=1+enum_start):
        print(doc)
        unit = str(doc["routes"]["stops"]["stop_nr"])[:4]
        post = str(doc["routes"]["stops"]["stop_nr"])[4:]
        r_name = doc["routes"]["route_nr"]
        stop_sequence_nr = doc["stop_sequence_nr"]
        with postgres_session() as session:
            stmt_stop = select(Stops.id).where(Stops.unit == unit, Stops.post == post)
            stop_id = session.scalars(stmt_stop).first()

            stmt_route = select(Routes.id).where(Routes.name == r_name)
            route_id = session.scalars(stmt_route).first()

        route_variant = RouteVariants(id=id_nr, stop_id=stop_id, route_id=route_id, stop_sequence_nr=stop_sequence_nr)
        yield route_variant


def send_route_variants_to_postgres() -> None:
    postgres_session = create_postgres_session(settings.POSTGRES_URL)
    with postgres_session() as session:
        session.execute(text('TRUNCATE TABLE dbo.route_variants CASCADE'))
        session.commit()
    limit = 30
    offset = 0
    routes_collection = get_mongo_collection('Routes')
    while True:
        pipeline = [
            {"$unset": ["_id", "number_of_routes", "routes.number_of_stops", "routes.stops.stop_name", "routes.stops.street", "routes.stops.r_flag", "routes.stops.min_time", "routes.stops.max_time"]},
            {"$unwind": "$routes"},
            {"$unwind": "$routes.stops"},
            {
                "$setWindowFields": {
                    "partitionBy": {"line_nr": "$line_nr", "route_nr": "$routes.route_nr"},
                    "sortBy": {"quantity": 1},
                    "output": {
                        "stop_sequence_nr": {
                            "$documentNumber": {}
                        }
                    }
                }
            },
            {"$skip": offset},
            {"$limit": limit}
        ]
        agg_route_variants = routes_collection.aggregate(pipeline)
        mapped_route_variants = list(map_route_variants_to_postgres_object(postgres_session, agg_route_variants, offset))
        with postgres_session() as session:
            try:
                session.bulk_save_objects(mapped_route_variants)
                session.commit()
            except IntegrityError:
                print("1st try: An IntegrityError has occurred")
                # do not commit this row
                session.rollback()
                for route in mapped_route_variants:
                    session.add(route)
                    try:
                        session.commit()
                    except IntegrityError:
                        print("2nd try: An IntegrityError has occurred")
                        # do not commit this row
                        session.rollback()
        if len(mapped_route_variants) < limit:
            break
        offset = offset + limit


if __name__ == "__main__":
    send_route_variants_to_postgres()
