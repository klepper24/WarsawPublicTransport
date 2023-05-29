from typing import Collection, Iterable

import pymongo
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sql_alchemy_classes import Timetables
from orchestrator.dags import settings
from datetime import datetime
from sqlalchemy.sql import text
from sqlalchemy.exc import IntegrityError


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


def map_timetables_to_postgres_object(timetables_collection: Iterable, enum_start: int):
    for id_nr, timetable in enumerate(timetables_collection, start=enum_start):
        timetable = Timetables(id=id_nr, day_type=timetable["day_type"],
                               departure_time=timetable["departure_time"],
                               departure_time_sequence_nr=timetable["order"],
                               stop_id=int(str(timetable["unit"]) + timetable["post"]), line_nr=timetable["line"],
                               created_at=datetime.now())
        yield timetable


def send_timetables_to_postgres() -> [int, int]:
    timetables_collection = get_mongo_collection('Timetable')
    postgres_session = create_postgres_session(settings.POSTGRES_URL)

    rollbacked_documents = 0
    committed_documents = 0

    with postgres_session() as session:
        session.execute(text('TRUNCATE TABLE dbo.timetables CASCADE'))
        session.commit()

    pypeline = [
        {"$group": {
            "_id": {"line": "$line", "route": "$route", "day_type": "$day_type", "unit": "$unit", "post": "$post",
                    "departure_time": "$departure_time", "order": "$order"}
                    }
        }
    ]
    timetables_cursor_distinct = timetables_collection.aggregate(pypeline)
    timetables_list = list()
    for timetable in timetables_cursor_distinct:
        timetables_list.append(timetable["_id"])

    batch = 1000
    offset = 0
    while True:
        mapped_timetables = list(map_timetables_to_postgres_object(timetables_list[offset:batch+offset], offset+1))
        with postgres_session() as session:
            try:
                session.bulk_save_objects(mapped_timetables)
                session.commit()
                committed_documents += len(mapped_timetables)
            except IntegrityError:
                print("1st try: An IntegrityError has occurred")
                # do not commit this row
                session.rollback()
                for timetable in mapped_timetables:
                    session.add(timetable)
                    try:
                        session.commit()
                        committed_documents += 1
                    except IntegrityError:
                        print("2nd try: An IntegrityError has occurred")
                        # do not commit this row
                        session.rollback()
                        rollbacked_documents += 1
        if len(mapped_timetables) < batch:
            break

        offset = offset + batch

    return committed_documents, rollbacked_documents


if __name__ == "__main__":
    send_timetables_to_postgres()
