from typing import Collection

import pymongo
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sql_alchemy_classes import Timetables
from orchestrator.dags import settings
from datetime import datetime
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


def send_timetables_to_postgres() -> [int, int]:
    stops_collection = get_mongo_collection('Timetable')
    postgres_session = create_postgres_session(settings.POSTGRES_URL)
    # clean_stops_unit(stops_collection)

    with postgres_session() as session:
        rollbacked_documents = 0
        committed_documents = 0
        id = 0
        for x in stops_collection.find({}):
            id += 1
            timetable = Timetables(id=id, day_type=x["day_type"],
                                   departure_time=x["departure_time"], departure_time_sequence_nr=x["order"],
                                   stop_id=int(str(x["unit"]) + x["post"]), line_nr=x["line"],
                                   created_at=datetime.now())

            session.add(timetable)
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
    send_timetables_to_postgres()
