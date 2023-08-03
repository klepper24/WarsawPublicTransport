import json
import os
from typing import Collection
from datetime import timedelta, datetime

import pymongo
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import text

import utils.settings as settings
from utils.sql_alchemy_classes import Timetables, Stops, Routes
from utils.warsaw_api import WarsawApi
from general_data_import_dag import MongoConnector
      

def create_postgres_session(postgres_url: str):
    source_engine = create_engine(postgres_url, echo=True)
    return sessionmaker(source_engine)


def clean_stops_unit(mongo_collection: Collection) -> None:
    # units which start with 'R'
    query = {"unit": {"$regex": "^R"}}
    x = mongo_collection.delete_many(query)
    print(x.deleted_count, " documents deleted.")


def send_stops_to_postgres(ti) -> [int, int]:
    stops_collection = MongoConnector().get_collection('Stops')
    postgres_session = create_postgres_session(settings.POSTGRES_URL)
    clean_stops_unit(stops_collection)

    with postgres_session() as session:
        rollbacked_documents = 0
        committed_documents = 0
        session.execute("TRUNCATE TABLE dbo.stops CASCADE")
        for x in stops_collection.find({}):
            stop = Stops(id=int(x["unit"] + x["post"]), full_name=x["unit_name"],
                         stop_longitude=x["longitude"], stop_latitude=x["latitude"],
                         street=x["direction"], unit=x["unit"], post=x["post"],
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


def map_routes_to_postgres_object(routes: object, enum_start: int) -> object:
    for id_nr, doc in enumerate(routes, start=1+enum_start):
        route = Routes(id=id_nr, line_name=doc["line_nr"] + doc["routes"]["route_nr"],
                       line_nr=doc["line_nr"], name=doc["routes"]["route_nr"],
                       stops_cnt=doc["routes"]["number_of_stops"],
                       created_at=datetime.now())
        yield route


def send_routes_to_postgres(ti) -> None:
    postgres_session = create_postgres_session(settings.POSTGRES_URL)
    with postgres_session() as session:
        session.execute(text('TRUNCATE TABLE dbo.routes CASCADE'))
        session.commit()
    limit = 10
    offset = 0
    routes_collection = MongoConnector().get_collection('Routes')
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


###############################################
# DAG Definition
###############################################
now = datetime.now()

DAG_DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1
}
with DAG(
        dag_id="mongo_to_postgres",
        description="This DAG sends data from mongo to postgres",
        default_args=DAG_DEFAULT_ARGS,
        schedule=None,
        catchup=False,
) as dag:

    start = EmptyOperator(task_id="start", dag=dag)

    task_send_stops_to_postgres = PythonOperator(
        task_id="send_stops_to_postgres",
        python_callable=send_stops_to_postgres
    )
    
    task_send_routes_to_postgres = PythonOperator(
        task_id="send_routes_to_postgres",
        python_callable=send_routes_to_postgres
    )

    end = EmptyOperator(task_id="end", dag=dag)

    start >> task_send_stops_to_postgres >> task_send_routes_to_postgres >> end
