import json
import os
from typing import Collection, Iterable
from datetime import timedelta, datetime

import pymongo
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import text

import utils.settings as settings
from utils.sql_alchemy_classes import Timetables, Stops, Routes, RouteVariants
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
    for id_nr, doc in enumerate(routes, start=1 + enum_start):
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


def map_route_variants_to_postgres_object(postgres_session: object, route_variants: object, enum_start: int) -> object:
    for id_nr, doc in enumerate(route_variants, start=1 + enum_start):
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
    routes_collection = MongoConnector().get_collection('Routes')
    while True:
        pipeline = [
            {"$unset": ["_id", "number_of_routes", "routes.number_of_stops", "routes.stops.stop_name",
                        "routes.stops.street", "routes.stops.r_flag", "routes.stops.min_time",
                        "routes.stops.max_time"]},
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
        mapped_route_variants = list(
            map_route_variants_to_postgres_object(postgres_session, agg_route_variants, offset))
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


def map_timetables_to_postgres_object(timetables_collection: Iterable, enum_start: int):
    for id_nr, timetable in enumerate(timetables_collection, start=enum_start):
        timetable = Timetables(id=id_nr, day_type=timetable["day_type"],
                               departure_time=timetable["departure_time"],
                               departure_time_sequence_nr=timetable["order"],
                               stop_id=int(str(timetable["unit"]) + timetable["post"]), line_nr=timetable["line"],
                               created_at=datetime.now())
        yield timetable


def send_timetables_to_postgres() -> [int, int]:
    timetables_collection = MongoConnector().get_collection('Stops')
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

    batch = 500
    offset = 0
    while True:
        mapped_timetables = list(map_timetables_to_postgres_object(timetables_list[offset:batch + offset], offset + 1))
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

    task_send_route_variants_to_postgres = PythonOperator(
        task_id="send_route_variants_to_postgres",
        python_callable=send_route_variants_to_postgres
    )
    
    task_send_timetables_to_postgres = PythonOperator(
        task_id="send_timetables_to_postgres",
        python_callable=send_timetables_to_postgres
    )

    end = EmptyOperator(task_id="end", dag=dag)

    start >> task_send_stops_to_postgres >> task_send_routes_to_postgres >>  \
        task_send_route_variants_to_postgres >> task_send_timetables_to_postgres >> end

