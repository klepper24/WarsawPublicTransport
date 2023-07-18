from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sql_alchemy_classes import Timetables
import settings
from datetime import datetime
from airflow.models import Variable
import json


def create_postgres_session(postgres_url: str):
    source_engine = create_engine(postgres_url, echo=True)
    return sessionmaker(source_engine)

def json_to_postgres():
    postgres_session = create_postgres_session(settings.POSTGRES_URL)
    session = postgres_session()

    filename = Variable.get("my_variable")

    with open(filename, 'r') as file:
        data = json.load(file)

    #autoincrement
    sequence_exists_query = text("SELECT EXISTS (SELECT 1 FROM pg_sequences WHERE schemaname = 'public' AND sequencename = 'trams_inc')")
    result = session.execute(sequence_exists_query)
    sequence_exists = result.scalar()
    if not sequence_exists:
        session.execute("""CREATE SEQUENCE trams_inc;""")
    session.execute("""ALTER TABLE dbo.trams ALTER COLUMN id SET DEFAULT nextval('trams_inc')""")
    session.commit()

    #insert_data_from_json_to_table
    insert_query = "INSERT INTO dbo.trams (id, vehicle_nr, brigade, line_nr, created_at) VALUES (DEFAULT, :vehicle_nr, :brigade, :line_nr, :created_at);"
    for r in data['result']:
        session.execute(insert_query, {"vehicle_nr": r['VehicleNumber'], "brigade": r['Brigade'], "line_nr": r['Lines'], "created_at": r['Time']})
    session.commit()


default_args = {
    "start_date": datetime(settings.NOW.year, settings.NOW.month, settings.NOW.day),
    "retries": 1,
    "retry_delay": timedelta(seconds=10)
}


with DAG(
        dag_id="trams_to_db_2",
        default_args=default_args,
        schedule_interval=timedelta(seconds=10),
        catchup=False
) as dag:
    
    file_sensor = FileSensor(
       task_id='file_sensor',
        filepath=f'{settings.TRAM_FOLDER}',
        dag=dag
    )

    dataload_task = PythonOperator(
        task_id = 'json_to_postgres',
        python_callable=json_to_postgres,
        dag=dag
    )

    dataload_task

    file_sensor >> dataload_task