from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import utils.settings, json

def create_postgres_session(postgres_url: str):
    source_engine = create_engine(postgres_url, echo=True)
    return sessionmaker(source_engine)


def json_to_postgres():
    postgres_session = create_postgres_session(utils.settings.POSTGRES_URL)
    session = postgres_session()

    filename = Variable.get("my_variable")

    with open(filename, 'r') as file:
        data = json.load(file)

    #autoincrement
    last_id_query = "SELECT COALESCE(MAX(id), 0) FROM dbo.trams;"
    enum_start = session.execute(last_id_query).scalar()

    #insert_data_from_json_to_trams
    insert_query = "INSERT INTO dbo.trams (id, vehicle_nr, brigade, line_nr) VALUES (:id, :vehicle_nr, :brigade, :line_nr);"
    for idx, r in enumerate(data['result'], start=enum_start + 1):
        session.execute(insert_query, {"id": idx, "vehicle_nr": r['VehicleNumber'], "brigade": r['Brigade'], "line_nr": r['Lines'], "created_at": datetime.now()})
    session.commit()


default_args = {
    "start_date": datetime(utils.settings.NOW.year, utils.settings.NOW.month, utils.settings.NOW.day),
    "retries": 1,
    "retry_delay": timedelta(seconds=10)
}


with DAG(
        dag_id="trams_to_db",
        default_args=default_args,
        schedule_interval=timedelta(seconds=10),
        catchup=False
) as dag:
    
    file_sensor = FileSensor(
       task_id='file_sensor',
        filepath=f'{utils.settings.TRAM_FOLDER}',
        dag=dag
    )

    dataload_task = PythonOperator(
        task_id = 'json_to_postgres',
        python_callable=json_to_postgres,
        dag=dag
    )

    file_sensor >> dataload_task
