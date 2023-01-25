import json
import os
import time
from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor

import settings
from warsaw_api import WarsawApi

###############################################
# Parameters
###############################################
API_KEY = Variable.get("api_key")
RESOURCE_ID = 'f2e5503e927d-4ad3-9500-4ab9e55deb59'


def save_tram_gps(ti) -> None:
    api = WarsawApi(apikey=API_KEY)
    json_response = api.get_busestrams(resource_id=RESOURCE_ID, resource_type=2)
    # saving json to file
    filename = f'{settings.TRAM_FOLDER}tram{settings.TODAY_TIME}.json'
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(json_response, f, ensure_ascii=False, indent=4)


###############################################
# DAG Definition
###############################################

DAG_DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(settings.NOW.year, settings.NOW.month, settings.NOW.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
        dag_id="save_tram_gps",
        description="This DAG saves tram's GPS signal every ~15-20 sec.",
        default_args=DAG_DEFAULT_ARGS,
        schedule_interval=timedelta(seconds=10),
        catchup=False,
) as dag:

    start = EmptyOperator(task_id="start", dag=dag)


    task_save_tram_gps = PythonOperator(
        task_id="save_tram_gps",
        python_callable=save_tram_gps
    )
  
    end = EmptyOperator(task_id="end", dag=dag)

    start >> task_save_tram_gps >> end
