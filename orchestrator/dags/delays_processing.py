import json
import os
from datetime import timedelta, datetime
from http import HTTPStatus

import requests
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import settings
from utils import generate_api_warszawa_url

###############################################
# Parameters
###############################################
API_KEY = Variable.get("api_key")
RESOURCE_ID = 'f2e5503e927d-4ad3-9500-4ab9e55deb59'


def save_tram_gps(ti) -> None:
    url = generate_api_warszawa_url(settings.ENDPOINT_BUSESTRAMS, API_KEY, RESOURCE_ID, 'resource_id', type=2)
    r = requests.get(url)
    status = r.status_code
    if status == HTTPStatus.OK:
        json_response = r.json()

        # saving json to file
        filename = f'{settings.TRAM_FOLDER}tram{settings.TODAY_TIME}.json'
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(json_response, f, ensure_ascii=False, indent=4)
            

###############################################
# DAG Definition
###############################################

default_args = {
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
        dag_id="delays_processing", 
        description="This DAG runs a Pyspark app that uses modules.",
        default_args=default_args, 
        schedule_interval=timedelta(1)
) as dag:

    start = EmptyOperator(task_id="start", dag=dag)

#    task_is_api_active = HttpSensor(
#        task_id='is_api_active',
#        http_conn_id='url_ztm_gps',
#        endpoint='api/action/busestrams_get/',
#        request_params={'type': '2', 'resource_id': resource_id, 'apikey' : api_key}
#    )

    task_save_tram_gps = PythonOperator(
        task_id="save_tram_gps",
        python_callable=save_tram_gps
    ) 

    spark_job = SparkSubmitOperator(
        task_id="spark_job",
        application="/opt/spark/execution_scripts/delays_proc.py", # Spark application path created in airflow and spark cluster
        application_args=[settings.TRAM_FOLDER],
        name="delays processing",
        conn_id="spark_default",
        conf={"spark.master":settings.SPARK_MASTER},
        packages="org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,com.microsoft.sqlserver:mssql-jdbc:11.2.0.jre8",
        dag=dag)
        
    task_move_to_archive = BashOperator(
        task_id="move_to_archive",
        bash_command=f'mkdir -p {settings.SPARK_OUT_DIR}ARCHIVE/ & mv {settings.TRAM_FOLDER}* {settings.SPARK_OUT_DIR}ARCHIVE/ & rm -R {settings.TRAM_FOLDER}'
    )        
    
    end = EmptyOperator(task_id="end", dag=dag)

    start >> task_save_tram_gps >> spark_job >> task_move_to_archive >> end