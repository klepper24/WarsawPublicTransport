from datetime import datetime, timedelta
from dateutil import tz
import requests
import os
import json

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from http import HTTPStatus

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
api_key = Variable.get("api_key")
resource_id = 'f2e5503e927d-4ad3-9500-4ab9e55deb59'
out_dir = '/opt/spark/execution_scripts/'

now = datetime.now()
to_zone = tz.gettz('Europe/Warsaw')
today_time = datetime.now().astimezone(to_zone)
date = today_time.strftime("%Y%m%d")
time = today_time.strftime("%H%M%S")

tram_folder = f'{out_dir}{date}/'


def save_tram_gps(ti) -> None:
    url = f'https://api.um.warszawa.pl/api/action/busestrams_get/?resource_id={resource_id}&type=2&apikey={api_key}'
    r = requests.get(url)
    status = r.status_code
    if status == HTTPStatus.OK:
        json_response = r.json()

        # saving json to file
        filename = f'{tram_folder}tram{time}.json'
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(json_response, f, ensure_ascii=False, indent=4)
            

###############################################
# DAG Definition
###############################################

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
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

    start = DummyOperator(task_id="start", dag=dag)

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
        application_args=[tram_folder],
        name="delays processing",
        conn_id="spark_default",
        conf={"spark.master":spark_master},
        packages="org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,com.microsoft.sqlserver:mssql-jdbc:11.2.0.jre8",
        dag=dag)
        
    task_move_to_archive = BashOperator(
        task_id="move_to_archive",
        bash_command=f'mkdir -p {out_dir}ARCHIVE/ & mv {tram_folder}* {out_dir}ARCHIVE/ & rm -R {tram_folder}'
    )        
    
    end = DummyOperator(task_id="end", dag=dag)

    start >> task_save_tram_gps >> spark_job >> task_move_to_archive >> end