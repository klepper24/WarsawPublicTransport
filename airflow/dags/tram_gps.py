import json
from bson.json_util import dumps
from datetime import timedelta
from datetime import datetime

import airflow
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from airflow.providers.mongo.hooks.mongo import MongoHook
 
 
default_args = {
    'owner': 'Michal Klepacki',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['mklep@softserveinc.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

api_key = Variable.get("api_key")
resource_id = 'f2e5503e927d-4ad3-9500-4ab9e55deb59'

def save_tram_gps(ti) -> None:
    trams = ti.xcom_pull(task_ids=['get_trams_gps'])
    with open('logs/tram_test.json', 'w') as f:
        json.dump(trams[0], f)
        
def test_mongo():
    meta_base = MongoHook('my_mongo')

    doc = meta_base.get_collection('Stops', 'WarsawPublicTransport')
    with open('logs/mongo_test.json', 'w') as file:
        file.write('[')
        for document in doc.find():
            file.write(dumps(document))
            file.write(',')
        file.write(']')

 
with DAG(
	dag_id='ztm_gps_collector', 
	description='Collects busses and trams GPS',
    schedule_interval='@daily', 
	start_date=datetime(2022, 5, 1),
    default_args=default_args,
    catchup=True
) as dag:

    task_is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='url_ztm_gps',
        endpoint='api/action/busestrams_get/',
        request_params={"type": "2", 'resource_id': resource_id, 'apikey' : api_key}
    )

    task_get_trams = SimpleHttpOperator(
        task_id='get_trams_gps',
        http_conn_id='url_ztm_gps',
        endpoint='api/action/busestrams_get/',
        method='GET',
        data={"type": "2", 'resource_id': resource_id, 'apikey' : api_key}
	)
    
    task_save = PythonOperator(
        task_id='save_tram_gps',
        python_callable=save_tram_gps
    )
    
    my_mongo_test = PythonOperator(
        task_id='my_mongo_test',
        python_callable=test_mongo
    )
	 

task_is_api_active >> task_get_trams >> task_save >> my_mongo_test
