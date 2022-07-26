import json
from bson.json_util import dumps
from datetime import timedelta
from datetime import datetime
from dateutil import tz
import requests
import logging
import os

from airflow.decorators import dag, task
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from airflow.providers.mongo.hooks.mongo import MongoHook
 
 
default_args = {
    'owner': 'Michal Klepacki',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 1),
    'email': ['mklep@softserveinc.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

api_key = Variable.get("api_key")
resource_id = 'f2e5503e927d-4ad3-9500-4ab9e55deb59'

 
@dag(
	dag_id='ztm_gps_collector', 
	description='Collects trams GPS',
    schedule_interval='*/1 * * * *', 
	start_date=datetime(2022, 5, 1),
    default_args=default_args,
    catchup=False
) 
def saveTramLocation():

    task_is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='url_ztm_gps',
        endpoint='api/action/busestrams_get/',
        request_params={'type': '2', 'resource_id': resource_id, 'apikey' : api_key}
    )

    @task(task_id='save_tram_gps', retries=2)
    def save_tram_gps():
        to_zone = tz.gettz('Europe/Warsaw')
        today_time = datetime.now().astimezone(to_zone)
        
        date = today_time.strftime("%Y%m%d")
        time = today_time.strftime("%H%M%S")
        url = 'https://api.um.warszawa.pl/api/action/busestrams_get/?resource_id=f2e5503e927d-4ad3-9500-4ab9e55deb59&type=2&apikey='+api_key
        r = requests.get(url)
        status = r.status_code
        logging.info(f'Status: {status}')
        if status == 200:
            json_response = r.json()

            # saving json to file
            filename = f'dags/tram_data/{date}/tram{time}.json'
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(json_response, f, ensure_ascii=False, indent=4)
    
 

    task_is_api_active >> save_tram_gps()

dag = saveTramLocation()
