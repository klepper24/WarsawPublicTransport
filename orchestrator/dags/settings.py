import os
from datetime import datetime

from dateutil.tz import tz


SPARK_MASTER = "spark://spark:7077"
SPARK_OUT_DIR = '/opt/spark/execution_scripts/'


NOW = datetime.now()
TO_ZONE = tz.gettz('Europe/Warsaw')
TODAY_DATETIME = datetime.now().astimezone(TO_ZONE)
TODAY_DATE = TODAY_DATETIME.strftime("%Y%m%d")
TODAY_TIME = TODAY_DATETIME.strftime("%H%M%S")

DELAYS_PROCESSING_HOUR = 23

TRAM_FOLDER = f'{SPARK_OUT_DIR}{TODAY_DATE}/'

ZTM_GENERAL_LINK = 'ftp://rozklady.ztm.waw.pl'

#MONGO_HOST = os.getenv('MONGO_HOST', default='git_mongo-python_1')
MONGO_HOST = os.getenv('MONGO_HOST', default='localhost')
MONGO_USER = os.getenv('MONGO_USER', default='root')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD', default='pass12345')
MONGO_PORT = os.getenv('MONGO_PORT', default='27017')
MONGO_URL = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"

MSSQL_HOST = os.getenv('MSSQL_HOST', default='git_ms-sql_1')
SQLSERVER_USERNAME = os.getenv('SQLSERVER_USERNAME', default='SA')
SQLSERVER_PASSWORD = os.getenv('SQLSERVER_PASSWORD', default='mssql1Ipw')

AIRFLOW_OUT_DIR = os.getenv('AIRFLOW_OUT_DIR', default='/opt/airflow/dags/data/')

POSTGRES_URL = "postgresql://my_user:password123@localhost:5432/WarsawTransportState"