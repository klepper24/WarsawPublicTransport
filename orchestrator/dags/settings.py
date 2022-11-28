import os
from datetime import datetime

from dateutil.tz import tz


SPARK_MASTER = "spark://spark:7077"
SPARK_OUT_DIR = '/opt/spark/execution_scripts/'

ENDPOINT_DBSTORE = 'api/action/dbstore_get/'
ENDPOINT_BUSESTRAMS = 'api/action/busestrams_get/'

NOW = datetime.now()
TO_ZONE = tz.gettz('Europe/Warsaw')
TODAY_DATETIME = datetime.now().astimezone(TO_ZONE)
TODAY_DATE = TODAY_DATETIME.strftime("%Y%m%d")
TODAY_TIME = TODAY_DATETIME.strftime("%H%M%S")

TRAM_FOLDER = f'{SPARK_OUT_DIR}{TODAY_DATE}/'

API_WARSZAWA_URL = 'https://api.um.warszawa.pl/'

ZTM_GENERAL_LINK = 'ftp://rozklady.ztm.waw.pl'

MONGO_HOST = 'git_mongo-python_1'