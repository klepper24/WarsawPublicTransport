import codecs
import json
import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict
import py7zr
import pymongo
import requests
import wget
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import utils.settings as settings
from utils.models import TimeTable, Stop, Calendar
from utils.warsaw_api import WarsawApi

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))


###############################################
# Parameters
###############################################
try:
    API_KEY = Variable.get("api_key")
except KeyError:
    API_KEY = os.getenv('WARSAW_API_KEY', default='dummy-incorrect-api-key')

RESOURCE_ID = 'ab75c33d-3a26-4342-b36a-6e5fef0a3ac3'
OUT_DIR = settings.AIRFLOW_OUT_DIR


###############################################
# Python functions
###############################################
class MongoConnector:
    def __init__(self, database_name="WarsawPublicTransport", database_url=settings.MONGO_URL):
        self._client = pymongo.MongoClient(database_url)
        self._database = self._client[database_name]

    def get_collection(self, collection):
        return self._database[collection]


def download_general_ztm_data(ti) -> None:
    """
    download timetable .txt file from ftp server
    change encoding from ansi to utf8

    """
    ftp = wget.download(settings.ZTM_GENERAL_LINK, out=OUT_DIR)
    with open(ftp) as f:
        files_list = [line.rstrip('\n') for line in f]
    file_name = files_list[-1][-11:]
    link_to_file = f'{settings.ZTM_GENERAL_LINK}/{file_name}'
    ztm_general_file = wget.download(link_to_file, out=OUT_DIR)

    with py7zr.SevenZipFile(ztm_general_file, mode='r') as z:
        z.extractall(f'{OUT_DIR}')
        general_file_name = z.getnames()
    general_file_name = general_file_name[0]

    # change encoding
    with codecs.open(f'{OUT_DIR}{general_file_name}', 'r', encoding='ISO-8859-1') as file:
        lines = file.read()
    with codecs.open(f'{OUT_DIR}{general_file_name}', 'w', encoding='utf8') as file:
        file.write(lines)

    ti.xcom_push(key="general_file_name", value=general_file_name)


def extract_timetable_lines(ti) -> None:
    general_file_name = ti.xcom_pull(key='general_file_name')
    with open(f'{OUT_DIR}{general_file_name}', "rt", encoding="utf8") as file_in:
        for line in file_in:
            if "Linia:" in line and len(line.split()[1]) < 3 and line.split()[1].isdecimal():
                tram_number = line.split()[1]
                with open(f'{OUT_DIR}tram_line{tram_number}.txt', "w", encoding="utf8") as outfile:
                    while True:
                        outfile.write(line)
                        try:
                            line = next(file_in)
                        except StopIteration:
                            # there is no lines left
                            break
                        if '#WK' in line:
                            # we've reached the end of the data for given tram line
                            break
                    # continue


def convert_to_time(str_hour: str) -> datetime.date:
    if '24.' not in str_hour:
        hour = datetime.strptime(str_hour, '%H.%M').time()
    else:
        hour = datetime.strptime(str_hour.replace('24.', '00.'), '%H.%M').time()
    return hour


def extract_timetable() -> List[TimeTable]:
    time_table = []
    for file in os.listdir(OUT_DIR):
        filename = os.fsdecode(file)
        if filename.endswith(".txt") and filename.startswith("tram"):
            with open(f'{OUT_DIR}{filename}', "r", encoding="utf8") as f:
                for line in f:
                    if 'Linia:' in line:
                        line_number = line.split()[1]
                    elif '*TR' in line:
                        num_of_route = line.strip()[3:]  # noqa: F841 (variable not used) @TODO - remove?
                    elif '*RP' in line or '#OP' in line:
                        try:
                            line = next(f)
                        except StopIteration:
                            break
                        stop_info = line.split()
                        unit = stop_info[0][0:4]
                        post = stop_info[0][4:]
                    elif '*OD' in line:
                        i = int(line.split()[1])
                        try:
                            line = next(f)
                        except StopIteration:
                            break
                        for n in range(i):
                            hour, stop = line.split()
                            departure_time = convert_to_time(hour)
                            route, day_type, _ = stop.split('/')
                            new_time_table = TimeTable(
                                int(line_number), route, day_type, int(unit), post, str(departure_time), n
                            )
                            time_table.append(new_time_table)
                            try:
                                line = next(f)
                            except StopIteration:
                                break

    return time_table


def load_timetable_to_mongo_db() -> None:
    timetable_collection = MongoConnector().get_collection('Timetable')
    timetable_collection.drop()
    timetables = extract_timetable()
    for timetable in timetables:
        timetable_collection.insert_one(timetable.obj_to_dict())


def extract_calendar_lines(general_file_name: str) -> List[Calendar]:
    '''
    Retrieves the *KA part from the input file, which consists of consecutive dates with a day type assigned to each
    Example:
        *KA  669
           2021-01-01    6    D5  N5  TS  DS  NP  NO
           2021-01-02    6    D6  N6  SB  DS  NP  NO
           2021-01-03    6    D7  N7  TS  DS  NS  NO
           ...

        Parameters:
                general_file_name (str): A latest file which can be found at ftp://rozklady.ztm.waw.pl'

        Returns:
                calendar_days (List[Calendar]):  list of Calendar's objects
    '''
    calendar_days = []
    current_date = str(datetime.today()).split()[0]
    with open(f"{OUT_DIR}{general_file_name}", "rt", encoding="utf8") as file_in:
        with open(f'{OUT_DIR}calendar{current_date}.txt', "w", encoding="utf8") as outfile:
            for line in file_in:
                if "*KA" in line:
                    line = next(file_in)
                    while True:
                        line_data = line.split()
                        new_calendar_day = Calendar(line_data[0], line_data[2:])
                        calendar_days.append(new_calendar_day)
                        outfile.write(line)
                        try:
                            line = next(file_in)
                        except StopIteration:
                            # there is no lines left
                            break
                        if '#KA' in line:
                            # we've reached the end of the data for given tram line
                            break
    return calendar_days


def load_calendar_to_mongo_db(ti) -> None:
    calendar_collection = MongoConnector().get_collection('Calendar')
    calendar_collection.drop()
    general_file_name = ti.xcom_pull(key='general_file_name')
    calendar_days = extract_calendar_lines(general_file_name)
    for calendar in calendar_days:
        calendar_collection.insert_one(calendar.obj_to_dict())


def get_json_from_api(link: str) -> json:
    response = requests.get(link)
    return json.loads(response.text)


def create_stops_list() -> List[Stop]:
    stops = []
    api = WarsawApi(apikey=API_KEY)
    json_string = api.get_dbstore(resource_id=RESOURCE_ID)
    for stop_values in json_string['result']:
        unit = stop_values['values'][0]['value']
        post = stop_values['values'][1]['value']
        unit_name = stop_values['values'][2]['value']
        street_id = None if stop_values['values'][3]['value'] == 'null' else int(stop_values['values'][3]['value'])
        latitude = None if stop_values['values'][4]['value'] == 'null' else float(stop_values['values'][4]['value'])
        longitude = None if stop_values['values'][5]['value'] == 'null' else float(stop_values['values'][5]['value'])
        direction = stop_values['values'][6]['value']
        valid_from = None if stop_values['values'][4]['value'] == 'null' else datetime.strptime(
            stop_values['values'][7]['value'], '%Y-%m-%d %H:%M:%S.%f')

        new_stop = Stop(unit, post, unit_name, street_id, latitude, longitude, direction, valid_from)
        stops.append(new_stop)
    return stops


def load_stops_to_mongo_db() -> None:
    stops_collection = MongoConnector().get_collection('Stops')
    stops_collection.drop()
    stops_list = create_stops_list()
    for stop in stops_list:
        stops_collection.insert_one(stop.obj_to_dict())

    stops_collection.create_index([("coordinates", pymongo.GEOSPHERE)])


def extract_routes_lines(ti) -> None:
    general_file_name = ti.xcom_pull(key='general_file_name')
    routes_output_file = "routes_file.txt"
    with open(f"{OUT_DIR}{general_file_name}", "rt", encoding="utf-8") as file:
        with open(routes_output_file, "w", encoding='utf-8') as f:
            previous_line = ""
            for line in file:
                if '*TR' in line or '*LW' in line:
                    f.write(previous_line)
                    while '#LW' not in line:
                        f.write(line)
                        try:
                            line = next(file)
                        except StopIteration:
                            # there is no lines left
                            break
                previous_line = line

    ti.xcom_push(key="routes_output_file", value=routes_output_file)


def create_routes_json(input_file: str) -> List[Dict]:
    routes_json = []
    with open(input_file, "rt", encoding="utf-8") as file:
        for line in file:
            if 'Linia' in line and 'TRAMWAJOWA' in line and line.split()[1] not in ['T', '36']:
                y = line.split()
                line_nr = y[1]
                line = next(file)
                z = line.split()
                number_of_routes = int(z[1])

                routes = []
                for _ in range(number_of_routes):
                    line = next(file)
                    route_nr = line.split()[0]
                    line = next(file)
                    number_of_stops = int(line.split()[1])

                    street = ""

                    stops = []
                    for _ in range(number_of_stops):
                        r_flag = False
                        line = next(file)
                        data = line.split()

                        if 'r' in data:
                            r_flag = True
                            data.remove('r')
                        if data[0].isdigit():
                            stop_number = int(data[0])
                        else:
                            street = ""
                            for element in data:
                                if element.isdigit():
                                    stop_number = int(element)
                                    break
                                else:
                                    street += "".join(element)
                                    street += " "
                        data_string = " ".join(data)
                        data_string = data_string.replace(street, "", 1)
                        data_string = data_string.replace(str(stop_number), "")
                        data_string = data_string.strip()
                        data_string = data_string.replace('--', ',')
                        stop_name = data_string.split(',')[0]

                        street = street.replace(",", "")

                        min_time = 0 if data_string.split('|')[1].lstrip() == '' else int(
                            data_string.split('|')[1].lstrip())
                        max_time = 0 if data_string.split('|')[2].lstrip() == '' else int(
                            data_string.split('|')[2].lstrip())

                        stop_json = {
                            'stop_nr': stop_number,
                            'stop_name': stop_name,
                            'street': street,
                            'r_flag': r_flag,
                            'min_time': min_time,
                            'max_time': max_time
                        }
                        stops.append(stop_json)

                    route = {
                        'route_nr': route_nr,
                        'number_of_stops': number_of_stops,
                        "stops": stops
                    }
                    routes.append(route)

                line = {
                    'line_nr': line_nr,
                    'number_of_routes': number_of_routes,
                    'routes': routes
                }
                routes_json.append(line)
    return routes_json


def load_routes_to_mongo_db(ti) -> None:
    routes_collection = MongoConnector().get_collection('Routes')
    routes_collection.drop()
    routes_file_name = ti.xcom_pull(key='routes_output_file')
    routes = create_routes_json(routes_file_name)
    for route in routes:
        routes_collection.insert_one(route)


###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "mklepacki",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["myemail@*****.pl"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "schedule interval": "@daily"
}

with DAG(
        dag_id="ztm_general_data",
        description="This DAG imports general Warsaw Public Transport data",
        default_args=default_args,
) as dag:

    start = EmptyOperator(task_id="start", dag=dag)

    task_download_general_ztm_data = PythonOperator(
        task_id="download_general_ztm_data",
        python_callable=download_general_ztm_data
    )

    task_extract_lines = PythonOperator(
        task_id="extract_timetable_lines",
        python_callable=extract_timetable_lines
    )

    task_load_timetable_to_MongoDB = PythonOperator(
        task_id="load_timetable_to_MongoDB",
        python_callable=load_timetable_to_mongo_db
    )

    task_remove_files = BashOperator(
        task_id="remove_files",
        bash_command=f"rm {OUT_DIR}download* && rm {OUT_DIR}tram* && rm {OUT_DIR}*.7z"
    )

    task_load_calendar_to_MongoDB = PythonOperator(
        task_id="load_calendar_to_MongoDB",
        python_callable=load_calendar_to_mongo_db
    )

    task_load_stops_to_MongoDB = PythonOperator(
        task_id="load_stops_to_MongoDB",
        python_callable=load_stops_to_mongo_db
    )

    task_extract_routes_lines = PythonOperator(
        task_id="extract_routes_lines",
        python_callable=extract_routes_lines
    )

    task_load_routes_to_MongoDB = PythonOperator(
        task_id="load_routes_to_MongoDB",
        python_callable=load_routes_to_mongo_db
    )

    end = EmptyOperator(task_id="end", dag=dag)

    start >> task_download_general_ztm_data >> task_extract_lines >> task_load_timetable_to_MongoDB \
        >> task_remove_files >> task_load_calendar_to_MongoDB >> task_load_stops_to_MongoDB \
        >> task_extract_routes_lines >> task_load_routes_to_MongoDB >> end
