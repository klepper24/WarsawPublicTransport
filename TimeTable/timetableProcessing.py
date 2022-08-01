import json
from TimeTable import TimeTable
import os
import datetime
import wget
import py7zr
import codecs
import pymongo

directory = os.fsencode('tram_data/')
link = 'ftp://rozklady.ztm.waw.pl'


def download_tram_data(ztm_url):
    """
    download timetable .txt file from ftp server
    change encoding from ansi to utf8

    :param str ztm_url: link to ftp server with timetable files
    :return str: the name of the latest timetable file
    """
    ftp = wget.download(ztm_url)
    with open(ftp) as f:
        files_list = [line.rstrip('\n') for line in f]
    file_name = files_list[-1][-11:]
    link_to_file = f'{link}/{file_name}'
    tram_file = wget.download(link_to_file)

    with py7zr.SevenZipFile(tram_file, mode='r') as z:
        z.extractall('./ftp')
        tram_file_name = z.getnames()
    tram_file_name = tram_file_name[0]

    # change encoding
    with codecs.open(f'./ftp/{tram_file_name}', 'r', encoding='ansi') as file:
        lines = file.read()
    with codecs.open(f'./ftp/{tram_file_name}', 'w', encoding='utf8') as file:
        file.write(lines)
    return tram_file_name


def load_timetable_to_MongoDB(timetables: list[TimeTable]):
    my_client = pymongo.MongoClient("mongodb://root:pass12345@localhost:27017/")
    my_database = my_client["WarsawPublicTransport"]
    my_collection = my_database["TimeTable"]

    my_collection.drop()
    for timetable in timetables:
        my_collection.insert_one(timetable.convert_to_json())

    cursor = my_collection.find()
    for record in cursor:
        print(record)


def extract_lines(tram_file_name):
    with open(f'./ftp/{tram_file_name}', "rt", encoding="utf8") as file:
        for line in file:
            if "Linia:" in line and len(line.split()[1]) < 3 and line.split()[1].isdecimal():
                tram_number = line.split()[1]
                f = open(f'tram_test/tram_line{tram_number}.txt', "w", encoding="utf8")
                while True:
                    try:
                        f.write(line)
                        line = next(file)
                    except StopIteration:
                        # there is no lines left
                        break
                    if '#WK' in line:
                        # we've reached the end of the data for given tram line
                        f.close()
                        break
                continue


def convert_to_time(str_hour: str):
    if '24.' not in str_hour:
        hour = datetime.datetime.strptime(str_hour, '%H.%M').time()
    else:
        hour = datetime.datetime.strptime(str_hour.replace('24.', '00.'), '%H.%M').time()
    return hour


def extract_time_table():
    time_table = []
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        if filename.endswith(".txt"):
            f = open(f'tram_test/{filename}', "r", encoding="utf8")
            for line in f:
                if 'Linia:' in line:
                    line_number = line.split()[1]
                elif '*TR' in line:
                    num_of_route = line.strip()[3:]
                elif '*RP' in line or '#OP' in line:
                    line = next(f)
                    stop_info = line.split()
                    unit = stop_info[0][0:4]
                    post = stop_info[0][4:]
                elif '*OD' in line:
                    i = int(line.split()[1])
                    line = next(f)
                    for n in range(i):
                        hour, stop = line.split()
                        departure_time = convert_to_time(hour)
                        route, day_type, _ = stop.split('/')
                        #print(line_number)
                        new_time_table = TimeTable(int(line_number), route, day_type, unit, post, str(departure_time), n)
                        time_table.append(new_time_table)
                        line = next(f)
                else:
                    continue
            f.close()

    return time_table


def main():
    file = download_tram_data(link)
    extract_lines(file)
    extract_time_table()
    time_tables = extract_time_table()
    load_timetable_to_MongoDB(time_tables)


if __name__ == "__main__":
   main()