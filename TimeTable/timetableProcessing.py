import json
from TimeTable import TimeTable
import os
import time
import datetime

directory = os.fsencode('tram_data/')


def extract_lines():
    with open("RA220213.txt", "rt", encoding="utf8") as file:
        for line in file:
            if "Linia:" in line and len(line.split()[1]) < 3 and line.split()[1].isdecimal():
                tram_number = line.split()[1]
                f = open(f'tram_data/tram_line{tram_number}.txt', "w", encoding="utf8")
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
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        time_table = []
        if filename.endswith(".txt"):
            f = open(f'tram_data/{filename}', "r", encoding="utf8")
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
                        new_time_table = TimeTable(int(line_number), route, day_type, unit, post, departure_time, n)
                        time_table.append(new_time_table.convert_to_json())
                        line = next(f)
                else:
                    continue
            f.close()
            with open(f'tram_data/tram_line{line_number}.json', 'w') as outfile:
                json.dump(time_table, outfile, ensure_ascii=False, indent=4, default=str)
                outfile.close()


def main():
    extract_lines()
    extract_time_table()


if __name__ == "__main__":
    main()