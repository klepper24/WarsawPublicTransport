from Calendar import Calendar
import os
import datetime
from routesProcessing import load_objects_to_MongoDB

directory = os.fsencode('calendar/')

current_date = str(datetime.datetime.today()).split()[0]


def extract_lines():
    calendar_days = []
    file_name = os.listdir('./ftp/')[0]
    with open(f"./ftp/{file_name}", "rt", encoding="utf8") as file:
        for line in file:
            if "*KA" in line:
                line = next(file)
                f = open(f'calendar/calendar{current_date}.txt', "w", encoding="utf8")
                while True:
                    try:
                        line_data = line.split()
                        new_calendar_day = Calendar(line_data[0], line_data[2:])
                        calendar_days.append(new_calendar_day.convert_to_json())
                        f.write(line)
                        line = next(file)
                    except StopIteration:
                        # there is no lines left
                        break
                    if '#KA' in line:
                        # we've reached the end of the data for given tram line
                        f.close()
                        break
                continue
        return calendar_days


def main():
    calendar_days = extract_lines()
    connection = "mongodb://root:pass12345@localhost:27017/"
    database = "WarsawPublicTransport"
    collection = "Calendar"
    load_objects_to_MongoDB(calendar_days, connection,  database, collection)


if __name__ == "__main__":
    main()
