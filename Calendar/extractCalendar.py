from Calendar import Calendar
import os
import datetime
import pymongo

directory = os.fsencode('calendar/')

current_date = str(datetime.datetime.today()).split()[0]


def extract_lines():
    calendar_days= []
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
                        calendar_days.append(new_calendar_day)
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


def load_calendar_to_MongoDB(calendar_days: list[Calendar]):
    my_client = pymongo.MongoClient("mongodb://root:pass12345@localhost:27017/")
    my_database = my_client["WarsawPublicTransport"]
    my_collection = my_database["Calendar"]

    my_collection.drop()
    for calendar in calendar_days:
        my_collection.insert_one(calendar.convert_to_json())

    cursor = my_collection.find()
    for record in cursor:
        print(record)


calendar_days = extract_lines()
load_calendar_to_MongoDB(calendar_days)
