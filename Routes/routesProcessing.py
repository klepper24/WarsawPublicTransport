import json
import os
import pymongo


def cut_file(input_file, output_file):
    with open(f"./ftp/{input_file}", "rt", encoding="utf-8") as file:
        f = open(output_file, "w", encoding='utf-8')
        previous_line = ""
        for line in file:
            if '*TR' in line or '*LW' in line:
                try:
                    f.write(previous_line)
                except StopIteration:
                    # there is no lines left
                    break
                while '#LW' not in line:
                    try:
                        f.write(line)
                        line = next(file)
                    except StopIteration:
                        # there is no lines left
                        break
            previous_line = line
        f.close()


def create_json(input_file: str):
    routes_json = []
    with open(input_file, "rt", encoding="utf-8") as file:
        for line in file:
            if 'Linia' in line and 'TRAMWAJOWA' in line and line.split()[1] not in ['T', '36']:
                y = line.split()
                line_nr = int(y[1])
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
    with open('routes_json.json', 'w', encoding='utf-8') as outfile:
        json.dump(routes_json, outfile, indent=4, ensure_ascii=False)
    return routes_json


def load_objects_to_MongoDB(objects: list, conn: str, database: str, collection: str):
    my_client = pymongo.MongoClient(conn)
    my_database = my_client[database]
    my_collection = my_database[collection]

    my_collection.drop()
    for object_ in objects:
        my_collection.insert_one(object_)

    cursor = my_collection.find()
    for record in cursor:
        print(record)


def main():
    file_name = os.listdir('./ftp/')[0]
    result = "result.txt"
    connection = "mongodb://root:pass12345@localhost:27017/"
    database = "WarsawPublicTransport"
    collection = "Routes"
    cut_file(file_name, result)
    routes = create_json(result)
    load_objects_to_MongoDB(routes, connection, database, collection)


if __name__ == "__main__":
    main()
