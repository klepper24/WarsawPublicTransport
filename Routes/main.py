import json
import time
from collections import deque


def cut_file():
    with open("RA211227.txt", "rt", encoding="ansi") as file:
        f = open("result.txt", "w", encoding='utf-8')
        previous_line = ""
        for line in file:
            if ('*TR') in line or ('*LW') in line:
                try:
                    f.write(previous_line)
                    print(previous_line)
                except StopIteration:
                    # there is no lines left
                    break

                while ('#LW') not in line:
                    try:
                        f.write(line)
                        print(line)
                        line = next(file)
                    except StopIteration:
                        # there is no lines left
                        break
            previous_line = line
        f.close()


def create_json():
    routes_json = []
    with open("result.txt", "rt", encoding="utf-8") as file:
        for line in file:
            if 'Linia' in line and 'TRAMWAJOWA' in line:
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
        json.dump(routes_json, outfile, indent=4, ensure_ascii=False )



def main():
    # cut_file()
    create_json()


if __name__ == "__main__":
    main()



