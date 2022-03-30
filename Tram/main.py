from Tram import Tram
from datetime import datetime
import redis
import json


def get_data():
    with open("tram20211007142800 - Cut.json") as trams_file:
        trams_json = json.load(trams_file)
        data = []

        for result in trams_json['result']:
            lines = result['Lines']
            lon = result['Lon']
            vehicle_num = result['VehicleNumber']
            time = result['Time']
            lat = result['Lat']
            brigade = result['Brigade']

            tram = Tram(lines, lon, vehicle_num, time, lat, brigade)
            data.append(tram)

    return data


def check_stop_on_route():
    return


def get_json_from_redis(key, path='.'):
    r = redis.StrictRedis()
    return json.loads(r.execute_command('JSON.GET', key, path))


def get_line_route_stops(line_number):
    r = redis.StrictRedis()
    routes = {}
    number_of_routes = json.loads(r.execute_command('JSON.GET', line_number, '.number_of_routes'))
    for route_num in range(number_of_routes):
        stops = []
        number_of_stops = json.loads(
            r.execute_command('JSON.GET', line_number, f'.routes[{route_num}].number_of_stops'))
        for stop_num in range(number_of_stops):
            stop_nr = json.loads(
                r.execute_command('JSON.GET', line_number, f'.routes[{route_num}].stops[{stop_num}].stop_unit_post'))
            stops.append(stop_nr)
        routes[str(route_num + 1)] = stops
    return routes


def find_stop_on_route(stop_nr, routes):
    on_route = False
    if stop_nr is not None and stop_nr.isdigit():
        for route_num in routes:
            for stop in routes[route_num]:
                if int(stop_nr) == stop:
                    on_route = True
                    return on_route
    return on_route


def main():
    # t = Tram(25, 21.044827, "1185+1184", datetime.strptime("2021-10-07 14:30:14", '%Y-%m-%d %H:%M:%S'), 52.248455,
    #          "10")
    #
    # print(t)
    # print(t.find_stop())

    trams_data = get_data()
    tram_routes = {}
    for tram in trams_data:
        print(tram)
        found_stop = tram.find_stop()
        found_stop_name = found_stop[0] if found_stop is not None else None
        found_stop_nr = found_stop[1] if found_stop is not None else None
        print(f"Found stop:{found_stop_name}, {found_stop_nr}")
        routes = tram_routes[tram.Lines] if tram.Lines in tram_routes else get_line_route_stops(tram.Lines)
        tram_routes[tram.Lines] = routes
        on_a_route = find_stop_on_route(found_stop_nr, routes)
        print(f"On route: {on_a_route}\n")




if __name__ == '__main__':
    main()
