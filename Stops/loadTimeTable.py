from datetime import datetime

#import folium
import requests
import json
from Stop import Stop
import pymongo
import webbrowser


def create_stops_list(json_string: dict):
    stops = []
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


def get_json(link: str):
    response = requests.get(link)
    return json.loads(response.text)


def load_stops_to_MongoDB(stops_list: list[Stop]):
    my_client = pymongo.MongoClient("mongodb://root:pass12345@localhost:27017/")
    my_database = my_client["WarsawPublicTransport"]
    my_collection = my_database["Stops"]

    my_collection.drop()
    for stop in stops_list:
        my_collection.insert_one(stop.convert_to_json())

    cursor = my_collection.find()
    for record in cursor:
        print(record)
        
        
    my_collection.createIndex({"coordinates": "2dsphere"})


# def create_stops_map(stops_list: list[Stop]):
    # my_map = folium.Map(
        # location=[52.237049, 21.017532],
        # zoom_start=13,
    # )
    # for stop in stops_list:
        # tooltip_txt = "{} {}, direction: {}".format(stop.unit_name, stop.post, stop.direction)

        # folium.Marker([stop.latitude, stop.longitude],
                      # tooltip=tooltip_txt).add_to(my_map)

    # my_map.save("stops_map.html")
    # webbrowser.open_new_tab('stops_map.html')





# main
json_file = get_json('https://api.um.warszawa.pl/api/action/dbstore_get?id=ab75c33d-3a26-4342-b36a-6e5fef0a3ac3'
                     '&apikey=963de509-f926-47d7-9337-d93113e2c4bc')
stops = create_stops_list(json_file)

# for stop in stops:
#     print(stop)
#     print(stop.convert_to_json())
# print(len(stops))

load_stops_to_MongoDB(stops)

#create_stops_map(stops)
