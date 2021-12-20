import requests
import json
from datetime import time
from Schedule import Schedule


def get_json(link: str):
    response = requests.get(link)
    return json.loads(response.text)


def create_schedule_list(json_string: dict):
    stops = []
    for schedule_values in json_string['result']:
        brigade = schedule_values['values'][2]['value']
        direction = schedule_values['values'][3]['value']
        route = schedule_values['values'][4]['value']
        scheduled_time = None if schedule_values['values'][5]['value'] == 'null' else time.fromisoformat(
            schedule_values['values'][5]['value'])

        new_stop = Schedule(brigade, direction, route, scheduled_time)
        stops.append(new_stop)
    return stops


def main():
    bus_stop_id = "1001"
    bus_stop_nr = "03"
    line = "3"
    json_file = get_json("https://api.um.warszawa.pl/api/action/dbtimetable_get?id=e923fa0e-d96c-43f9-ae6e-60518c9f3238"
                         f"&busstopId={bus_stop_id}"
                         f"&busstopNr={bus_stop_nr}"
                         f"&line={line}"
                         '&apikey=963de509-f926-47d7-9337-d93113e2c4bc')

    schedules = create_schedule_list(json_file)

    for schedule in schedules:
        print(schedule)



if __name__ == '__main__':
    main()
