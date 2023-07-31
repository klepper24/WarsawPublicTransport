from datetime import datetime
from typing import Dict


class TimeTable:
    line: int
    route: str
    day_type: str
    unit: int
    post: str
    departure_time: str
    order: int

    def __init__(self, line: int, route: str, day_type: str, unit: int, post: str, departure_time: str, order: int):
        self.line = line
        self.route = route
        self.day_type = day_type
        self.unit = unit
        self.post = post
        self.departure_time = departure_time
        self.order = order

    def __str__(self):
        return f"{self.line} {self.route} {self.day_type} {self.unit} {self.post} {self.departure_time} {self.order}"

    def obj_to_dict(self) -> Dict:
        return self.__dict__


class Calendar:
    day: datetime
    day_types: list

    def __init__(self, day: str, day_types: list):
        self.day = day
        self.day_types = day_types

    def __str__(self):
        return f"{self.day} {self.day_types}"

    def obj_to_dict(self) -> Dict:
        return self.__dict__


class Stop:
    unit: str
    post: str
    unit_name: str
    street_id: int
    latitude: float
    longitude: float
    direction: str
    valid_from: datetime

    def __init__(self, unit: str, post: str, unit_name: str, street_id: int, latitude: float, longitude: float,
                 direction: str, valid_from: datetime):
        self.unit = unit
        self.post = post
        self.unit_name = unit_name
        self.street_id = street_id
        self.latitude = latitude
        self.longitude = longitude
        self.direction = direction
        self.valid_from = valid_from

    def __str__(self):
        return f"{self.unit} {self.post} {self.unit_name} {self.street_id} {self.latitude} {self.longitude} " \
               f"{self.direction} {self.valid_from}"

    def obj_to_dict(self) -> Dict:
        return self.__dict__
