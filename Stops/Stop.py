from datetime import datetime


class Stop:
    unit: str
    post: str
    unit_name: str
    street_id: int
    latitude: float
    longitude: float
    direction: str
    valid_from: datetime

    def __init__(self, unit, post, unit_name, street_id, latitude, longitude, direction, valid_from):
        self.unit = unit
        self.post = post
        self.unit_name = unit_name
        self.street_id = street_id
        self.latitude = latitude
        self.longitude = longitude
        self.direction = direction
        self.valid_from = valid_from

    def __str__(self):
        return "{} {} {} {} {} {} {} {}".format(self.unit, self.post, self.unit_name, self.street_id,
                                                self.latitude, self.longitude, self.direction, self.valid_from)

    def convert_to_json(self):
        return {
            "unit": self.unit,
            "post": self.post,
            "unit_name": self.unit_name,
            "street_id": self.street_id,
            "coordinates": [self.latitude, self.longitude],
            "direction": self.direction,
            "valid_from": self.valid_from
        }

