from datetime import time


class Schedule:
    brigade: str
    direction: str
    route: str
    scheduled_time: time

    def __init__(self, brigade, direction, route, scheduled_time):
        self.brigade = brigade
        self.direction = direction
        self.route = route
        self.scheduled_time = scheduled_time

    def __str__(self):
        return f"Brigade: {self.brigade}, Direction: {self.direction}, " \
               f"Route: {self.route}, Time: {self.scheduled_time}"

    def convert_to_json(self):
        return {
            "brigade": self.brigade,
            "direction": self.direction,
            "time": self.scheduled_time
        }

