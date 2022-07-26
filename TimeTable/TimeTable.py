from datetime import datetime


class TimeTable:
    line: int
    route: str
    day_type: str
    unit: int
    post: str
    departure_time: datetime
    order: int

    def __init__(self, line, route, day_type, unit, post, departure_time, order):
        self.line = line
        self.route = route
        self.day_type = day_type
        self.unit = unit
        self.post = post
        self.departure_time = departure_time
        self.order = order

    def __str__(self):
        return "{} {} {} {} {} {}".format(self.line, self.route, self.day_type, self.unit,
                                                self.post, self.departure_time, self.order)

    def convert_to_json(self):
        return {
            "line": self.line,
            "route": self.route,
            "day_type": self.day_type,
            "unit": self.unit,
            "post": self.post,
            "departure_time": self.departure_time,
            "order": self.order
        }

