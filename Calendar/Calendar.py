from datetime import datetime


class Calendar:
    day: datetime
    day_types: list

    def __init__(self, day, day_types):
        self.day = day
        self.day_types = day_types

    def __str__(self):
        return "{} {}".format(self.day, self.day_types)

    def convert_to_json(self):
        return {
            "day": self.day,
            "day_types": self.day_types
        }