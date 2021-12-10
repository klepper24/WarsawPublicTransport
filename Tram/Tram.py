from datetime import datetime
import pymongo


class Tram:
    Lines: str
    Longitude: float
    VehicleNumber: str
    Time: datetime
    Latitude: float
    Brigade: str

    def __init__(self, lines, lon, vehicle_num, time, lat, brigade):
        self.Lines = lines
        self.Longitude = lon
        self.VehicleNumber = vehicle_num
        self.Time = time
        self.Latitude = lat
        self.Brigade = brigade

    def __str__(self):
        new_line = '\n'
        return f"Line number: {self.Lines} {new_line} Coordinates: [{self.Longitude}E, {self.Latitude}N] {new_line} " \
               f"Time: {self.Time} {new_line} VehicleNumber: {self.VehicleNumber} {new_line} Brigade: {self.Brigade}"

    def find_stop(self):
        my_client = pymongo.MongoClient("mongodb://root:pass12345@localhost:27017/")
        my_database = my_client["WarsawPublicTransport"]
        my_collection = my_database["Stops"]
        stop = my_collection.find({'coordinates': {'$nearSphere': {'$geometry': {'type': 'Point', 'coordinates': [self.Latitude, self.Longitude]}, '$maxDistance': 100}}}).limit(1)
        for document in stop:
            return document["unit_name"]


