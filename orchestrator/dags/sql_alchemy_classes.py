from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

base = declarative_base()


class Stops(base):
    _tablename_ = "stops"

    unit_post = Column(Integer, primary_key=True)
    full_name = Column(String)
    stop_longitude = Column(Float)
    stop_latitude = Column(Float)
    street = Column(String)
    unit = Column(String)
    post = Column(String)
    is_depot = Column(Integer)
    create_at = Column(DateTime)

    def __init__(self, unit_post: int, full_name: str, stop_longitude: float, stop_latitude: float, street: str,
                 unit: str, post: str, is_depot: int, create_at: datetime):
        self.unit_post = unit_post
        self.full_name = full_name
        self.stop_longitude = stop_longitude
        self.stop_latitude = stop_latitude
        self.street = street
        self.unit = unit
        self.post = post
        self.is_depot = is_depot
        self.create_at = create_at


class Routes(base):
    _tablename_ = "routes"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    stops_cnt = Column(Integer)
    created_at = Column(DateTime)

    def __init__(self, id: int, name: str, stops_cnt: int, created_at: datetime):
        self.id = id
        self.name = name
        self.stops_cnt = stops_cnt
        self.created_at = created_at


class Trams(base):
    _tablename_ = "trams"

    id = Column(Integer, primary_key=True)
    vehicle_nr = Column(String)
    brigade = Column(String)
    line_nr = Column(String)
    created_at = Column(datetime)

    def __init__(self, id: int, vehicle_nr: str, brigade: str, line_nr: str, created_at: datetime):
        self.id = id
        self.vehicle_nr = vehicle_nr
        self.brigade = brigade
        self.line_nr = line_nr
        self.created_at = created_at


class Timetables(base):
    _tablename_ = "timetables"

    id = Column(Integer, primary_key=True)
    day_type = Column(String)
    departure_time = Column(DateTime)
    departure_time_sequence_nr = Column(Integer)
    stop_id = Column(Integer, ForeignKey("stops.unit_post"))
    line_nr = Column(String)
    created_at = Column(DateTime)

    def __init__(self, id: int, day_type: str, departure_time: datetime, departure_time_sequence_nr: int, stop_id: int,
                 line_nr: str, created_at: datetime):
        self.id = id
        self.day_type = day_type
        self.departure_time = departure_time
        self.departure_time_sequence_nr = departure_time_sequence_nr
        self.stop_id = stop_id
        self.line_nr = line_nr
        self.created_at = created_at


class RouteVariants(base):
    _tablename_ = "route_variants"

    id = Column(Integer, primary_key=True)
    stop_id = Column(Integer, ForeignKey("stops.unit_post"))
    route_id = Column(Integer, ForeignKey("routes.id"))
    stop_sequence_nr = Column(Integer)

    def __init__(self, id: int, stop_id: int, route_id: int, stop_sequence_nr: int):
        self.id = id
        self.stop_id = stop_id
        self.route_id = route_id
        self.stop_sequence_nr = stop_sequence_nr


class TramStates(base):
    _tablename_ = "tram_states"

    id = Column(Integer, primary_key=True)
    current_tram_time = Column(DateTime)
    stop_state = Column(String)
    tram_id = Column(Integer, ForeignKey("trams.id"))
    route_variant_id = Column(Integer, ForeignKey("route_variants.id"))
    tram_longitude = Column(Float)
    tram_latitude = Column(Float)
    distance = Column(Integer)

    def __init__(self, id: int, current_tram_time: datetime, stop_state: str, tram_id: int, route_variant_id: int,
                 tram_longitude: float, tram_latitude: float, distance: int):
        self.id = id
        self.current_tram_time = current_tram_time
        self.stop_state = stop_state
        self.tram_id = tram_id
        self.route_variant_id = route_variant_id
        self.tram_longitude = tram_longitude
        self.tram_latitude = tram_latitude
        self.distance = distance
