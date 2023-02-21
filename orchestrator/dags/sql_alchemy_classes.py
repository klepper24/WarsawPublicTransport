from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class Stops(Base):
    _tablename_ = "stops"

    unit_post = Column(Integer, primary_key=True)
    full_name = Column(String)
    stop_longitude = Column(Float)
    stop_latitude = Column(Float)
    street = Column(String)
    unit = Column(String)
    post = Column(String)
    is_depot = Column(Boolean)
    created_at = Column(DateTime)


class Routes(Base):
    _tablename_ = "routes"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    stops_cnt = Column(Integer)
    created_at = Column(DateTime)


class Trams(Base):
    _tablename_ = "trams"

    id = Column(Integer, primary_key=True)
    vehicle_nr = Column(String)
    brigade = Column(String)
    line_nr = Column(String)
    created_at = Column(datetime)


class Timetables(Base):
    _tablename_ = "timetables"

    id = Column(Integer, primary_key=True)
    day_type = Column(String)
    departure_time = Column(DateTime)
    departure_time_sequence_nr = Column(Integer)
    stop_id = Column(Integer, ForeignKey("stops.unit_post"))
    line_nr = Column(String)
    created_at = Column(DateTime)


class RouteVariants(Base):
    _tablename_ = "route_variants"

    id = Column(Integer, primary_key=True)
    stop_id = Column(Integer, ForeignKey("stops.unit_post"))
    route_id = Column(Integer, ForeignKey("routes.id"))
    stop_sequence_nr = Column(Integer)


class TramStates(Base):
    _tablename_ = "tram_states"

    id = Column(Integer, primary_key=True)
    current_tram_time = Column(DateTime)
    stop_state = Column(String)
    tram_id = Column(Integer, ForeignKey("trams.id"))
    route_variant_id = Column(Integer, ForeignKey("route_variants.id"))
    tram_longitude = Column(Float)
    tram_latitude = Column(Float)
    distance = Column(Integer)
