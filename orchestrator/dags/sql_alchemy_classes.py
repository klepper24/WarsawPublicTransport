from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class Stops(Base):
    __tablename__ = "stops"
    __table_args__ = {"schema": "dbo"}

    id = Column(Integer, primary_key=True)
    full_name = Column(String)
    stop_longitude = Column(Float)
    stop_latitude = Column(Float)
    street = Column(String)
    unit = Column(String)
    post = Column(String)
    is_depot = Column(Boolean)
    created_at = Column(DateTime)


class Routes(Base):
    __tablename__ = "routes"
    __table_args__ = {"schema": "dbo"}

    id = Column(Integer, primary_key=True)
    line_name = Column(String, unique=True)
    line_nr = Column(String)
    name = Column(String)
    stops_cnt = Column(Integer)
    created_at = Column(DateTime)


class Trams(Base):
    __tablename__ = "trams"
    __table_args__ = {"schema": "dbo"}

    id = Column(Integer, primary_key=True)
    vehicle_nr = Column(String)
    brigade = Column(String)
    line_nr = Column(String)
    created_at = Column(DateTime)


class Timetables(Base):
    __tablename__ = "timetables"
    __table_args__ = {"schema": "dbo"}

    id = Column(Integer, primary_key=True)
    day_type = Column(String)
    departure_time = Column(DateTime)
    departure_time_sequence_nr = Column(Integer)
    stop_id = Column(Integer, ForeignKey("stops.unit_post"))
    line_nr = Column(String)
    created_at = Column(DateTime)


class RouteVariants(Base):
    __tablename__ = "route_variants"
    __table_args__ = {"schema": "dbo"}

    id = Column(Integer, primary_key=True)
    stop_id = Column(Integer, ForeignKey("stops.unit_post"))
    route_id = Column(Integer, ForeignKey("routes.id"))
    stop_sequence_nr = Column(Integer)


class TramStates(Base):
    __tablename__ = "tram_states"
    __table_args__ = {"schema": "dbo"}

    id = Column(Integer, primary_key=True)
    current_tram_time = Column(DateTime)
    stop_state = Column(String)
    tram_id = Column(Integer, ForeignKey("trams.id"))
    route_variant_id = Column(Integer, ForeignKey("route_variants.id"))
    tram_longitude = Column(Float)
    tram_latitude = Column(Float)
    distance = Column(Integer)
