CREATE SCHEMA "dbo";

CREATE TABLE "WarsawTransportState"."dbo"."stops" (
  "id" int PRIMARY KEY,
  "full_name" int,
  "longitude" double precision,
  "latitude" double precision,
  "street" varchar,
  "unit" varchar,
  "post" varchar,
  "is_depot" int
);

CREATE TABLE "WarsawTransportState"."dbo"."tram_states" (
  "id" int PRIMARY KEY,
  "current_tram_time" timestamp,
  "stop_state" varchar,
  "tram_id" int,
  "route_variant_id" int,
  "longitude" double precision,
  "latitude" double precision,
  "distance" int
);

CREATE TABLE "WarsawTransportState"."dbo"."routes" (
  "id" int PRIMARY KEY,
  "name" varchar,
  "stops_cnt" int
);

CREATE TABLE "WarsawTransportState"."dbo"."route_variants" (
  "id" int PRIMARY KEY,
  "stop_id" int,
  "route_id" int,
  "sequence_nr" int
);

CREATE TABLE "WarsawTransportState"."dbo"."timetables" (
  "id" int PRIMARY KEY,
  "day_type" varchar,
  "departure_time" timestamp,
  "sequence_nr" int,
  "stop_id" int
);

CREATE TABLE "WarsawTransportState"."dbo"."trams" (
  "id" int PRIMARY KEY,
  "vehicle_nr" varchar,
  "brigade" varchar,
  "line_nr" varchar
);