CREATE TABLE "stops" (
  "id" serial PRIMARY KEY,
  "full_name" varchar(100),
  "stop_longitude" double precision,
  "stop_latitude" double precision,
  "street" varchar(80),
  "unit" varchar(4),
  "post" varchar(2),
  "is_depot" boolean,
  "created_at" timestamp DEFAULT (now())
);

CREATE TABLE "tram_states" (
  "id" bigserial PRIMARY KEY,
  "current_tram_time" timestamp,
  "stop_state" varchar(20),
  "tram_id" integer,
  "route_variant_id" integer,
  "tram_longitude" double precision,
  "tram_latitude" double precision,
  "distance" integer
);

CREATE TABLE "routes" (
  "id" serial PRIMARY KEY,
  "name" varchar(30) UNIQUE,
  "stops_cnt" smallint,
  "created_at" timestamp DEFAULT (now())
);

CREATE TABLE "route_variants" (
  "id" serial PRIMARY KEY,
  "stop_id" integer NOT NULL,
  "route_id" integer NOT NULL,
  "stop_sequence_nr" smallint
);

CREATE TABLE "timetables" (
  "id" serial PRIMARY KEY,
  "day_type" varchar,
  "departure_time" time,
  "departure_time_sequence_nr" smallint,
  "stop_id" integer NOT NULL,
  "line_nr" varchar(3),
  "created_at" timestamp DEFAULT (now())
);

CREATE TABLE "trams" (
  "id" serial PRIMARY KEY,
  "vehicle_nr" varchar(10),
  "brigade" varchar(4),
  "line_nr" varchar(3),
  "created_at" timestamp DEFAULT (now())
);

ALTER TABLE "tram_states" ADD FOREIGN KEY ("tram_id") REFERENCES "trams" ("id");

ALTER TABLE "tram_states" ADD FOREIGN KEY ("route_variant_id") REFERENCES "route_variants" ("id");

ALTER TABLE "route_variants" ADD FOREIGN KEY ("stop_id") REFERENCES "stops" ("id");

ALTER TABLE "route_variants" ADD FOREIGN KEY ("route_id") REFERENCES "routes" ("id");

ALTER TABLE "timetables" ADD FOREIGN KEY ("stop_id") REFERENCES "stops" ("id");

ALTER TABLE "tram_states" ADD CONSTRAINT "possible_tram_states"
  CHECK ("stop_state" IN (
	'non-visited'  -- tram is before the next stop
    'occupied', -- tram is within 20m diameter to stop
    'visited' -- tram with occupied state leaves 20m diameter
  ));
