CREATE TABLE "stops" (
  "unit_post" int PRIMARY KEY,
  "full_name" int,
  "stop_longitude" double,
  "stop_latitude" double,
  "street" varchar,
  "unit" varchar,
  "post" varchar,
  "is_depot" int,
  "created_at" datetime DEFAULT (now())
);

CREATE TABLE "tram_states" (
  "id" int PRIMARY KEY,
  "current_tram_time" timestamp,
  "stop_state" varchar,
  "tram_id" int,
  "route_variant_id" int,
  "tram_longitude" double,
  "tram_latitude" double,
  "distance" int
);

CREATE TABLE "routes" (
  "id" int PRIMARY KEY,
  "name" varchar UNIQUE,
  "stops_cnt" int,
  "created_at" datetime DEFAULT (now())
);

CREATE TABLE "route_variants" (
  "id" int PRIMARY KEY,
  "stop_id" int NOT NULL,
  "route_id" int NOT NULL,
  "stop_sequence_nr" int
);

CREATE TABLE "timetables" (
  "id" int PRIMARY KEY,
  "day_type" varchar,
  "departure_time" datetime,
  "departure_time_sequence_nr" int,
  "stop_id" int NOT NULL,
  "line_nr" varchar,
  "created_at" datetime DEFAULT (now())
);

CREATE TABLE "trams" (
  "id" int PRIMARY KEY,
  "vehicle_nr" varchar,
  "brigade" varchar,
  "line_nr" varchar,
  "created_at" datetime DEFAULT (now())
);

ALTER TABLE "tram_states" ADD FOREIGN KEY ("tram_id") REFERENCES "trams" ("id");

ALTER TABLE "tram_states" ADD FOREIGN KEY ("route_variant_id") REFERENCES "route_variants" ("id");

ALTER TABLE "route_variants" ADD FOREIGN KEY ("stop_id") REFERENCES "stops" ("unit_post");

ALTER TABLE "route_variants" ADD FOREIGN KEY ("route_id") REFERENCES "routes" ("id");

ALTER TABLE "timetables" ADD FOREIGN KEY ("stop_id") REFERENCES "stops" ("unit_post");
