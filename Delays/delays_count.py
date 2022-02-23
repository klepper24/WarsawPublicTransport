from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window


# harvesine method in meters
def get_distance(longit_a, latit_a, longit_b, latit_b):
    # Transform to radians
    longit_a, latit_a, longit_b, latit_b = map(radians, [longit_a, latit_a, longit_b, latit_b])
    dist_longit = longit_b - longit_a
    dist_latit = latit_b - latit_a
    # Calculate area
    area = sin(dist_latit / 2) ** 2 + cos(latit_a) * cos(latit_b) * sin(dist_longit / 2) ** 2
    # Calculate the central angle
    central_angle = 2 * asin(sqrt(area))
    radius = 6371000
    # Calculate Distance
    distance = central_angle * radius
    return abs(round(distance, 2))


udf_get_distance = func.udf(get_distance, DoubleType())

MONGO_URL = "mongodb://root:pass12345@localhost:27017/WarsawPublicTransport.Stops?authSource=admin"

spark = SparkSession \
    .builder \
    .appName("WarsawTransportation") \
    .config("spark.mongodb.input.uri", MONGO_URL) \
    .config("spark.mongodb.output.uri", MONGO_URL) \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .getOrCreate()

# STOPS INFO
mongo_stops = spark \
    .read.format("mongo").load()
stops = mongo_stops \
    .withColumn("Lon", mongo_stops["coordinates"].getItem(1)) \
    .withColumn("Lat", mongo_stops["coordinates"].getItem(0)) \
    .drop(func.col("_id")) \
    .drop(func.col("coordinates")) \
    .withColumn("stop_nr", func.concat(func.col("unit"), func.col("post"))) \
    .drop(func.col("unit")) \
    .drop(func.col("post")) \
    .withColumnRenamed("unit_name", "StopName")

stops.show()
# test_stops = stops.filter(func.col("unit") > 5003).select("unit_name", "Lon", "Lat")
# test_stops.show()

# ROUTES INFO
routes_json = spark \
    .read \
    .option("multiLine", "True") \
    .format("json") \
    .load("tram_data/routes/routes_json.json")

exploded_routes_json = routes_json \
    .withColumn("routes_details", func.explode(func.col("routes"))).drop(func.col("routes")) \
    .select("line_nr", "number_of_routes", "routes_details.number_of_stops", "routes_details.route_nr",
            "routes_details.stops") \
    .withColumn("stops_details", func.explode(func.col("stops"))).drop(func.col("stops"))

routes = exploded_routes_json \
    .select("line_nr", "number_of_routes", "number_of_stops", "route_nr", "stops_details.stop_nr",
            func.col("stops_details.stop_name").alias("StopName"), "stops_details.street", "stops_details.min_time",
            "stops_details.max_time")

routes_coord = routes.alias("r").join(stops.alias("s"), routes.stop_nr == stops.stop_nr) \
    .select("r.*", "s.Lat", "s.Lon")

routes_coord.show()

# TIMETABLE INFO
timetable = spark \
    .read \
    .option("multiLine", "True") \
    .format("json") \
    .load("tram_data/timetable/tram_line1.json") \
    .withColumn("stop_nr", func.concat(func.col("unit"), func.col("post")))

timetable_stop = timetable.alias("tt") \
    .join(stops, timetable.stop_nr == stops.stop_nr) \
    .select(func.col("line").alias("Lines"), func.col("route").alias("Route"),
            func.col("tt.stop_nr").alias("StopNo"), func.col("departure_time").alias("DepTime"),
            func.col("order").alias("Order"), func.col("day_type").alias("DayType"), "StopName")
timetable_stop.show()
timetable_stop.createOrReplaceTempView("timetable")

# TRAM INFO
tram_json = spark.read.option("multiLine", "True").format("json").load(
    ["tram_data/test/tram20211007143535.json"])
exploded_tram_json = tram_json.withColumn("details", func.explode(func.col("result"))).drop("result")
trams = exploded_tram_json \
    .select("details.Lines", "details.Lon", "details.Lat", "details.VehicleNumber", "details.Time", "details.Brigade") \
    .withColumn('CurrTramTime', func.date_format(func.split(func.col("Time"), " ").getItem(1), 'HH:mm:ss'))
trams.sort("Lines", "Brigade").show()

trams_stops = trams \
    .join(routes_coord, trams.Lines == routes_coord.line_nr, "inner") \
    .withColumn("distance", udf_get_distance(trams.Lon, trams.Lat, routes_coord.Lon, routes_coord.Lat)) \
    .withColumn("row_number",
                func.row_number().over(Window.partitionBy("Lines", "Brigade", "Time").orderBy(func.col("distance")))) \
    .filter(func.col("row_number") == 1) \
    .select("Lines", trams["Lat"], trams["Lon"], "VehicleNumber", "SplitTime", "Brigade", "StopName", "distance") \
    .distinct() \
    .withColumn("LowerTime", func.date_format(func.col("CurrTramTime") - func.expr("INTERVAL 10 minutes"), 'HH:mm:ss')) \
    .withColumn("UpperTime", func.date_format(func.col("CurrTramTime") + func.expr("INTERVAL 3 minutes"), 'HH:mm:ss'))
trams_stops.orderBy("Lines", "Brigade", "Time").show()
trams_stops.createOrReplaceTempView("trams_stops")

results = spark.sql(
    "SELECT t.*, ti.Lines AS TT_Lines, date_format(ti.DepTime, 'HH:mm:ss') AS DepTime, ti.StopNo, ti.DayType, ti.Route , ti.Order " +
    "FROM trams_stops AS t " +
    "INNER JOIN timetable AS ti " +
    "ON t.Lines == ti.Lines " +
    "and t.StopName == ti.StopName " +
    "and t.lowerTime < ti.DepTime " +
    "and t.upperTime > ti.DepTime " +
    "and ti.DayType == 'DP' ")
results.sort("Lines", "Brigade", "distance", "DepTime", "Route", "Order").show(1000)
