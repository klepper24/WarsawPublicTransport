import sys
from math import radians, cos, sin, asin, sqrt

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window

mongo_host = 'git_mongo-python_1'
mssql_host = 'git_ms-sql_1'

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


def read_new_collection(collection_name):
    mongo_url = f"mongodb://root:pass12345@{mongo_host}:27017/WarsawPublicTransport.{collection_name}?authSource=admin"
    new_collection = spark.read \
        .option("uri", mongo_url) \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .load()
    return new_collection
    
# Send data to SQL Server database
def write_data_to_sql_server(database, table_name, data_frame):
    username = "SA"
    #password = "Pass1234!"
    password = "mssql1Ipw"

    server_name = f"jdbc:sqlserver://{mssql_host}:1433;encrypt=true;trustServerCertificate=true"
    url = server_name + ";" + "databaseName=" + database + ";"

    try:
        data_frame.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("url", url) \
            .option("dbtable", table_name) \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .save()
    except ValueError as error:
        print("Connector write failed", error)    


MONGO_URL_STOPS = f"mongodb://root:pass12345@{mongo_host}:27017/WarsawPublicTransport.Stops?authSource=admin"

spark = SparkSession \
    .builder \
    .appName("WarsawTransportation") \
    .config("spark.mongodb.input.uri", MONGO_URL_STOPS) \
    .config("spark.mongodb.output.uri", MONGO_URL_STOPS) \
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
    .drop(func.col("post")) \
    .withColumnRenamed("unit_name", "StopName")


# ROUTES INFO
routes_json = read_new_collection("Routes")
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
    .select("r.*", "s.Lat", "s.Lon", func.col("s.unit").alias("Unit"))
routes_coord.orderBy("line_nr", "route_nr", "min_time")

# CALENDAR
mongo_calendar = read_new_collection("Calendar")
calendar = mongo_calendar \
    .withColumn("DayType", func.when(func.array_contains(func.col("day_types"), "DP") == True, "DP")
    .when(func.array_contains(func.col("day_types"), "N7") == True, "DS")
    .when(func.array_contains(func.col("day_types"), "SB") == True, "SB")
    .otherwise("Unknown")) \
    .drop(func.col("_id"))
calendar.createOrReplaceTempView("calendar")


# TIMETABLE INFO
timetable = read_new_collection("Timetable")
timetable = timetable \
    .withColumn("stop_nr", func.concat(func.col("unit"), func.col("post")))

timetable_stop = timetable.alias("tt")  \
    .join(stops, timetable.stop_nr == stops.stop_nr) \
    .select(func.col("line").alias("Lines"), func.col("route").alias("Route"),
            func.col("tt.stop_nr").alias("StopNo"), func.col("departure_time").alias("DepTime"),
            func.col("order").alias("Order"), func.col("day_type").alias("DayType"), func.col("tt.unit").alias("Unit"))
timetable_stop.createOrReplaceTempView("timetable")

# TRAM INFO
#tram_json = spark.read.option("multiLine", "True").format("json").load(["tram_data/20220811/*.json"])
print(sys.argv[1])
tram_json = spark.read.option("multiLine", "True").format("json").load([f"{sys.argv[1]}*.json"])
exploded_tram_json = tram_json.withColumn("details", func.explode(func.col("result"))).drop("result")
trams = exploded_tram_json \
    .select("details.Lines", "details.Lon", "details.Lat", "details.VehicleNumber", "details.Time", "details.Brigade") \
    .withColumn('CurrTramTime', func.date_format(func.split(func.col("Time"), " ").getItem(1), 'HH:mm:ss'))\
    .withColumn("Date", func.date_format(func.split(func.col("Time"), " ").getItem(0), 'yyyy-MM-dd'))
trams.sort("Lines", "Brigade")

trams_stops = trams \
    .join(routes_coord, trams.Lines == routes_coord.line_nr, "inner") \
    .withColumn("distance", udf_get_distance(trams.Lon, trams.Lat, routes_coord.Lon, routes_coord.Lat)) \
    .withColumn("row_number",
                func.row_number().over(Window.partitionBy("Lines", "Brigade", "Time").orderBy(func.col("distance")))) \
    .filter(func.col("row_number") == 1) \
    .select("Lines", trams["Lat"], trams["Lon"], "VehicleNumber", "CurrTramTime", "Date", "Brigade", "StopName", "Unit", "distance") \
    .distinct() \
    .withColumn("LowerTime", func.date_format(func.col("CurrTramTime") - func.expr("INTERVAL 10 minutes"), 'HH:mm:ss')) \
    .withColumn("UpperTime", func.date_format(func.col("CurrTramTime") + func.expr("INTERVAL 3 minutes"), 'HH:mm:ss'))
trams_stops.orderBy("Lines", "Brigade", "Time")
trams_stops.createOrReplaceTempView("trams_stops")


results = spark.sql(
    "SELECT distinct t.*, ti.Lines AS TT_Lines, date_format(ti.DepTime, 'HH:mm:ss') AS DepTime, ti.StopNo, ti.DayType, ti.Route , ti.Order, " +
    "(unix_timestamp(to_timestamp(t.CurrTramTime))-unix_timestamp(to_timestamp(ti.DepTime))) AS Delay " +
    "FROM trams_stops AS t " +
    "INNER JOIN timetable AS ti " +
    "ON t.Lines == ti.Lines " +
    "and t.Unit == ti.Unit " +
    "INNER JOIN calendar AS c " +
    "ON t.Date == c.Day " +
    "and ti.DayType == c.DayType " +
    "and t.lowerTime < ti.DepTime " +
    "and t.upperTime > ti.DepTime ")
results = results.sort("Lines", "VehicleNumber", "CurrTramTime", "Brigade", "distance", "DepTime", "Route", "Order")


# Find previous stop
my_window = Window.partitionBy().orderBy("Lines", "VehicleNumber", "CurrTramTime", "distance", "DepTime", "Route", "Order")
previous_stops = results.withColumn("PrevStop",
                                    func.when(results.StopName != func.lag(results.StopName).over(my_window),
                                              func.lag(results.StopName).over(my_window))) \
         .withColumn("PrevStopCorrected", func.when(func.col("PrevStop").isNull() == True,
                                                    func.last(func.col("PrevStop"), True).over(my_window))
                     .otherwise(func.col("PrevStop"))) \
         .drop(func.col("PrevStop"))
previous_stops.sort("Lines", "VehicleNumber", "CurrTramTime", "Brigade", "distance", "DepTime", "Route", "Order")
previous_stops.createOrReplaceTempView("previous_stops")

routes_window = Window.partitionBy().orderBy("line_nr", "route_nr", "min_time")
routes_order = routes_coord.select("line_nr", "route_nr", "stop_nr", "StopName", "min_time") \
    .withColumn("Prev_Stop_Route", func.when(routes_coord.StopName != func.lag(routes_coord.StopName).over(routes_window),
                                             func.lag(routes_coord.StopName).over(routes_window))) \
    .withColumn("PrevCorrected", func.when(func.col("Prev_Stop_Route").isNull() == True,
                                           func.last(func.col("Prev_Stop_Route"), True).over(routes_window))
                .otherwise(func.col("Prev_Stop_Route"))) \
    .drop(func.col("Prev_Stop_Route")) \
    .distinct()
routes_order.orderBy("line_nr", "route_nr", "min_time")
routes_order.createOrReplaceTempView("routes_order")

# Find tram direction based on previous stop
direction = spark.sql(
    "SELECT ps.*, ro.line_nr, ro.stop_nr, ro.StopName AS Stop_Name, ro.route_nr AS Direction " +
    "FROM previous_stops AS ps " +
    "LEFT JOIN routes_order AS ro " +
    "ON ps.Lines == ro.line_nr " +
    "and ps.StopNo == ro.stop_nr " +
    "and ps.PrevStopCorrected == ro.PrevCorrected ")
direction.filter("Direction is not NULL").orderBy("Lines", "VehicleNumber", "LowerTime")
    

stops_data = stops["stop_nr", "StopName"].distinct()
stops_data = stops_data.withColumn("stop_nr", stops_data["stop_nr"].cast(IntegerType()))

tram_data = trams_stops["Lines", "VehicleNumber", "Brigade"].distinct()
tram_data = tram_data.withColumn("Lines", tram_data["Lines"].cast(IntegerType())) \
                        .withColumn("Brigade", tram_data["Brigade"].cast(IntegerType()))

calendar_data = calendar["DayType", "day"]
calendar_data = calendar_data.withColumn("day", func.to_date(calendar_data["day"], "yyyy-MM-dd"))

fact_data = direction["StopNo", "VehicleNumber", "Date", "DepTime", "Delay"]
fact_data = fact_data.withColumn("StopNo", fact_data["StopNo"].cast(IntegerType())) \
                    .withColumn("Date", func.to_date(fact_data["Date"], "yyyy-MM-dd")) \


write_data_to_sql_server("WarsawPublicTransport", "Delays", fact_data)

'''''
write_data_to_sql_server("WarsawPublicTransport", "Stops", stops_data)
write_data_to_sql_server("WarsawPublicTransport", "Trams", tram_data)
write_data_to_sql_server("WarsawPublicTransport", "Calendar", calendar_data)
'''''