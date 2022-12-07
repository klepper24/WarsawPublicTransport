import os
import sys
from math import radians, cos, sin, asin, sqrt

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window


class DelaysProcessor:
    MONGO_URL = os.getenv('MONGO_URL')
    MSSQL_HOST = os.getenv('MSSQL_HOST')
    SQLSERVER_USERNAME = os.getenv('SQLSERVER_USERNAME')
    SQLSERVER_PASSWORD = os.getenv('SQLSERVER_PASSWORD')

    def __init__(self):
        self._check_variables()
        self.spark = self._initialize_spark_connection()
        self.udf_get_distance = func.udf(self.get_distance, DoubleType())

    @staticmethod
    def _check_variables():
        if not DelaysProcessor.MONGO_URL:
            raise RuntimeError('MONGO_URL env variable is not set')
        if not DelaysProcessor.MSSQL_HOST:
            raise RuntimeError('MSSQL_HOST env variable is not set')
        if not DelaysProcessor.SQLSERVER_USERNAME:
            raise RuntimeError('SQLSERVER_USERNAME env variable is not set')
        if not DelaysProcessor.SQLSERVER_PASSWORD:
            raise RuntimeError('SQLSERVER_PASSWORD env variable is not set')

    def execute_script(self):
        stops = self._get_stops()
        route_coordinates = self._get_route_coordinates(stops)
        calendar = self._get_calendar()
        timetable = self._get_timetable(stops)
        tram_stops = self._get_tram_stops(route_coordinates)
        results = self._get_results()
        previous_stops = self._get_previous_stops(results)
        routes_order = self._get_routes_order(route_coordinates)
        direction = self._get_direction()
        # self._write_stops_data(stops)
        # self._write_tram_data(tram_stops)
        # self._write_calendar_data(calendar)
        self._write_fact_data(direction)

    @staticmethod
    def _get_mongo_collection_url(collection_name: str):
        return f'{DelaysProcessor.MONGO_URL}WarsawPublicTransport.{collection_name}?authSource=admin'

    def _initialize_spark_connection(self):
        mongo_enpoint = self._get_mongo_collection_url('Stops')
        return SparkSession \
            .builder \
            .appName("WarsawTransportation") \
            .config("spark.mongodb.input.uri", mongo_enpoint) \
            .config("spark.mongodb.output.uri", mongo_enpoint) \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()

    def _get_stops(self):
        mongo_stops = self.spark.read.format("mongo").load()
        stops = mongo_stops \
            .withColumn("Lon", mongo_stops["longitude"]) \
            .withColumn("Lat", mongo_stops["latitude"]) \
            .drop(func.col("_id")) \
            .withColumn("stop_nr", func.concat(func.col("unit"), func.col("post"))) \
            .drop(func.col("post")) \
            .withColumnRenamed("unit_name", "StopName")
        return stops

    def _get_route_coordinates(self, stops):
        routes_json = self.read_new_collection("Routes")
        exploded_routes_json = routes_json \
            .withColumn("routes_details", func.explode(func.col("routes"))).drop(func.col("routes")) \
            .select(
                "line_nr", "number_of_routes", "routes_details.number_of_stops", "routes_details.route_nr",
                "routes_details.stops",
            ) \
            .withColumn("stops_details", func.explode(func.col("stops"))) \
            .drop(func.col("stops"))
        routes = exploded_routes_json \
            .select(
                "line_nr", "number_of_routes", "number_of_stops", "route_nr", "stops_details.stop_nr",
                func.col("stops_details.stop_name").alias("StopName"), "stops_details.street",
                "stops_details.min_time", "stops_details.max_time"
            )
        routes_coord = routes.alias("r") \
            .join(stops.alias("s"), routes.stop_nr == stops.stop_nr) \
            .select("r.*", "s.Lat", "s.Lon", func.col("s.unit").alias("Unit"))\
            .orderBy("line_nr", "route_nr", "min_time")
        return routes_coord

    def _get_calendar(self):
        mongo_calendar = self.read_new_collection("Calendar")
        calendar = mongo_calendar \
            .withColumn(
                "DayType",
                func.when(func.array_contains(func.col("day_types"), "DP") == True, "DP")
                    .when(func.array_contains(func.col("day_types"), "N7") == True, "DS")
                    .when(func.array_contains(func.col("day_types"), "SB") == True, "SB")
                    .otherwise("Unknown")
            ) \
            .drop(func.col("_id"))
        calendar.createOrReplaceTempView("calendar")
        return calendar

    def _get_timetable(self, stops):
        timetable = self.read_new_collection("Timetable")
        timetable = timetable.withColumn("stop_nr", func.concat(func.col("unit"), func.col("post")))

        timetable_stop = timetable.alias("tt") \
            .join(stops, timetable.stop_nr == stops.stop_nr) \
            .select(func.col("line").alias("Lines"), func.col("route").alias("Route"),
                    func.col("tt.stop_nr").alias("StopNo"), func.col("departure_time").alias("DepTime"),
                    func.col("order").alias("Order"), func.col("day_type").alias("DayType"),
                    func.col("tt.unit").alias("Unit"))
        timetable_stop.createOrReplaceTempView("timetable")
        return timetable_stop

    def _get_tram_stops(self, route_coordinates):
        tram_json = self.spark.read.option("multiLine", "True").format("json").load([f"{sys.argv[1]}*.json"])
        exploded_tram_json = tram_json.withColumn("details", func.explode(func.col("result"))).drop("result")
        trams = exploded_tram_json \
            .select("details.Lines", "details.Lon", "details.Lat", "details.VehicleNumber", "details.Time",
                    "details.Brigade") \
            .withColumn('CurrTramTime', func.date_format(func.split(func.col("Time"), " ").getItem(1), 'HH:mm:ss')) \
            .withColumn("Date", func.date_format(func.split(func.col("Time"), " ").getItem(0), 'yyyy-MM-dd'))
        trams.sort("Lines", "Brigade")

        trams_stops = trams \
            .join(route_coordinates, trams.Lines == route_coordinates.line_nr, "inner") \
            .withColumn(
                "distance",
                self.udf_get_distance(trams.Lon, trams.Lat, route_coordinates.Lon, route_coordinates.Lat)
            ) \
            .withColumn("row_number",
                        func.row_number().over(
                            Window.partitionBy("Lines", "Brigade", "Time").orderBy(func.col("distance")))) \
            .filter(func.col("row_number") == 1) \
            .select("Lines", trams["Lat"], trams["Lon"], "VehicleNumber", "CurrTramTime", "Date", "Brigade", "StopName",
                    "Unit",
                    "distance") \
            .distinct() \
            .withColumn("LowerTime",
                        func.date_format(func.col("CurrTramTime") - func.expr("INTERVAL 10 minutes"), 'HH:mm:ss')) \
            .withColumn("UpperTime",
                        func.date_format(func.col("CurrTramTime") + func.expr("INTERVAL 3 minutes"), 'HH:mm:ss'))
        trams_stops.orderBy("Lines", "Brigade", "Time")
        trams_stops.createOrReplaceTempView("trams_stops")
        return trams_stops

    def _get_results(self):
        results = self.spark.sql(
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
        results = results.sort("Lines", "VehicleNumber", "CurrTramTime", "Brigade", "distance", "DepTime", "Route",
                               "Order")
        return results

    @staticmethod
    def _get_previous_stops(results):
        my_window = Window.partitionBy().orderBy("Lines", "VehicleNumber", "CurrTramTime", "distance", "DepTime",
                                                 "Route", "Order")
        previous_stops = results.withColumn(
            "PrevStop",
            func.when(
                results.StopName != func.lag(results.StopName).over(my_window),
                func.lag(results.StopName).over(my_window))
            ) \
            .withColumn("PrevStopCorrected", func.when(func.col("PrevStop").isNull() == True,
                                                       func.last(func.col("PrevStop"), True).over(my_window))
                        .otherwise(func.col("PrevStop"))) \
            .drop(func.col("PrevStop"))
        previous_stops.sort("Lines", "VehicleNumber", "CurrTramTime", "Brigade", "distance", "DepTime", "Route",
                            "Order")
        previous_stops.createOrReplaceTempView("previous_stops")
        return previous_stops

    @staticmethod
    def _get_routes_order(route_coordinates):
        routes_window = Window.partitionBy().orderBy("line_nr", "route_nr", "min_time")
        routes_order = route_coordinates.select("line_nr", "route_nr", "stop_nr", "StopName", "min_time") \
            .withColumn("Prev_Stop_Route",
                        func.when(route_coordinates.StopName != func.lag(route_coordinates.StopName).over(routes_window),
                                  func.lag(route_coordinates.StopName).over(routes_window))) \
            .withColumn("PrevCorrected", func.when(func.col("Prev_Stop_Route").isNull() == True,
                                                   func.last(func.col("Prev_Stop_Route"), True).over(routes_window))
                        .otherwise(func.col("Prev_Stop_Route"))) \
            .drop(func.col("Prev_Stop_Route")) \
            .distinct()
        routes_order.orderBy("line_nr", "route_nr", "min_time")
        routes_order.createOrReplaceTempView("routes_order")
        return routes_order

    def _get_direction(self):
        direction = self.spark.sql(
            "SELECT ps.*, ro.line_nr, ro.stop_nr, ro.StopName AS Stop_Name, ro.route_nr AS Direction " +
            "FROM previous_stops AS ps " +
            "LEFT JOIN routes_order AS ro " +
            "ON ps.Lines == ro.line_nr " +
            "and ps.StopNo == ro.stop_nr " +
            "and ps.PrevStopCorrected == ro.PrevCorrected ")
        direction.filter("Direction is not NULL").orderBy("Lines", "VehicleNumber", "LowerTime")
        return direction

    def _write_stops_data(self, stops):
        stops_data = stops["stop_nr", "StopName"].distinct()
        stops_data = stops_data.withColumn("stop_nr", stops_data["stop_nr"].cast(IntegerType()))
        self.write_data_to_sql_server("WarsawPublicTransport", "Stops", stops_data)

    def _write_tram_data(self, tram_stops):
        tram_data = tram_stops["Lines", "VehicleNumber", "Brigade"].distinct()
        tram_data = tram_data.withColumn("Lines", tram_data["Lines"].cast(IntegerType())) \
            .withColumn("Brigade", tram_data["Brigade"].cast(IntegerType()))
        self.write_data_to_sql_server("WarsawPublicTransport", "Trams", tram_data)

    def _write_calendar_data(self, calendar):
        calendar_data = calendar["DayType", "day"]
        calendar_data = calendar_data.withColumn("day", func.to_date(calendar_data["day"], "yyyy-MM-dd"))
        self.write_data_to_sql_server("WarsawPublicTransport", "Calendar", calendar_data)

    def _write_fact_data(self, direction):
        fact_data = direction["StopNo", "VehicleNumber", "Date", "DepTime", "Delay"]
        fact_data = fact_data.withColumn("StopNo", fact_data["StopNo"].cast(IntegerType())) \
            .withColumn("Date", func.to_date(fact_data["Date"], "yyyy-MM-dd"))
        self.write_data_to_sql_server("WarsawPublicTransport", "Delays", fact_data)

    def read_new_collection(self, collection_name: str) -> DataFrame:
        mongo_url = self._get_mongo_collection_url(collection_name)
        new_collection = self.spark.read \
            .option("uri", mongo_url) \
            .format("com.mongodb.spark.sql.DefaultSource") \
            .load()
        return new_collection

    @staticmethod
    def write_data_to_sql_server(database: str, table_name: str, data_frame: DataFrame) -> None:
        server_name = f"jdbc:sqlserver://{DelaysProcessor.MSSQL_HOST}:1433;encrypt=true;trustServerCertificate=true"
        url = server_name + ";" + "databaseName=" + database + ";"

        try:
            data_frame.write \
                .format("jdbc") \
                .mode("overwrite") \
                .option("url", url) \
                .option("dbtable", table_name) \
                .option("user", DelaysProcessor.SQLSERVER_USERNAME) \
                .option("password", DelaysProcessor.SQLSERVER_PASSWORD) \
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                .save()
        except ValueError as error:
            print("Connector write failed", error)

    @staticmethod
    def get_distance(longit_a: float, latit_a: float, longit_b: float, latit_b: float) -> float:
        """
        It calculates the distance between two points on the Earth's surface using the Harversine method

        :param longit_a: longitude of point A
        :type longit_a: float
        :param latit_a: latitude of point A
        :type latit_a: float
        :param longit_b: longitude of point B
        :type longit_b: float
        :param latit_b: Latitude of point B
        :type latit_b: float
        :return: The distance between two points in meters.
        """
        longit_a, latit_a, longit_b, latit_b = map(radians, [longit_a, latit_a, longit_b, latit_b])
        dist_longit = longit_b - longit_a
        dist_latit = latit_b - latit_a
        triangle_area = sin(dist_latit / 2) ** 2 + cos(latit_a) * cos(latit_b) * sin(dist_longit / 2) ** 2
        central_angle = 2 * asin(sqrt(triangle_area))
        earth_radius_in_meters = 6371000
        distance = central_angle * earth_radius_in_meters
        return abs(round(distance, 2))


if __name__ == "__main__":
    dp = DelaysProcessor()
    dp.execute_script()
