import sys
from pyspark.sql import *
from pyspark.sql import SparkSession
import pandas as pd

#spark = SparkSession.builder \
#    .appName("spark_task") \
#    .master("spark://spark:7077") \
#    .getOrCreate()

account_file = sys.argv[1]

spark = (SparkSession
    .builder
    .getOrCreate()
)

account = spark \
    .read \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .option("delimiter", ";") \
    .csv(account_file)

top5 = account.where("id < 6") \
        .select("first_name", "last_name", "age", "country")


top5.toPandas().to_csv("/opt/spark/execution_scripts/test_result.csv")

