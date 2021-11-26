from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master("local")\
    .appName("flight-sample")\
    .getOrCreate()

file_path = "data/flight-data/csv/2015-summary.csv"
flights = spark.read.csv(file_path, inferSchema=True, header=True)

flights.createOrReplaceTempView("flights_data")

print(flights.count())

print(flights.sort(col("count").desc()).show())

query_data = spark.sql("""
    SELECT DEST_COUNTRY_NAME, SUM(count) AS total
    FROM flights_data
    GROUP BY DEST_COUNTRY_NAME
    ORDER BY total DESC 
""")
print(query_data.show())

q_dt = flights.groupBy("DEST_COUNTRY_NAME")\
        .sum("count").withColumnRenamed("sum(count)", "total")\
        .sort(desc("total"))\
        .limit(5)
print(q_dt.show())
    