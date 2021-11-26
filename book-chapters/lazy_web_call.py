from typing import Optional
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
import requests

spark = SparkSession.builder \
    .master("local")\
    .appName("lazy-web-call")\
    .getOrCreate()

urls = [Row(url='http://worldclockapi.com/api/json/utc/now')] * 100
call_api = lambda url: requests.get(url).json()['currentFileTime']

df = spark.createDataFrame(urls)\
    .withColumn('time', udf(call_api)('url'))

print(df.show())