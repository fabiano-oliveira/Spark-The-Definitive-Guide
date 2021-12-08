from pyspark.sql import SparkSession

sp_session = SparkSession.builder \
                .master("local") \
                .appName("pyspark_samples") \
                .getOrCreate()