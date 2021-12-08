from common import sp_session
from pyspark.sql.functions import window, column, desc, col, split

file_path = "data/retail-data/by-day/*.csv"

df = sp_session.read.csv(file_path, header=True, inferSchema=True)
static_schema = df.schema

df = df.withColumn("desc_arr", split(df.Description, " "))

print(df.isLocal)
print(df.isStreaming)
print(df.show(10))
#print(static_schema)

df_s = df.selectExpr("CustomerID", "UnitPrice * Quantity as total", "InvoiceDate") \
        .groupBy("CustomerID", window("InvoiceDate", "20 day")) \
        .sum("total")
print(df_s.show(5))

# streaming
stream_df = sp_session.readStream.schema(static_schema).option("maxFilesPerTrigger", 1).format("csv").option("header", "true").load(file_path)
print(stream_df.isStreaming)

# output from streaming
purchase_by_hour = stream_df.selectExpr("CustomerID", "UnitPrice * Quantity as total", "InvoiceDate") \
        .groupBy("CustomerID", window("InvoiceDate", "1 hour")) \
        .sum("total")
purchase_by_hour.writeStream.format("memory").queryName("purchase_by_hour").outputMode("complete").start()

print("Fim.")