# spark_ingest.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime
import threading
import pyspark.sql.types

# Get the current working directory
current_dir = os.path.dirname(os.path.abspath(__file__))
save_path = os.path.join(current_dir, "raw_data")

# Initialize Spark session
spark = SparkSession.builder.appName("RedditStreamingIngest").getOrCreate()

# Read data from socket
df = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Define schema
schema = pyspark.sql.types.StructType([
    pyspark.sql.types.StructField("title", pyspark.sql.types.StringType(), True),
    pyspark.sql.types.StructField("created_utc", pyspark.sql.types.DoubleType(), True),
    pyspark.sql.types.StructField("text", pyspark.sql.types.StringType(), True)
])

# Parse the incoming data
json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Ensure 'created_utc' is recognized as long type
json_df = json_df.withColumn("created_utc", col("created_utc").cast("long"))

# Convert 'created_utc' to human-readable date
json_df = json_df.withColumn("created_date", from_unixtime(col("created_utc")))

def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        print(f"Batch {batch_id} is empty. Skipping processing.")
        return

    # Save raw data to files on disk (e.g., Parquet format)
    batch_df.write.mode("append").parquet(save_path)

# Start the streaming query with foreachBatch
query = json_df.writeStream.foreachBatch(process_batch).start()

# Function to start the streaming query
def start_streaming_query():
    query.awaitTermination()

# Start the streaming query in a separate thread
streaming_thread = threading.Thread(target=start_streaming_query)
streaming_thread.start()
