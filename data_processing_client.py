# data_processing_client.py
import socket
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize Spark session
spark = SparkSession.builder.appName("RedditStreaming").getOrCreate()

# Define the schema for the incoming data
schema = StructType([
    StructField("title", StringType(), True),
    StructField("created_utc", DoubleType(), True),
    StructField("text", StringType(), True)
])

# Define the socket client
def socket_client():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', 9999))
    
    buffer = ""

    while True:
        data = client_socket.recv(1024).decode('utf-8')
        if not data:
            break
        
        buffer += data
        while '\n' in buffer:
            line, buffer = buffer.split('\n', 1)
            if line.strip():  # Ensure it's not an empty line
                post_data = json.loads(line)
                
                # Create a DataFrame from the received data
                df = spark.createDataFrame([post_data], schema=schema)
                
                # Ensure 'created_utc' is recognized as long type
                df = df.withColumn("created_utc", col("created_utc").cast("long"))
                
                # Convert 'created_utc' to human-readable date
                df = df.withColumn("created_date", from_unixtime(col("created_utc")))
                
                # Process the DataFrame (you can add your processing steps here)
                df.show()

    client_socket.close()

# Run the socket client
socket_client()
