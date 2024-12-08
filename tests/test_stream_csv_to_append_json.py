from repository.hdfs_repository import create_directory, upload_file
from services.spark_service import spark_service
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from typing import Any
import os


def test_stream_csv_to_append_json():
    # Define the schema
    schema = StructType([
        StructField("Id", IntegerType(), True),
        StructField("ProductId", StringType(), True),
        StructField("UserId", StringType(), True),
        StructField("ProfileName", StringType(), True),
        StructField("HelpfulnessNumerator", IntegerType(), True),
        StructField("HelpfulnessDenominator", IntegerType(), True),
        StructField("Score", IntegerType(), True),
        StructField("Time", IntegerType(), True),
        StructField("Summary", StringType(), True),
        StructField("Text", StringType(), True)
    ])

    # Get the absolute path of the project root directory
    create_directory('/reviews/data/in')
    upload_file('/reviews/data/in/reviews-250.csv', 'data/in/reviews-250.csv')

    # Define the input and output directories relative to the project root
    input_dir = 'hdfs://192.168.1.100:8020/reviews/data/in'
    output_dir = 'hdfs://192.168.1.100:8020/reviews/data/out'

    # Print paths for debugging
    print(f"Input Directory: {input_dir}")
    print(f"Output Directory: {output_dir}")

    # -- EXTRACT
    # Set up the stream reader to monitor the input directory for new CSV files
    stream_df = spark_service.readStream \
        .schema(schema) \
        .csv(input_dir)
    
    # -- TRANSFORM
    top_rated_df = stream_df.filter(stream_df["Score"] == 5)

    # -- LOAD - CONSOLE
    query = top_rated_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("path", output_dir) \
        .option("checkpointLocation", "hdfs://192.168.1.100:8020/reviews/checkpoints/stream1") \
        .start()
    
    # -- LOAD - JSON
    query = top_rated_df.writeStream \
        .format("json") \
        .outputMode("append") \
        .option("path", output_dir) \
        .option("checkpointLocation", "hdfs://192.168.1.100:8020/reviews/checkpoints/stream1") \
        .start()
    

    # Wait for the streaming to finish (Ctrl+C to terminate)
    query.awaitTermination()
