from pyspark.sql import SparkSession


# Initialize Spark session
def get_spark_session():
    return SparkSession.builder \
        .appName("HDFS-Spark") \
        .config("spark.hadoop.dfs.client.rpc.max-size", "134217728") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
        .master("spark://localhost:7077") \
        .getOrCreate()


spark_service = get_spark_session()