from repository.hdfs_repository import create_directory, upload_file
from services.spark_service import spark_service
import os


# Write the dataset to HDFS
def start():
    hdfs_folder = '/food-reviews'
    hdfs_file = 'reviews-250.csv'

    # 1. Create directory
    create_directory(hdfs_folder)

    # 2. Upload to HDFS
    upload_file(f'{hdfs_folder}/{hdfs_file}', './data/Reviews-250.csv')

    # 3. Read the file locally (workaround because of the HDFS issue)
    file_path = os.path.join(os.getcwd(), "data", "Reviews-250.csv")
    # df = spark_service.read.csv(f"file:///{file_path}", header=True, inferSchema=True)
    df = spark_service.read.csv(f"hdfs://namenode:8020/{hdfs_folder}/{hdfs_file}", header=True, inferSchema=True)

    # Perform a simple transformation
    df = df.withColumn("grade", df["score"] >= 90)

    # Save the transformed data back to HDFS in Parquet format
    output_path = "hdfs://localhost:9000/user/hdfs/transformed_data"
    df.write.parquet(output_path, mode="overwrite")


if __name__ == '__main__':
    start()