from repository.hdfs_repository import create_directory, upload_file
from services.spark_service import spark_service
from pyspark.sql.functions import col, avg


def test_spark_hdfs_df():
    hdfs_folder = '/food-reviews'
    hdfs_file = 'reviews-250.csv'

    # 1. Create directory
    create_directory(hdfs_folder)

    # 2. Upload to HDFS
    upload_file(f'{hdfs_folder}/{hdfs_file}', './data/in/reviews-250.csv')

    # 3. Read the file locally (workaround because of the HDFS issue)
    df = spark_service.read.csv(f"hdfs://192.168.1.100:8020/{hdfs_folder}/{hdfs_file}", header=True, inferSchema=True)

    # 2 - Filter reviews with a Score of 5
    high_rating_df = df.filter(df["Score"] > 3)
    # high_rating_df.show()

    # 3 - Select specific columns
    selected_columns_df = high_rating_df.select("ProductId", "Score", "Summary", "HelpfulnessNumerator", "HelpfulnessDenominator")
    # selected_columns_df.show(5)
    # print(selected_columns_df.collect()[0].asDict())

    # 4 - Add HelpfulnessRatio column
    ratio_df = selected_columns_df.withColumn(
        "HelpfulnessRatio",
        (col("HelpfulnessNumerator") / col("HelpfulnessDenominator")).cast("double")
    )
    # df.show(5)
    
    # 5 - Filter rows with HelpfulnessDenominator > 0
    valid_helpfulness_df = ratio_df.filter(df["HelpfulnessDenominator"] > 0)
    # valid_helpfulness_df.show(5)

    # 20 - Calculate average Score per ProductId
    average_score_df = valid_helpfulness_df.groupBy("ProductId").agg(avg("Score").alias("AverageScore"))
    average_score_df.show(5)


    # ---- SQL ----
    df.createOrReplaceTempView("reviews")

    # 1 - Register the DataFrame as a temporary view
    sql_query = "SELECT * FROM reviews"
    result = spark_service.sql(sql_query)
    result.show()