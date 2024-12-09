# from tests.test_stream_csv_to_append_json import test_stream_csv_to_append_json
from tests.test_spark_hdfs_df_2 import test_spark_hdfs_df as test2
from tests.test_spark_hdfs_df_1 import test_spark_hdfs_df as test1
import os

if __name__ == '__main__':
    test1()
    # test2()
    # test_stream_csv_to_append_json()
    # test_stream_csv_to_complete_json()