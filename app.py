from tests.test_stream_csv_to_append_json import test_stream_csv_to_append_json
from tests.test_spark_hdfs_df_2 import test_spark_hdfs_df
import os

if __name__ == '__main__':
    test_spark_hdfs_df()
    # test_stream_csv_to_append_json()
    # test_stream_csv_to_complete_json()