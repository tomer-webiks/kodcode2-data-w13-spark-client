from hdfs import InsecureClient

# Connect to HDFS
hdfs_client = InsecureClient('http://localhost:9870', user='hadoop')