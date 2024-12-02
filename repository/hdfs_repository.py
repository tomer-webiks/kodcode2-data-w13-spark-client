from services.hdfs_service import hdfs_client

def list_directory(path: str):
    return hdfs_client.list(path)

def create_directory(path: str):
    hdfs_client.makedirs(path)

def upload_file(hdfs_path: str, local_path: str):
    if hdfs_client.status(hdfs_path, strict=False):
        print(f"File {hdfs_path} already exists. Skipping upload.")
        return
    
    hdfs_client.upload(hdfs_path, local_path)
    print(f"Successfully uploaded {local_path} to {hdfs_path}.")

def download_file(hdfs_path: str, local_path: str):
    hdfs_client.download(hdfs_path, local_path)


def delete(path: str, recursive: bool = True):
    if hdfs_client.delete(path, recursive=recursive):
        print(f"Deleted: {path}")
    else:
        print(f"Failed to delete: {path}")

def read_path(input_path: str):
    with hdfs_client.read(input_path) as reader:
        return [line.decode('utf-8').strip() for line in reader]

def write_path(output_path: str, output_content: str):
    with hdfs_client.write(output_path) as writer:
        writer.write(output_content.encode('utf-8'))