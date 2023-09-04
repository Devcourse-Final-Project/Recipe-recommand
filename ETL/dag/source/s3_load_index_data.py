# src.py

import boto3
import getpass


def get_last_data_from_s3(bucket_name, file_key):
    print("***************Running as user*******************:", getpass.getuser())

    s3 = boto3.client(
        "s3",
        aws_access_key_id="*******************",
        aws_secret_access_key="*******************",
    )

    file_path = "/tmp/index_data_count.txt"

    # Download the file from S3 to a local directory.
    s3.download_file(bucket_name, file_key, file_path)

    # Read the last page from the downloaded text file.
    with open(file_path, "r") as f:
        lines = f.readlines()
        if lines:
            return int(lines[-1].strip())

    return None


"""
if __name__ == "__main__":


    bucket_name = "de-2-3-airflow"
    key= "test/recipe/page_index/index_page.txt"

    last_page = get_last_page_from_s3(bucket_name,key)

    print(f"Last page: {last_page}")
"""
