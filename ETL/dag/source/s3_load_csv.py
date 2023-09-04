import os
import pandas as pd
import numpy as np
import glob
import boto3


def get_last_data_from_s3(bucket_name, file_key):
    s3 = boto3.client(
        "s3",
        aws_access_key_id="*****************",
        aws_secret_access_key="*****************",
    )

    file_path = "/tmp/" + os.path.basename(file_key)

    # Download the file from S3 to a local directory.
    s3.download_file(bucket_name, file_key, file_path)

    return file_path
