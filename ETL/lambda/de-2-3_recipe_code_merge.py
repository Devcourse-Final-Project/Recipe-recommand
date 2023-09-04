import json
import pandas as pd
import boto3
from datetime import datetime
from io import StringIO


def lambda_handler(event, context):
    """recipe_codes merge 코드 - s3 put trigger"""

    START_TIME = datetime.now().strftime("%Y-%m-%d")

    s3 = boto3.client("s3")
    s3_resource = boto3.resource("s3")

    bucket_name = "de-2-3-airflow"

    # Assuming the date is always today's date in 'yyyy-mm-dd' format
    prefix = f"test/recipe/codes/{START_TIME}/"

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    files_to_read = [
        item["Key"]
        for item in response["Contents"]
        if item["Key"].startswith(prefix + "codes_list_")
    ]

    dfs = []  # an empty list to store the data frames

    for file_name in files_to_read:
        obj = s3.get_object(Bucket=bucket_name, Key=file_name)
        data_section = pd.read_csv(obj["Body"])  # read the body of the csv file
        dfs.append(data_section)  # append each data frame to the list

    combined_df = pd.concat(dfs, axis=0)  # combine all data frames into one

    # value 열에서 중복 제거
    combined_df.drop_duplicates(subset=["recipe_codes"], keep="first", inplace=True)

    # Reset index with drop=True to discard the old index and create a new one
    combined_df.reset_index(drop=True, inplace=True)

    csv_buffer = StringIO()

    combined_df.to_csv(csv_buffer, index=False)

    s3_resource.Object(
        bucket_name, f"test/recipe/merge_list/{START_TIME}/code_list_all.csv"
    ).put(Body=csv_buffer.getvalue())
