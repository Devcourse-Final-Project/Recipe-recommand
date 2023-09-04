import pandas as pd
import numpy as np
import glob
import boto3
import os
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def save_csv_to_s3(dataframe):
    s3_hook = S3Hook(aws_conn_id="aws_conn_id")

    # 파일 이름에 날짜 정보를 포함하여 생성합니다.
    current_date = datetime.now().strftime("%y-%m-%d_%H:%M:%S")
    output_filename_s3 = f"web_merge_data_{current_date}.csv"

    output_dir_s3 = "test/recipe/merge_recipe/"

    # 데이터프레임을 CSV 파일로 저장합니다.
    dataframe.to_csv("tmp.csv", index=False)

    # CSV 파일을 S3에 업로드 합니다.
    with open("tmp.csv", "rb") as data:
        s3_hook.load_file(
            filename="tmp.csv",
            key=f"{output_dir_s3}{output_filename_s3}",
            bucket_name="de-2-3-airflow",
            replace=False,
        )

    os.remove("tmp.csv")


def get_last_data_from_s3():
    s3 = boto3.resource(
        "s3",
        aws_access_key_id="*********************",
        aws_secret_access_key="*****************",
    )

    bucket_name = "de-2-3-airflow"
    bucket = s3.Bucket(bucket_name)

    # 모든 .csv 파일 이름 가져오기
    files_in_bucket = [
        obj.key
        for obj in bucket.objects.filter(Prefix="test/recipe/recipe/2023-09-01/")
        if obj.key.endswith(".csv")
    ]

    dfs = []
    for file_key in files_in_bucket:
        local_file_path = "/tmp/" + os.path.basename(file_key)

        # Download the file from S3 to a local directory.
        bucket.download_file(file_key, local_file_path)

        df_temp = pd.read_csv(local_file_path)

        dfs.append(df_temp)

    return dfs


def website_preprocessing_ingredient(df):
    name = []
    unit = []
    for i in range(len(df)):
        try:
            name_data = (
                df["ingredient"][1]
                .split("ingredient")[1]
                .split("[")[i + 2]
                .split("]")[0]
            )
            ingredient_name = name_data.replace('"', "").replace("'", "").split(",")
        except IndexError:
            print(f"Name data IndexError occurred at index {i}")
            # print(f"Data at index {i}: {df['ingredient'][i]}")
            ingredient_name = [np.nan]

        try:
            unit_data = (
                df["ingredient"][1]
                .split("ingredient")[2]
                .split("[")[i + 2]
                .split("]")[0]
            )
            ingre_unit = unit_data.replace('"', "").replace("'", "").split(",")
        except IndexError:
            print(f"Unit data IndexError occurred at index {i}")
            # print(f"Data at index {i}: {df['ingredient'][i]}")
            ingre_unit = [np.nan]

        while len(ingredient_name) < 10:
            ingredient_name.append(np.nan)

        while len(ingre_unit) < 10:
            ingre_unit.append(np.nan)

        name.append(ingredient_name[:10])
        unit.append(ingre_unit[:10])

    name_df = pd.DataFrame(
        name,
        columns=[
            "ingredient_name1",
            "ingredient_name2",
            "ingredient_name3",
            "ingredient_name4",
            "ingredient_name5",
            "ingredient_name6",
            "ingredient_name7",
            "ingredient_name8",
            "ingredient_name9",
            "ingredient_name10",
        ],
    )
    unit_df = pd.DataFrame(
        unit,
        columns=[
            "ingredient_unit1",
            "ingredient_unit2",
            "ingredient_unit3",
            "ingredient_unit4",
            "ingredient_unit5",
            "ingredient_unit6",
            "ingredient_unit7",
            "ingredient_unit8",
            "ingredient_unit9",
            "ingredient_unit10",
        ],
    )
    merged_df = pd.merge(df, name_df, left_index=True, right_index=True)
    merged = pd.merge(merged_df, unit_df, left_index=True, right_index=True)

    return merged


def web_run_job():
    # S3에서 데이터를 가져와서 데이터프레임 생성
    dfs = get_last_data_from_s3()

    # 각각의 데이터프레임에 대해 전처리 함수 적용하기
    processed_dfs = [website_preprocessing_ingredient(df) for df in dfs]

    # 모든 전처리된 데이터프레임 병합하기
    df_all_processed = pd.concat(processed_dfs, ignore_index=True)

    df_all_processed = df_all_processed.drop(columns=["Unnamed: 0"])

    # 'new_index'라는 이름의 새로운 인덱스 열 추가하기
    df_all_processed = df_all_processed.reset_index().rename(
        columns={"index": "web_id"}
    )

    save_csv_to_s3(df_all_processed)


if __name__ == "__main__":
    print(web_run_job())
