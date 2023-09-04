import pandas as pd
import numpy as np
import glob
import boto3
import os
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import re


def save_csv_to_s3(dataframe):
    s3_hook = S3Hook(aws_conn_id="aws_conn_id")

    # 파일 이름에 날짜 정보를 포함하여 생성합니다.
    current_date = datetime.now().strftime("%y-%m-%d")
    output_filename_s3 = f"api_merge_data_{current_date}.csv"

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
        aws_secret_access_key="*********************",
    )

    bucket_name = "de-2-3-airflow"
    bucket = s3.Bucket(bucket_name)

    # 원하는 파일의 경로 지정
    file_key = "test/recipe/api/api_dataset.csv"
    local_file_path = "/tmp/" + os.path.basename(file_key)

    # Download the file from S3 to a local directory.
    bucket.download_file(file_key, local_file_path)

    df_temp = pd.read_csv(local_file_path)

    return df_temp


def api_preprocessing_metadata(df):
    ingredient_set = set()
    name_dt = []
    for i in range(len(df)):
        data = df["ingredient"][i]
        if data is None or not isinstance(data, str):
            data = [np.nan] * 10
        else:
            data = (
                data.replace(" ", "")
                .replace("조림장 :", "")
                .replace("고명 :", "")
                .replace("(채)", "")
                .replace("\n", "")
                .replace("●", "")
                .replace("주재료 : ", "")
                .replace("재료 ", "")
                .replace("양념장 :", "")
                .split(",")
            )
            ch = []
            u = []
            for i in data:
                k = i.split("(")[0]
                ch.append(k)
            for i in range(len(ch)):
                if len(ch) > 10:
                    ch = ch[:10]
                else:
                    while len(ch) < 10:
                        ch.append(np.nan)

            name_dt.append(ch)

    data_pre = []
    data_unit = []
    for i in range(len(name_dt)):
        if name_dt[i] == [
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
        ]:
            data_pre.append(name_dt[i])
            data_unit.append(name_dt[i])
            continue
        else:
            ingre_name = []
            ingre_unit = []
            for j in name_dt[i]:
                if isinstance(j, str):
                    match = re.match(r"^(.*?)([0-9/]+[^\d]*)$", j)
                    if match:
                        name = match.group(1).strip()
                        amount = match.group(2).strip()
                        ingre_name.append(name)
                        ingre_unit.append(amount)
            data_pre.append(ingre_name)
            data_unit.append(ingre_unit)

    for i in range(len(data_pre)):
        if len(data_pre[i]) > 10:
            data_pre[i] = data_pre[i][:10]
            data_unit[i] = data_unit[i][:10]
        else:
            while len(data_pre[i]) < 10:
                data_pre[i].append(np.nan)
                data_unit[i].append(np.nan)

    name_df = pd.DataFrame(
        data_pre,
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
        data_unit,
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

    merged["recipe_link"] = (
        merged["recipe_01"]
        + "\n"
        + merged["recipe_02"]
        + "\n"
        + merged["recipe_03"]
        + "\n"
        + merged["recipe_04"]
        + "\n"
        + merged["recipe_05"]
        + "\n"
        + merged["recipe_06"]
    )
    return merged


def api_run_job():
    # S3에서 데이터를 가져와서 데이터프레임 생성
    df = get_last_data_from_s3()

    # 각각의 데이터프레임에 대해 전처리 함수 적용하기
    api_processed_df = api_preprocessing_metadata(df)

    save_csv_to_s3(api_processed_df)


if __name__ == "__main__":
    print(api_run_job())
