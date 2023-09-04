import pandas as pd
import numpy as np
import glob
import boto3
import os
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import re
import json


def json_to_csv(df):
    url = []
    thumbnails = []
    title = []
    description = []
    views = []
    uploaded_date = []
    likes = []
    comments = []
    first_comment = []

    for i in range(len(df)):
        url.append(df[i]["url"])
        thumbnails.append(df[i]["thumbnail"])
        title.append(df[i]["title"])
        description.append(df[i]["description"])
        views.append(df[i]["views"])
        uploaded_date.append(df[i]["uploaded_date"])
        likes.append(df[i]["likes"])
        comments.append(df[i]["comments"])
        first_comment.append(df[i]["first_comment"])

    data = {
        "recipe_link": url,
        "food_img": thumbnails,
        "food_name": title,
        "ingredient": description,
        "views": views,
        "created_date": uploaded_date,
        "likes": likes,
        "comments": comments,
        "first_comments": first_comment,
    }
    return pd.DataFrame(data)


def save_csv_to_s3(dataframe):
    s3_hook = S3Hook(aws_conn_id="aws_conn_id")

    # 파일 이름에 날짜 정보를 포함하여 생성합니다.
    current_date = datetime.now().strftime("%y-%m-%d_%H:%M:%S")
    output_filename_s3 = f"video_merge_data_{current_date}.csv"

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

    # 원하는 파일의 경로 지정
    file_key = "test/recipe/video/video_dataset.json"
    local_file_path = "/tmp/" + os.path.basename(file_key)

    # Download the file from S3 to a local directory.
    bucket.download_file(file_key, local_file_path)

    # Read the JSON file into a Python object.

    with open(local_file_path, "rt", encoding="utf-8-sig") as json_file:
        json_data = json.load(json_file)

    return json_data


def video_ingredient_separate(df):
    ingre = []
    for i in range(len(df)):
        if "[ 재료 ]" in df["ingredient"][i] and "[ 만드는 법 ]" in df["ingredient"][i]:
            food_name = df["ingredient"][i].split("[ 재료 ]")[1].split("[ 만드는 법 ]")[0]
            ingre.append(food_name)
        elif "[ 재료 ]" in df["ingredient"][i] and "[만드는 법]" in df["ingredient"][i]:
            food_name = df["ingredient"][i].split("[ 재료 ]")[1].split("[만드는 법]")[0]
            ingre.append(food_name)
        elif (
            "[재료]" in df["ingredient"][i]
            and "\n\n[볶아서 만드는 법]\n\n" in df["ingredient"][i]
        ):
            food_name = (
                df["ingredient"][i].split("[재료]")[1].split("\n\n[볶아서 만드는 법]\n\n")[0]
            )
            ingre.append(food_name)
        elif "[재료]" in df["ingredient"][i] and "[만드는 법]" in df["ingredient"][i]:
            food_name = df["ingredient"][i].split("[재료]")[1].split("[만드는 법]")[0]
            ingre.append(food_name)
        elif "[재료]" in df["ingredient"][i] and "[조리법]" in df["ingredient"][i]:
            food_name = df["ingredient"][i].split("[재료]")[1].split("[조리법]")[0]
            ingre.append(food_name)
        else:
            ingre.append(np.nan)
    fin = []
    for i in ingre:
        ksd = []
        if i is np.nan:
            i = [np.nan] * 10
            fin.append(i)
        else:
            k = i.split("\n")
            fin.append(k)
    detail_fin = []
    for li in fin:
        if li == [np.nan] * 10:
            detail_fin.append(li)
            continue
        else:
            dk = []
            for i in li:
                if i == "" or i == " ":
                    continue
                else:
                    dk.append(i)
            detail_fin.append(dk)
    last_pre = []
    for li in detail_fin:
        if li == [np.nan] * 10:
            last_pre.append(li)
        elif len(li) < 10:
            while len(li) < 10:
                li.append(np.nan)
            last_pre.append(li)
        elif len(li) > 10:
            li = li[:10]
            last_pre.append(li)
        elif len(li) == 10:
            last_pre.append(li)
    ch = []
    for food_ingre in last_pre:
        detail = []
        if food_ingre == [np.nan] * 10:
            ch.append(food_ingre)
        else:
            for i in food_ingre:
                if isinstance(i, str) and "(" in i:
                    k = i.split("(")[0]
                    detail.append(k)
                else:
                    detail.append(i)
            ch.append(detail)
    parsed_data = []
    parsed_amount = []
    for recipe in ch:
        parsed_ingre = []
        parsed_unit = []
        if recipe == [np.nan] * 10:
            parsed_data.append(recipe)
            parsed_amount.append(recipe)
            continue
        else:
            for ingredient in recipe:
                if isinstance(ingredient, str):
                    match = re.match(r"^(.*?)([0-9/]+[^\d]*)$", ingredient)
                    if match:
                        name = match.group(1).strip()
                        amount = match.group(2).strip()
                        parsed_ingre.append(name)
                        parsed_unit.append(amount)

        parsed_data.append(parsed_ingre)
        parsed_amount.append(parsed_unit)

    ingredient_df = pd.DataFrame(
        parsed_data,
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
        parsed_amount,
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
    merged_df = pd.merge(df, ingredient_df, left_index=True, right_index=True)
    merged = pd.merge(merged_df, unit_df, left_index=True, right_index=True)

    return merged


def video_run_job():
    video_data = get_last_data_from_s3()

    video_processed_data = json_to_csv(video_data)

    video_processed_df = video_ingredient_separate(video_processed_data)

    save_csv_to_s3(video_processed_df)


if __name__ == "__main__":
    print(video_run_job())
