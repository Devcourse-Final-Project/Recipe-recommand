from datetime import datetime
import time
import os
from bs4 import BeautifulSoup
import math
import boto3
import csv
import urllib.request
from urllib.error import HTTPError
import numpy as np
import pandas as pd
from io import StringIO


START_TIME = datetime.now().strftime("%Y-%m-%d")


def upload_recipe_info_to_s3(df_chunk_data_dict, start_index, end_index):
    bucket_name = "de-2-3-airflow"

    csv_buffer = StringIO()
    df_chunk_data_dict.to_csv(csv_buffer)

    s3_resource = boto3.resource("s3")
    response = s3_resource.Object(
        bucket_name,
        f"test/recipe/recipe/{START_TIME}/recipe_{start_index}_{end_index}.csv",
    ).put(Body=csv_buffer.getvalue())

    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        index_file_path = "/tmp/index_data_count.txt"
        # Check if the index file exists and read the last line.
        last_line = None

        if os.path.exists(index_file_path):
            with open(index_file_path, "r") as f:
                lines = f.readlines()
                if lines:
                    last_line = lines[-1].strip()

        # Write or overwrite the end page to a text file.
        with open(index_file_path, "w") as f:
            if last_line is not None and int(last_line) > end_index:
                # If the existing last line (page number) is larger than end_page,
                # we keep it (i.e., do not overwrite with a smaller page number).
                f.write(last_line)
                print(f"Success!! Now index: {last_line}.")
            else:
                # Otherwise we record or update with the new end page.
                f.write(str(end_index))
                print(f"Success!! Now index: {end_index}.")

        # Upload the text file to S3.
        with open(index_file_path, "rb") as txt_file:
            s3_resource.Bucket(bucket_name).put_object(
                Key=f"test/recipe/page_index/index_data_count.txt", Body=txt_file
            )

        return True
    else:
        return False


def extract_data(codes):
    views = []
    food_img = []
    food_name = []

    food_info = {}
    info1 = []
    info2 = []
    info3 = []

    ingredient_name = []
    sauce_name = []
    ingredient_unit = []
    ingredient_list = []
    ingredient_dict = {}
    ingredient_set = set()
    sauce_dic = {}
    date = []
    recipe_list = []
    recipe_dict = {}

    reviews = []
    comments = []

    for recipe_code in codes:
        url = f"https://www.10000recipe.com/recipe/{recipe_code}"

        time.sleep(5)
        req = urllib.request.Request(url)
        try:
            code = urllib.request.urlopen(url).read()
        except HTTPError as e:
            if e.code == 404:
                print(f"Recipe not found for URL: {url}")
                continue
            else:
                raise
        soup = BeautifulSoup(code, "html.parser")

        if soup.find("div", "view_cate_num"):
            res = soup.find("div", "view_cate_num")
            res = res.find("span")
            viewer = res.get_text().replace(",", "")
            views.append(int(viewer))
        else:
            views.append(0)

        # image
        if soup.find("div", "centeredcrop"):
            res = soup.find("div", "centeredcrop")
            res = res.find("img")
            menu_img = res.get("src")
            food_img.append(menu_img)
        else:
            food_img.append("Na")

        # name
        if soup.find("div", "view2_summary"):
            res = soup.find("div", "view2_summary")
            res = res.find("h3")
            menu_name = res.get_text()
            food_name.append(menu_name)
        else:
            food_name.append("Na")

        # infomation (음식 정보 몇인분, 시간, 난이도 순)
        if soup.find("span", "view2_summary_info1") is not None:
            res = soup.find("span", "view2_summary_info1")
            menu_info1 = res.get_text()
            info1.append(menu_info1)
        else:
            info1.append("Na")
        if soup.find("span", "view2_summary_info2") is not None:
            res = soup.find("span", "view2_summary_info2")
            menu_info2 = res.get_text()
            info2.append(menu_info2)
        else:
            info2.append("Na")
        if soup.find("span", "view2_summary_info3") is not None:
            res = soup.find("span", "view2_summary_info3")
            menu_info3 = res.get_text()
            info3.append(menu_info3)
        else:
            info3.append("Na")
        food_info = {"info1": info1, "info2": info2, "info3": info3}

        # ingredients (재료)
        # div view_cont -> div cont_ingre -> dt(재료, 양념 등) , dd(식재료)
        # 구버전) view_cont -> br / p .get_text() 진행
        if soup.find("div", class_="ready_ingre3"):
            res = soup.find("div", class_="ready_ingre3")
            for n in res.find_all("ul"):
                sauce = []
                unit = []
                name = []
                if n.find("b"):
                    title = n.find("b").get_text()
                    sauce.append(title)
                else:
                    sauce.append("Na")
                for i in n.find_all("li"):
                    if i.find("span") is not None and i.find("a") is not None:
                        ingre_unit = i.find("span").get_text().replace("+", "")
                        ingre_name = i.get_text().split(
                            "                                                        "
                        )[0]
                        unit.append(ingre_unit)
                        name.append(ingre_name)

                    else:
                        unit.append("Na")
                        name.append("Na")
            ingredient_unit.append(unit)
            ingredient_name.append(name)

            ingredient_dict = {
                "sauce": sauce,
                "ingredient_name": ingredient_name,
                "ingredient_unit": ingredient_unit,
            }
            ingredient_list.append(ingredient_dict)
        elif soup.find("div", class_="ready_ingre3") is None and soup.find(
            "div", class_="cont_ingre"
        ):
            res = soup.find("div", class_="cont_ingre")
            for n in res.find_all("dl"):
                sauce = []
                title = n.find("dt").get_text()
                sauce.append(title)
                name = n.find("dd").get_text()
                ingredient_name.append(name)
            ingredient_dict = {
                "sauce": sauce,
                "ingredient_name": ingredient_name,
                "ingredient_unit": "Na",
            }
            ingredient_list.append(ingredient_dict)
        else:
            ingredient_dict = {
                "sauce": "Na",
                "ingredient_name": "Na",
                "ingredient_unit": "Na",
            }
            ingredient_list.append(ingredient_dict)

        # recipe (링크달기?)
        recipe_list.append(url)

        # created_date
        res = soup.find("div", "view_notice")
        if res and res.find("b"):
            created_date = res.find("b").get_text()
            created_date = created_date.split(":")[1].replace("'", "").replace(" ", "")
            date.append(created_date)
        elif res is None:
            date.append("Na")

        # reviews, comments
        # span, recipeCommentListCount
        res = soup.find_all(id="recipeCommentListCount")
        if len(res) == 2:
            re_cnt = res[0].get_text()
            reviews.append(int(re_cnt))
            com_cnt = res[1].get_text()
            comments.append(int(com_cnt))
        elif len(res) == 1:
            com_cnt = res[0].get_text()
            comments.append(int(com_cnt))
            reviews.append(0)
        else:
            comments.append(0)
            reviews.append(0)

    # Return a dictionary that contains the data:
    food_dict = {
        "food_name": food_name,
        "food_img": food_img,
        "views": views,
        "serving": info1,
        "timer": info2,
        "difficulty": info3,
        # "food_infomation": food_info,
        "ingredient": ingredient_list,
        # "ingredient_dict": ingredient_dict,
        "created_date": date,
        "recipe_link": recipe_list,
        "reviews": reviews,
        "comments": comments,
    }

    # Convert the dictionary to a pandas DataFrame:
    recipe_df = pd.DataFrame(food_dict).fillna(value=0)
    # recipe_df = pd.DataFrame(food_dict)

    return recipe_df


def lambda_handler(event, context):
    start_index = event["start_index"]
    end_index = event["end_index"]

    # start_index = 551 # test set
    # end_index = 600

    print(f"Lambda task started on indexs {start_index} to {end_index}")
    print("Crawling...")

    s3 = boto3.client("s3")
    bucket_name = "de-2-3-airflow"  # your s3 bucket name here

    file_key = f"test/recipe/merge_list/{START_TIME}/code_list_all.csv"
    # print(f"{START_TIME}")

    obj = s3.get_object(Bucket=bucket_name, Key=file_key)

    # df = pd.read_csv(filename, usecols=['column1', 'column2'], nrows=10)
    data_section = pd.read_csv(obj["Body"])  # read the body of the csv file

    # get list of codes from the DataFrame and convert to list:
    codes = data_section["recipe_codes"].tolist()

    chunk_codes = codes[start_index:end_index]
    df_chunk_data_dict = extract_data(chunk_codes)

    check = upload_recipe_info_to_s3(df_chunk_data_dict, start_index, end_index)

    if check == True:
        return {"Page": f"{start_index}~{end_index}", "status": "success"}
    else:
        return {"Page": f"{start_index}~{end_index}", "status": "fail"}
