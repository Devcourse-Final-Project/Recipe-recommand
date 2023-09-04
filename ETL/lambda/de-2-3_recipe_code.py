from datetime import datetime
import time
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import WebDriverException
from bs4 import BeautifulSoup
import math
import boto3
import csv


def get_page(driver, start_page, end_page):
    # 모든 페이지 크롤링
    url = "https://www.10000recipe.com/recipe/list.html?order=date&page=1"
    driver.get(url)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    res = soup.find("div", "m_list_tit")
    res = res.find("b")
    page_length = int(res.get_text().replace(",", ""))
    recipe_codes = []

    # 각 페이지 내 음식 가져오기
    # for page in range(1, math.ceil(page_length / 40) + 1):
    for page in range(start_page, end_page + 1):
        food_url = (
            f"https://www.10000recipe.com/recipe/list.html?order=date&page={page}"
        )
        driver.get(food_url)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        recipe_links = soup.find_all("a", class_="common_sp_link")

        for link in recipe_links:
            href = link.get("href")
            recipe_code = href.split("/")[2]
            recipe_codes.append(recipe_code)

        print(f"Page {page} processed.")  # Add this line to check the progress.
        driver.back()
    driver.quit()

    return recipe_codes


def upload_codes_list_to_s3(codes_list, start_page, end_page):
    START_TIME = datetime.now().strftime("%Y-%m-%d")

    s3 = boto3.resource("s3")
    bucket_name = "de-2-3-airflow"
    file_path = "/tmp/codes_list.csv"
    index_file_path = "/tmp/index_page.txt"

    # Write the codes list to a CSV file.
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["recipe_codes"])  # Write header.
        writer.writerows(map(lambda x: [x], codes_list))

    print("CSV file created for codes list.")

    # Upload the CSV file to S3.
    with open(file_path, "rb") as csv_file:
        s3.Bucket(bucket_name).put_object(
            Key=f"test/recipe/codes/{START_TIME}/codes_list_{start_page}_{end_page}.csv",
            Body=csv_file,
        )

    print("CSV file uploaded.")

    # Check if the index file exists and read the last line.
    last_line = None

    if os.path.exists(index_file_path):
        with open(index_file_path, "r") as f:
            lines = f.readlines()
            if lines:
                last_line = lines[-1].strip()

    # Write or overwrite the end page to a text file.
    with open(index_file_path, "w") as f:
        if last_line is not None and int(last_line) > end_page:
            # If the existing last line (page number) is larger than end_page,
            # we keep it (i.e., do not overwrite with a smaller page number).
            f.write(last_line)
            print(f"Index page {last_line}.")
        else:
            # Otherwise we record or update with the new end page.
            f.write(str(end_page))
            print(f"Index page {end_page}.")

    # Upload the text file to S3.
    with open(index_file_path, "rb") as txt_file:
        s3.Bucket(bucket_name).put_object(
            Key=f"test/recipe/page_index/index_page.txt", Body=txt_file
        )

    return True


def lambda_handler(event, context):
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1280x1696")
    chrome_options.add_argument("--user-data-dir=/tmp/user-data")
    chrome_options.add_argument("--hide-scrollbars")
    chrome_options.add_argument("--enable-logging")
    chrome_options.add_argument("--log-level=0")
    chrome_options.add_argument("--v=99")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument("--data-path=/tmp/data-path")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--homedir=/tmp")
    chrome_options.add_argument("--disk-cache-dir=/tmp/cache-dir")
    chrome_options.binary_location = "/opt/python/bin/headless-chromium"
    driver = webdriver.Chrome(
        chrome_options=chrome_options, executable_path="/opt/python/bin/chromedriver"
    )

    start_page = event["start_page"]
    end_page = event["end_page"]

    print(f"Lambda task started on pages {start_page} to {end_page}")

    codes_list = get_page(driver, start_page, end_page)

    check = upload_codes_list_to_s3(codes_list, start_page, end_page)

    if check == True:
        return {"Page": f"{start_page}~{end_page}", "status": "success"}
    else:
        return {"Page": f"{start_page}~{end_page}", "status": "fail"}
