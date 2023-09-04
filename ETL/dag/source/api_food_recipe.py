import requests
import pandas as pd
from xml.etree import ElementTree
import os
import boto3
from dotenv import load_dotenv
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime


# test 폴더에 파일 저장하기 위한 함수
def save_csv_to_s3(dataframe):
    s3_hook = S3Hook(aws_conn_id="aws_conn_id")

    # 현재 날짜를 가져와서 "yy-mm-dd" 형식의 문자열로 변환합니다.
    current_date = datetime.now().strftime("%y-%m-%d")

    # 파일 이름에 날짜 정보를 포함하여 생성합니다.
    file_path = f"test/api_food_recipe_{current_date}.csv"

    # 데이터프레임을 CSV 파일로 저장합니다.
    dataframe.to_csv("tmp.csv", index=False)

    # CSV 파일을 S3에 업로드 합니다.
    with open("tmp.csv", "rb") as data:
        s3_hook.load_file(
            filename="tmp.csv",
            key=file_path,
            bucket_name="de-2-3-airflow",
            replace=False,
        )

    os.remove("tmp.csv")


def fetchData():
    # .env 파일에서 환경 변수 불러오기
    load_dotenv()

    # 환경 변수에서 API 키를 불러옵니다.
    api_key = os.environ["MINISTRY_FOOD_API_KEY"]

    # 데이터 요청에 필요한 변수를 초기화합니다.
    start_index = 1
    total_data_count = 1114
    items_per_request = 1000

    data = []

    # 컬럼 이름을 선언합니다.
    columns = [
        "RCP_SEQ",
        "RCP_NM",
        "RCP_PARTS_DTLS",
        "INFO_WGT",
        "RCP_WAY2",
        "RCP_PAT2",
        "ATT_FILE_NO_MAIN",
        "ATT_FILE_NO_MK",
        "MANUAL01",
        "MANUAL02",
        "MANUAL03",
        "MANUAL04",
        "MANUAL05",
        "MANUAL06",
        "MANUAL07",
        "MANUAL08",
        "MANUAL09",
        "MANUAL10",
        "MANUAL11",
        "MANUAL12",
        "MANUAL13",
        "MANUAL14",
        "MANUAL15",
        "MANUAL16",
        "MANUAL17",
        "MANUAL18",
        "MANUAL19",
        "MANUAL20",
        "MANUAL_IMG01",
        "MANUAL_IMG02",
        "MANUAL_IMG03",
        "MANUAL_IMG04",
        "MANUAL_IMG05",
        "MANUAL_IMG06",
        "MANUAL_IMG07",
        "MANUAL_IMG08",
        "MANUAL_IMG09",
        "MANUAL_IMG10",
        "MANUAL_IMG11",
        "MANUAL_IMG12",
        "MANUAL_IMG13",
        "MANUAL_IMG14",
        "MANUAL_IMG15",
        "MANUAL_IMG16",
        "MANUAL_IMG17",
        "MANUAL_IMG18",
        "MANUAL_IMG19",
        "MANUAL_IMG20",
    ]

    # 새로운 컬럼 이름으로 바꿀 이름들을 선언합니다.
    new_column_names = {
        "RCP_SEQ": "API_ID",
        "RCP_NM": "food_name",
        "RCP_PARTS_DTLS": "ingredient",
        "INFO_WGT": "serving",
        "RCP_WAY2": "category",
        "RCP_PAT2": "category_2",
        "ATT_FILE_NO_MAIN": "food_img_1",
        "ATT_FILE_NO_MK": "food_img_2",
        "MANUAL01": "recipe_01",
        "MANUAL02": "recipe_02",
        "MANUAL03": "recipe_03",
        "MANUAL04": "recipe_04",
        "MANUAL05": "recipe_05",
        "MANUAL06": "recipe_06",
        "MANUAL07": "recipe_07",
        "MANUAL08": "recipe_08",
        "MANUAL09": "recipe_09",
        "MANUAL10": "recipe_10",
        "MANUAL11": "recipe_11",
        "MANUAL12": "recipe_12",
        "MANUAL13": "recipe_13",
        "MANUAL14": "recipe_14",
        "MANUAL15": "recipe_15",
        "MANUAL16": "recipe_16",
        "MANUAL17": "recipe_17",
        "MANUAL18": "recipe_18",
        "MANUAL19": "recipe_19",
        "MANUAL20": "recipe_20",
        "MANUAL_IMG01": "recipe_img_01",
        "MANUAL_IMG02": "recipe_img_02",
        "MANUAL_IMG03": "recipe_img_03",
        "MANUAL_IMG04": "recipe_img_04",
        "MANUAL_IMG05": "recipe_img_05",
        "MANUAL_IMG06": "recipe_img_06",
        "MANUAL_IMG07": "recipe_img_07",
        "MANUAL_IMG08": "recipe_img_08",
        "MANUAL_IMG09": "recipe_img_09",
        "MANUAL_IMG10": "recipe_img_10",
        "MANUAL_IMG11": "recipe_img_11",
        "MANUAL_IMG12": "recipe_img_12",
        "MANUAL_IMG13": "recipe_img_13",
        "MANUAL_IMG14": "recipe_img_14",
        "MANUAL_IMG15": "recipe_img_15",
        "MANUAL_IMG16": "recipe_img_16",
        "MANUAL_IMG17": "recipe_img_17",
        "MANUAL_IMG18": "recipe_img_18",
        "MANUAL_IMG19": "recipe_img_19",
        "MANUAL_IMG20": "recipe_img_20",
    }

    # API 호출 시작
    while start_index <= total_data_count:
        end_index = start_index + items_per_request - 1
        if end_index > total_data_count:
            end_index = total_data_count

        # API URL을 설정하고 요청합니다.
        url = f"http://openapi.foodsafetykorea.go.kr/api/{api_key}/COOKRCP01/xml/{start_index}/{end_index}"
        response = requests.get(url)

        # 요청이 성공적인 경우, 데이터를 추출합니다.
        if response.status_code == 200:
            tree = ElementTree.fromstring(response.content)
            rows = tree.findall("row")

            for row in rows:
                row_data = [
                    row.find(col).text if row.find(col) is not None else ""
                    for col in columns
                ]
                data.append(row_data)

        else:
            print("API 호출 실패:", response.status_code)
            break

        # 다음 요청을 위해 시작 인덱스를 업데이트합니다.
        start_index += items_per_request

    # 추출한 데이터를 데이터프레임으로 변환합니다.
    df = pd.DataFrame(data, columns=columns)
    df = df.rename(columns=new_column_names)

    # 불필요한 컬럼들을 제거합니다.
    df = df.drop(
        [
            "recipe_07",
            "recipe_08",
            "recipe_09",
            "recipe_10",
            "recipe_11",
            "recipe_12",
            "recipe_13",
            "recipe_14",
            "recipe_15",
            "recipe_16",
            "recipe_17",
            "recipe_18",
            "recipe_19",
            "recipe_20",
            "recipe_img_07",
            "recipe_img_08",
            "recipe_img_09",
            "recipe_img_10",
            "recipe_img_11",
            "recipe_img_12",
            "recipe_img_13",
            "recipe_img_14",
            "recipe_img_15",
            "recipe_img_16",
            "recipe_img_17",
            "recipe_img_18",
            "recipe_img_19",
            "recipe_img_20",
        ],
        axis=1,
    )

    df["serving"] = "1인분"

    # 원래 컬럼에서 "[숫자인분]" 패턴 제거
    df["ingredient"] = df["ingredient"].str.replace("(\[\d+인분\])", "", regex=True)
    df["ingredient"] = df["ingredient"].str.replace("(\[\s*\d+인분\s*\])", "", regex=True)

    # 'API_ID' 컬럼을 int 타입으로 변환
    df["API_ID"] = df["API_ID"].astype(int)

    # API_ID 제외 컬럼 문자열 타입으로 변환
    for col in df.columns:
        if col != "API_ID" and pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].astype(str)

    row_count = df.shape[0]

    # 데이터프레임을 S3에 csv 파일로 저장합니다.
    save_csv_to_s3(df)

    return f"행 개수: {row_count}"


if __name__ == "__main__":
    print(fetchData())
