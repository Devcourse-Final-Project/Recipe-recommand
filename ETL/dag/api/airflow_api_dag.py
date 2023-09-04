from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, task
from airflow.utils.dates import days_ago

# fetchData 함수를 임포트합니다.
from src.api_food_recipe import fetchData

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "api_food_recipe",
    default_args=default_args,
    description="Api Food Recipe Data Collection",
    schedule_interval="0 12 * * *",  # 매일 12시에 실행
    start_date=days_ago(1),
    catchup=False,
)


@task(task_id="api_food_recipe_task", dag=dag)
def fetch_data_task():
    return fetchData()


fetch_data_task()
