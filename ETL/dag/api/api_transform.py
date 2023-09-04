from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.python import PythonOperator, task
from airflow.utils.dates import days_ago

# 함수를 임포트합니다.
from src.api_data_transform import api_run_job

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 9, 2),
}

dag = DAG(
    dag_id="api_data_transform_dag",
    default_args=default_args,
    description="api data s3 processing dag",
    schedule_interval="@once",
    catchup=False,
)


@task(task_id="api_data_transform_task", dag=dag)
def api_data_transform_task():
    return api_run_job()


api_data_transform_task()
