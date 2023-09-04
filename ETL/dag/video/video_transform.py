from airflow import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.python import PythonOperator, task
from airflow.utils.dates import days_ago

# 함수를 임포트합니다.
from src.video_data_transform import video_run_job

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 9, 2),
}

dag = DAG(
    dag_id="video_data_transform_dag",
    default_args=default_args,
    description="video data s3 processing dag",
    schedule_interval="@once",
    catchup=False,
)


@task(task_id="video_data_transform_task", dag=dag)
def video_data_transform_task():
    return video_run_job()


video_data_transform_task()
