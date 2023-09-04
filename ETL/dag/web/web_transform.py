from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from datetime import timedelta
from airflow.operators.python import PythonOperator, task
from airflow.utils.dates import days_ago

# 함수를 임포트합니다.
from src.web_data_transform import web_run_job

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 9, 2),
}

dag = DAG(
    dag_id="web_data_transform_dag",
    default_args=default_args,
    description="web data s3 processing dag",
    schedule_interval="@once",
    catchup=False,
)


@task(task_id="web_data_transform_task", dag=dag)
def web_data_transform_task():
    return web_run_job()


web_data_transform_task()
