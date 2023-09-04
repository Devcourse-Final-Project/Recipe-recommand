from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.utils.task_group import TaskGroup
import json
import time

# Import the function from src.py file.
from src.s3_load_index_data import get_last_data_from_s3

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 8, 30),
    "catchup": False,
}

data_cnt = 50
lambda_count = 10
# end_data = 200000
end_data = 8550


def lambda_task(start_index: str, end_index: str):
    start_index = int(start_index)
    end_index = int(end_index)

    hook = LambdaHook(aws_conn_id="aws_conn_id", region_name="ap-northeast-2")

    payload = {"start_index": start_index, "end_index": end_index}

    # Convert the dict to a JSON string
    payload_json = json.dumps(payload)

    print(f"Payload: {payload_json}")

    response_from_lambda = hook.invoke_lambda(
        function_name="******************",
        payload=payload_json,
        invocation_type="Event",
    )

    if response_from_lambda["StatusCode"] == 202:
        print(f"Lambda task started on indices {start_index}~{end_index}")
    else:
        print("Lambda task failed to start.")


def check_page_number(page_number):
    if int(page_number) >= end_data:
        return "dummy_end"
    else:
        return [f"invoke_lambda_group.lambda_task_{i}" for i in range(lambda_count)]


with DAG(
    "web_recipe_data_lambda_dag",
    default_args=default_args,
    schedule_interval="0,20,40 * * * *",
) as dag:

    def read_last_indices():
        bucket_name = "de-2-3-airflow"
        index_file_key = "test/recipe/page_index/index_data_count.txt"

        last_indices_num = get_last_data_from_s3(bucket_name, index_file_key)

        if last_indices_num is not None:
            return last_indices_num

    read_last_indices_task = PythonOperator(
        task_id="read_last_indices",
        python_callable=read_last_indices,
        do_xcom_push=True,
    )

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=check_page_number,
        op_kwargs={"page_number": "{{ ti.xcom_pull(task_ids='read_last_indices') }}"},
    )

    dummy_end = DummyOperator(task_id="dummy_end")

    def invoke_lambda_tasks():
        tasks = []
        for i in range(lambda_count):
            start_idx = (
                "{{ (ti.xcom_pull(task_ids='read_last_indices') or 0) + "
                + str(i * data_cnt + 1)
                + " }}"
            )
            end_idx = (
                "{{ (ti.xcom_pull(task_ids='read_last_indices') or 0) + "
                + str((i + 1) * data_cnt)
                + " }}"
            )

            task = PythonOperator(
                task_id=f"lambda_task_{i}",
                python_callable=lambda_task,
                op_kwargs={"start_index": start_idx, "end_index": end_idx},
            )
            tasks.append(task)
        return tasks

    with TaskGroup("invoke_lambda_group") as invoke_lambda_group:
        lambda_tasks = invoke_lambda_tasks()

    read_last_indices_task >> branch_task
    branch_task >> dummy_end

    for task in lambda_tasks:
        branch_task >> task
