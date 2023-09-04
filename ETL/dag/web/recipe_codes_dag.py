from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.utils.task_group import TaskGroup
import time
import json

# Import the function from src.py file.
from src.s3_load_index import get_last_page_from_s3

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 8, 30),
    "catchup": False,
}

page_cnt = 200
lambda_count = 6
want_page = 2500


def lambda_task(start_page: str, end_page: str):
    start_page = int(start_page)
    end_page = int(end_page)

    hook = LambdaHook(aws_conn_id="aws_conn_id", region_name="ap-northeast-2")

    payload = {"start_page": start_page, "end_page": end_page}

    # Convert the dict to a JSON string
    payload_json = json.dumps(payload)

    print(f"Payload: {payload_json}")

    response_from_lambda = hook.invoke_lambda(
        function_name="*******************",
        payload=payload_json,
        invocation_type="Event",
    )

    if response_from_lambda["StatusCode"] == 202:
        print(f"Lambda task started on pages {start_page}~{end_page}")
    else:
        print("Lambda task failed to start.")


def check_page_number(page_number):
    if int(page_number) >= want_page:
        return "dummy_end"
    else:
        return [f"invoke_lambda_group.lambda_task_{i}" for i in range(lambda_count)]


with DAG(
    "web_recipe_code_lambda_dag",
    default_args=default_args,
    schedule_interval="0,20,40 * * * *",
) as dag:
    # Define a task to read the index page from S3 bucket
    def read_index_file():
        bucket_name = "de-2-3-airflow"
        index_file_key = "test/recipe/page_index/index_page.txt"

        last_pages_num = get_last_page_from_s3(bucket_name, index_file_key)

        if last_pages_num is not None:
            return last_pages_num

    read_index_task = PythonOperator(
        task_id="read_index", python_callable=read_index_file, do_xcom_push=True
    )

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=check_page_number,
        op_kwargs={"page_number": "{{ ti.xcom_pull(task_ids='read_index') }}"},
    )

    dummy_end = DummyOperator(task_id="dummy_end")

    # @dag.task_group(start_after=branch_task)
    def invoke_lambda_tasks():
        tasks = []
        for i in range(lambda_count):
            start_page = (
                "{{ (ti.xcom_pull(task_ids='read_index') or 0) + "
                + str(i * page_cnt)
                + " }}"
            )
            end_page = (
                "{{ (ti.xcom_pull(task_ids='read_index') or 0) + "
                + str((i + 1) * page_cnt - 1)
                + " }}"
            )

            task = PythonOperator(
                task_id=f"lambda_task_{i}",
                python_callable=lambda_task,
                op_kwargs={"start_page": start_page, "end_page": end_page},
            )
            tasks.append(task)
        return tasks

    with TaskGroup("invoke_lambda_group") as invoke_lambda_group:
        lambda_tasks = invoke_lambda_tasks()

    read_index_task >> branch_task
    branch_task >> dummy_end

    for task in lambda_tasks:
        branch_task >> task

"""
    branch_task >> dummy_end

    # Define a new operator to call the invoke_lambda function.
    invoke_lambda_op = PythonOperator(
        task_id="invoke_lambda",
        python_callable=invoke_lambda)

    # Add a new edge from the branch operator to the invoke_lambda_op.
    branch_task >> invoke_lambda_op
"""
