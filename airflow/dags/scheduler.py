import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

craw_time = datetime.now().strftime("%d%m%y-%I%M")
dag_path = "/usr/local/airflow/dags"
crawl_path = os.path.join(dag_path, "spiders")
file_name = f"job-{craw_time}.csv"

with DAG(
    "job_dashboard",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
    description="ETL pipeline crawl job description from itviec",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime.now(),
    catchup=False,
    tags=["job"],
) as dag:

    crawl_job = BashOperator(
        task_id="crawl_job_data",
        bash_command=f"python3 {os.path.join(crawl_path, 'job_spider.py')} {craw_time}",
        retries=3,
        retry_delay=timedelta(seconds=30),
    )

    upload_to_hdfs = BashOperator(
        task_id="upload_data_to_hdfs",
        bash_command=f"{os.path.join(dag_path, 'upload_hdfs.sh')} {os.path.join(crawl_path, file_name)} {file_name}",
        retries=3,
        retry_delay=timedelta(seconds=30),
    )
crawl_job >> upload_to_hdfs
