from datetime import datetime, timedelta
from extract.upload_hdfs import upload_hdfs

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

crawl_time = datetime.now().strftime("%d%m%y-%H%M")
dag_path = "/usr/local/airflow/dags"
crawl_path = "/usr/local/airflow/extract"

with DAG(
    "job_dashboard",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
    description="ETL pipeline crawl job description from itviec",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["job"],
) as dag:

    crawl_job = BashOperator(
        task_id="crawl_job_data",
        bash_command=f"python3 {crawl_path}/job_spider.py {crawl_time}",
        retries=3,
        retry_delay=timedelta(seconds=30),
        dag=dag,
    )

    upload_to_hdfs = PythonOperator(
        task_id="upload_data_to_hdfs",
        python_callable=upload_hdfs,
        op_kwargs={"filename": f"{crawl_path}/{crawl_time}"},
        retries=3,
        retry_delay=timedelta(seconds=30),
        dag=dag,
        depends_on_past=False,
    )
crawl_job >> upload_to_hdfs
