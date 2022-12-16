import logging
from datetime import datetime, timedelta

from extract.job_spider import crawl_data
# from extract.upload_s3 import upload_file_to_s3
from load.load import load_data
from transform.transform import transform_insert_staging

from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3_bucket import \
    S3CreateBucketOperator


def upload_s3(crawl_time: str):
    filename_job = f"/opt/airflow/dags/job-{crawl_time}.json"
    key = f"job-{crawl_time}.json"
    logging.info(f"filename: {filename_job}")
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename_job, key=key, bucket_name='duccn-bucket')


dag_path = "/usr/local/airflow/dags"
crawl_path = f"{dag_path}/extract"

with DAG(
    "job_etl_pipeline",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
    description="ETL pipeline crawl job description from itviec",
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["job"],
) as dag:

    crawl_job = PythonOperator(
        task_id="crawl_job_data",
        python_callable=crawl_data,
        retries=3,
        retry_delay=timedelta(seconds=30),
        dag=dag,
    )

    create_bucket = S3CreateBucketOperator(
        task_id='s3_bucket_dag_create',
        bucket_name="duccn-bucket",
        region_name='us-east-1',
        aws_conn_id='s3_conn'
    )

    upload_to_s3 = PythonOperator(
        task_id="upload_data_to_s3",
        python_callable=upload_s3,
        op_kwargs={
            "crawl_time": "{{ task_instance.xcom_pull(task_ids='crawl_job_data') }}"},
        retries=3,
        retry_delay=timedelta(seconds=30),
        dag=dag,
        depends_on_past=False,
    )
    # transform = PythonOperator(
    #     task_id="transform_and_insert_staging",
    #     python_callable=transform_insert_staging,
    #     op_kwargs={
    #         "crawl_time": "{{ task_instance.xcom_pull(task_ids='crawl_job_data') }}"},
    #     retries=3,
    #     retry_delay=timedelta(seconds=5),
    #     dag=dag,
    #     depends_on_past=False,
    # )

    # load_data = PythonOperator(
    #     task_id="load_data_to_hive",
    #     python_callable=load_data,
    #     retries=3,
    #     retry_delay=timedelta(seconds=5),
    #     dag=dag,
    #     depends_on_past=False,
    # )
crawl_job >> create_bucket >> upload_to_s3
# >> upload_to_hdfs >> transform >> load_data
