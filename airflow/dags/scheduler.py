import logging
from datetime import datetime, timedelta

from extract.job_spider import crawl_data
# from extract.upload_s3 import upload_file_to_s3
from load.load import load_data
from transform.transform import transform_insert_staging

from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.providers.amazon.aws.operators.s3_bucket import \
    S3CreateBucketOperator
from airflow.sensors.filesystem import FileSensor


def upload_s3(crawl_time: str, prefix: str):
    filename_job = f"/opt/airflow/dags/job-{crawl_time}.csv"
    key = f"{prefix}/job-{crawl_time}.csv"
    logging.info(f"filename: {filename_job}")
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename_job, key=key, bucket_name='duccn-bucket')
    import os
    if os.path.exists(filename_job):
        os.remove(filename_job)


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

    start_operator = DummyOperator(task_id='begin-execution', dag=dag)

    extract_data = PythonOperator(
        task_id="extract_job_data",
        python_callable=crawl_data,
        retries=3,
        retry_delay=timedelta(seconds=30),
        dag=dag,
    )

    waiting_for_job = FileSensor(
        task_id="waiting_extract_job_data_file",
        poke_interval=30,
        filepath="job-{{ task_instance.xcom_pull(task_ids='extract_job_data') }}.csv"
    )

    waiting_for_job_skill = FileSensor(
        task_id="waiting_extract_data_skill_data_file",
        poke_interval=30,
        filepath="job_skill-{{ task_instance.xcom_pull(task_ids='extract_job_data') }}.csv"
    )

    # create_bucket = S3CreateBucketOperator(
    #     task_id='s3_bucket_dag_create',
    #     bucket_name="duccn-bucket",
    #     region_name='us-east-1',
    #     aws_conn_id='s3_conn'
    # )

    upload_job_s3 = PythonOperator(
        task_id="upload_job_data_to_s3",
        python_callable=upload_s3,
        op_kwargs={
            "crawl_time": "{{ task_instance.xcom_pull(task_ids='extract_job_data') }}"},
        retries=3,
        retry_delay=timedelta(seconds=30),
        dag=dag,
        depends_on_past=False,
    )

    upload_job_skill_s3 = PythonOperator(
        task_id="upload_job_skill_data_to_s3",
        python_callable=upload_s3,
        op_kwargs={
            "crawl_time": "{{ task_instance.xcom_pull(task_ids='extract_job_data') }}"},
        retries=3,
        retry_delay=timedelta(seconds=30),
        dag=dag,
        depends_on_past=False,
    )

    create_staging_table = RedshiftSQLOperator(
        redshift_conn_id='redshift',
        task_id='create_staging_table',
        sql="utils/create_tables.sql",
    )

    end_operator = DummyOperator(task_id='finish-execution', dag=dag)

start_operator >> extract_data >> [waiting_for_job,
                                   waiting_for_job_skill]

waiting_for_job >> upload_job_s3
waiting_for_job_skill >> upload_job_skill_s3

[upload_job_s3, upload_job_skill_s3] >> create_staging_table >> end_operator
