import configparser
import logging
import os
import pathlib
from datetime import datetime, timedelta

from extract.job_spider import crawl_data
from utils import create_tables
from utils.extract_helper import PREFIX_JOB, PREFIX_JOB_SKILL

from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.sensors.filesystem import FileSensor

# Read Configuration File
parser = configparser.ConfigParser()
script_path = pathlib.Path(__file__).parent.resolve()
config_file = "configuration.conf"
parser.read(f"{script_path}/{config_file}")


BUCKET_NAME = parser.get("aws_config", "bucket_name")
AWS_REGION = parser.get("aws_config", "aws_region")
IAM_ROLE = parser.get("aws_config", "iam_role")


def upload_s3(crawl_time: str, prefix: str):
    filename_job = f"/opt/airflow/dags/{prefix}-{crawl_time}.csv"
    key = f"{prefix}/{prefix}-{crawl_time}.csv"
    logging.info("filename: %s ", filename_job)
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename_job, key=key, bucket_name='duccn-bucket')

    if os.path.exists(filename_job):
        os.remove(filename_job)


with DAG(
    "job_data_pipeline",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
    description="Data pipeline crawl job description from itviec",
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

    job_data_to_data_lake = PythonOperator(
        task_id="job_data_to_data_lake",
        python_callable=upload_s3,
        op_kwargs={
            "crawl_time": "{{ task_instance.xcom_pull(task_ids='extract_job_data') }}",
            "prefix": PREFIX_JOB
        },
        retries=3,
        retry_delay=timedelta(seconds=30),
        dag=dag,
        depends_on_past=False,
    )

    job_skill_data_to_data_lake = PythonOperator(
        task_id="job_skill_data_to_data_lake",
        python_callable=upload_s3,
        op_kwargs={
            "crawl_time": "{{ task_instance.xcom_pull(task_ids='extract_job_data') }}",
            "prefix": PREFIX_JOB_SKILL
        },
        retries=3,
        retry_delay=timedelta(seconds=30),
        dag=dag,
        depends_on_past=False,
    )

    job_data_to_staging = RedshiftSQLOperator(
        task_id="job_data_to_staging",
        redshift_conn_id="redshift",
        sql=[
            create_tables.create_stg_schema,
            create_tables.drop_stg_job_table,
            create_tables.create_stg_job_table,
            """
            COPY staging.job_info (id, title, company, city, url, created_date, created_time)
            FROM 's3://duccn-bucket/job/job-{{ ti.xcom_pull(task_ids='extract_job_data') }}.csv'
            REGION 'us-east-1' IAM_ROLE 'arn:aws:iam::191513327969:role/service-role/AmazonRedshift-CommandsAccessRole-20221217T160944' 
            DELIMITER ','
            IGNOREHEADER 1
            REMOVEQUOTES
            EMPTYASNULL
            BLANKSASNULL;
        """],
    )

    job_skill_data_to_staging = RedshiftSQLOperator(
        task_id="job_skill_data_to_staging",
        redshift_conn_id="redshift",
        sql=[
            create_tables.create_stg_schema,
            create_tables.drop_stg_job_skill_table,
            create_tables.create_stg_job_skill_table,
            """
            COPY staging.job_skill (id, skill, created_date, created_time)
            FROM 's3://duccn-bucket/job_skill/job_skill-{{ ti.xcom_pull(task_ids='extract_job_data') }}.csv'
            REGION 'us-east-1' IAM_ROLE 'arn:aws:iam::191513327969:role/service-role/AmazonRedshift-CommandsAccessRole-20221217T160944' 
            DELIMITER ',' 
            IGNOREHEADER 1;
        """],
    )

    job_data_staging_to_public = RedshiftSQLOperator(
        task_id="job_data_staging_to_public",
        redshift_conn_id="redshift",
        sql=[
            create_tables.create_public_job_table,
            create_tables.insert_public_job
        ],
    )

    job_skill_data_staging_to_public = RedshiftSQLOperator(
        task_id="job_skill_data_staging_to_public",
        redshift_conn_id="redshift",
        sql=[
            create_tables.create_public_job_skill_table,
            create_tables.insert_public_job_skill
        ],
    )

    end_operator = DummyOperator(task_id='finish-execution', dag=dag)

start_operator >> extract_data >> [waiting_for_job,
                                   waiting_for_job_skill]

waiting_for_job >> job_data_to_data_lake
waiting_for_job_skill >> job_skill_data_to_data_lake

job_data_to_data_lake >> job_data_to_staging
job_skill_data_to_data_lake >> job_skill_data_to_staging

job_data_to_staging >> job_data_staging_to_public
job_skill_data_to_staging >> job_skill_data_staging_to_public

[job_data_staging_to_public, job_skill_data_staging_to_public] >> end_operator
