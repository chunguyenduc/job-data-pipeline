import configparser
import logging
import os
import pathlib
import sys
from datetime import datetime, timedelta

from extract.job_spider import crawl_data
from utils import queries
from utils.extract_helper import PREFIX_JOB, PREFIX_JOB_SKILL

from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.sql import SQLCheckOperator
from airflow.providers.amazon.aws.operators.redshift_sql import \
    RedshiftSQLOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

# Read Configuration File
parser = configparser.ConfigParser()
script_path = pathlib.Path(__file__).parent.resolve()
config_path = os.path.join(script_path, "configuration.conf")
logging.info(config_path)
if os.path.exists(config_path):
    logging.info(f'Configuration exists: {config_path}')
    parser.read(config_path)


BUCKET_NAME = parser.get("aws_config", "bucket_name")
AWS_REGION = parser.get("aws_config", "aws_region")
IAM_ROLE = parser.get("aws_config", "iam_role")
REDSHIFT_CONN_ID = parser.get("aws_config", "redshift_conn_id")
ALERT_EMAIL = parser.get("config", "email")


def upload_s3(crawl_time: str, prefix: str):
    filename_job = f"/opt/airflow/dags/{prefix}-{crawl_time}.csv"
    key = f"{prefix}/{prefix}-{crawl_time}.csv"
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename_job, key=key, bucket_name=BUCKET_NAME)

    if os.path.exists(filename_job):
        os.remove(filename_job)


with DAG(
    "job_data_pipeline",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
        'email': [ALERT_EMAIL],
        'email_on_failure': True
    },
    description="Data pipeline crawl job from ITViec",
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["job"],
    dagrun_timeout=timedelta(minutes=10)
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
        redshift_conn_id=REDSHIFT_CONN_ID,
        sql=[
            queries.create_stg_schema,
            queries.drop_stg_job_table,
            queries.create_stg_job_table,
            queries.insert_job
        ],
        params={
            'bucket': BUCKET_NAME,
            'iam_role': IAM_ROLE,
            'region': AWS_REGION,
        },
        dag=dag
    )

    job_skill_data_to_staging = RedshiftSQLOperator(
        task_id="job_skill_data_to_staging",
        redshift_conn_id=REDSHIFT_CONN_ID,
        sql=[
            queries.create_stg_schema,
            queries.drop_stg_job_skill_table,
            queries.create_stg_job_skill_table,
            queries.insert_job_skill
        ],
        params={
            'bucket': BUCKET_NAME,
            'iam_role': IAM_ROLE,
            'region': AWS_REGION,
        },
        dag=dag
    )

    job_data_staging_to_public = RedshiftSQLOperator(
        task_id="job_data_staging_to_public",
        redshift_conn_id=REDSHIFT_CONN_ID,
        sql=[
            queries.create_public_job_table,
            queries.insert_public_job
        ],
    )

    job_skill_data_staging_to_public = RedshiftSQLOperator(
        task_id="job_skill_data_staging_to_public",
        redshift_conn_id=REDSHIFT_CONN_ID,
        sql=[
            queries.create_public_job_skill_table,
            queries.insert_public_job_skill
        ],
    )

    with TaskGroup(group_id="data_quality_check") as data_quality_check:
        job_data_quality_check = SQLCheckOperator(
            task_id='job_data_quality_check',
            conn_id=REDSHIFT_CONN_ID,
            sql=queries.sql_check_job,
        )

        job_skill_data_quality_check = SQLCheckOperator(
            task_id='job_skill_data_quality_check',
            conn_id=REDSHIFT_CONN_ID,
            sql=queries.sql_check_skill,
        )
    end_operator = DummyOperator(task_id='finish-execution', dag=dag)

start_operator >> extract_data >> [waiting_for_job,
                                   waiting_for_job_skill]

waiting_for_job >> job_data_to_data_lake
waiting_for_job_skill >> job_skill_data_to_data_lake

job_data_to_data_lake >> job_data_to_staging
job_skill_data_to_data_lake >> job_skill_data_to_staging

[job_data_to_staging, job_skill_data_to_staging] >> data_quality_check >> [
    job_data_staging_to_public, job_skill_data_staging_to_public]

[job_data_staging_to_public, job_skill_data_staging_to_public] >> end_operator
