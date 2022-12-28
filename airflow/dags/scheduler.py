import logging
import os
from datetime import datetime, timedelta

from extract.job_spider import crawl_data
from load.load import load_data
from transform.transform import transform_insert_staging
from utils import create_tables
from utils.extract_helper import PREFIX_JOB, PREFIX_JOB_SKILL

from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor


def upload_s3(crawl_time: str, prefix: str):
    filename_job = f"/opt/airflow/dags/{prefix}-{crawl_time}.csv"
    key = f"{prefix}/{prefix}-{crawl_time}.csv"
    logging.info(f"filename: {filename_job}")
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename_job, key=key, bucket_name='duccn-bucket')

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

    create_staging_table = RedshiftSQLOperator(
        redshift_conn_id='redshift',
        task_id='create_staging_table',
        sql=[
            create_tables.create_stg_schema,
            create_tables.drop_stg_job_table,
            create_tables.create_stg_job_table,
            create_tables.create_stg_job_skill_table
        ]
    )

    job_data_to_staging = RedshiftSQLOperator(
        task_id="job_data_to_staging",
        redshift_conn_id="redshift",
        sql="""
            COPY staging.job_info (id, title, company, city, url, created_date)
            FROM 's3://duccn-bucket/job/job-{{ ti.xcom_pull(task_ids='extract_job_data') }}.csv'
            REGION 'us-east-1' IAM_ROLE 'arn:aws:iam::191513327969:role/service-role/AmazonRedshift-CommandsAccessRole-20221217T160944' 
            DELIMITER ','
            IGNOREHEADER 1
            REMOVEQUOTES
            EMPTYASNULL
            BLANKSASNULL;
        """,
    )

    job_skill_data_to_staging = RedshiftSQLOperator(
        task_id="job_skill_data_to_staging",
        redshift_conn_id="redshift",
        sql="""
            COPY staging.job_skill (id, skill, created_date)
            FROM 's3://duccn-bucket/job_skill/job_skill-{{ ti.xcom_pull(task_ids='extract_job_data') }}.csv'
            REGION 'us-east-1' IAM_ROLE 'arn:aws:iam::191513327969:role/service-role/AmazonRedshift-CommandsAccessRole-20221217T160944' 
            DELIMITER ',' 
            IGNOREHEADER 1;
        """,
    )

    create_public_tables = RedshiftSQLOperator(
        task_id="create_public_tables",
        redshift_conn_id="redshift",
        sql=[
            create_tables.create_public_job_table,
            create_tables.create_public_job_skill_table
        ]
    )

    job_data_staging_to_public = RedshiftSQLOperator(
        task_id="job_data_staging_to_public",
        redshift_conn_id="redshift",
        sql="""
            INSERT INTO public.job_info (id, title, company, city, url, created_date, insert_time) 
            SELECT sub.id, sub.title, sub.company, sub.city, sub.url, sub.created_date, sub.insert_time FROM staging.job_info AS sub 
            LEFT OUTER JOIN public.job_info AS pub ON sub.id = pub.id 
            WHERE pub.id is NULL;
        """,
    )

    job_skill_data_staging_to_public = RedshiftSQLOperator(
        task_id="job_skill_data_staging_to_public",
        redshift_conn_id="redshift",
        sql="""
            INSERT INTO public.job_skill (id, skill, insert_time, created_date)
            SELECT sub.id, sub.skill, sub.insert_time, sub.created_date FROM staging.job_skill AS sub 
            LEFT OUTER JOIN public.job_skill AS pub ON sub.id = pub.id 
            WHERE pub.id is NULL;
        """,
    )

    end_operator = DummyOperator(task_id='finish-execution', dag=dag)

start_operator >> extract_data >> [waiting_for_job,
                                   waiting_for_job_skill]

waiting_for_job >> job_data_to_data_lake
waiting_for_job_skill >> job_skill_data_to_data_lake

[job_data_to_data_lake, job_skill_data_to_data_lake] >> create_staging_table

create_staging_table >> [job_data_to_staging,
                         job_skill_data_to_staging]

[job_data_to_staging, job_skill_data_to_staging] >> create_public_tables

create_public_tables >> [job_data_staging_to_public,
                         job_skill_data_staging_to_public] >> end_operator
