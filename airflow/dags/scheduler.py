from datetime import datetime, timedelta
from extract.upload_hdfs import upload_hdfs
from extract.job_spider import crawl_data
from transform.transform import transform_insert_staging
from load.load import load_data


from airflow import DAG
from airflow.operators.python import PythonOperator

dag_path = "/usr/local/airflow/dags"
crawl_path = f"{dag_path}/extract"

with DAG(
    "job_dashboard",
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

    upload_to_hdfs = PythonOperator(
        task_id="upload_data_to_hdfs",
        python_callable=upload_hdfs,
        op_kwargs={
            "crawl_time": "{{ task_instance.xcom_pull(task_ids='crawl_job_data') }}"},
        retries=3,
        retry_delay=timedelta(seconds=30),
        dag=dag,
        depends_on_past=False,
    )
    transform = PythonOperator(
        task_id="transform_and_insert_staging",
        python_callable=transform_insert_staging,
        op_kwargs={
            "crawl_time": "{{ task_instance.xcom_pull(task_ids='crawl_job_data') }}"},
        retries=3,
        retry_delay=timedelta(seconds=5),
        dag=dag,
        depends_on_past=False,
    )

    load_data = PythonOperator(
        task_id="load_data_to_hive",
        python_callable=load_data,
        retries=3,
        retry_delay=timedelta(seconds=5),
        dag=dag,
        depends_on_past=False,
    )
crawl_job >> upload_to_hdfs >> transform >> load_data
