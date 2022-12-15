import logging
import boto3
BUCKET_NAME = "duccn-bucket"


def upload_file_to_s3(crawl_time: str):
    filename_job = f"/opt/airflow/dags/job-{crawl_time}.csv"
    key = f"job-{crawl_time}.csv"
    logging.info(f"filename: {filename_job}")
    conn = boto3.resource("s3")

    conn.meta.client.upload_file(
        Filename=filename_job, Bucket=BUCKET_NAME, Key=key
    )
