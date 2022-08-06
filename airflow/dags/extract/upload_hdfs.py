from hdfs import InsecureClient
import logging

DATA_DIR_JOB = "job"
DATA_DIR_JOB_SKILL = "job_skill"


def upload_hdfs(crawl_time):

    logging.info(f"Filename: {crawl_time}")
    client = InsecureClient("http://namenode:50070", user="root")
    if client.content(DATA_DIR_JOB, strict=False) is None:
        client.makedirs(DATA_DIR_JOB)
    if client.content(DATA_DIR_JOB_SKILL, strict=False) is None:
        client.makedirs(DATA_DIR_JOB_SKILL)

    filename_job = f"/opt/airflow/dags/job-{crawl_time}.csv"
    if client.content(filename_job, strict=False) is None:
        client.upload(DATA_DIR_JOB, filename_job)

    filename_job_skill = f"/opt/airflow/dags/job_skill-{crawl_time}.csv"
    if client.content(filename_job_skill, strict=False) is None:
        client.upload(DATA_DIR_JOB_SKILL, filename_job_skill)

    import os
    os.system(f"rm {filename_job} || true")
    os.system(f"rm {filename_job_skill} || true")
