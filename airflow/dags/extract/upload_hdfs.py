from hdfs import InsecureClient

DATA_DIR_JOB = "job"
DATA_DIR_JOB_SKILL = "job_skill"


def upload_hdfs(crawl_time):

    print("Filename: ", crawl_time)
    client = InsecureClient("http://namenode:50070", user="root")
    if client.content(DATA_DIR_JOB, strict=False) is None:
        client.makedirs(DATA_DIR_JOB)
    if client.content(DATA_DIR_JOB_SKILL, strict=False) is None:
        client.makedirs(DATA_DIR_JOB_SKILL)

    # filename_job = f"/opt/airflow/dags/job-{crawl_time}.csv"
    client.upload(DATA_DIR_JOB, f"/opt/airflow/dags/job-{crawl_time}.csv")

    # filename_job = f"/opt/airflow/dags/job-{crawl_time}.csv"
    client.upload(DATA_DIR_JOB_SKILL, f"/opt/airflow/dags/job_skill-{crawl_time}.csv")

    # import os

    # os.system(f"rm {filename}")
