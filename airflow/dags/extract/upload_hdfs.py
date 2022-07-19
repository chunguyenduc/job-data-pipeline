from hdfs import InsecureClient

DATA_DIR = "job"


def upload_hdfs(crawl_time):

    print("Filename: ", crawl_time)
    client = InsecureClient("http://namenode:50070", user="root")
    if client.content(DATA_DIR, strict=False) is None:
        client.makedirs(DATA_DIR)
    filename = f"/opt/airflow/dags/job-{crawl_time}.csv"
    client.upload(DATA_DIR, f"/opt/airflow/dags/job-{crawl_time}.csv")

    # import os

    # os.system(f"rm {filename}")
