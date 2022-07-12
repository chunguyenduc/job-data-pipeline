from hdfs import InsecureClient

DATA_DIR = "job"


def upload_hdfs(crawl_time):
    print("Filename: ", crawl_time)
    client = InsecureClient("http://namenode:50070", user="root")
    if client.content(DATA_DIR, strict=False) is None:
        client.makedirs(DATA_DIR)

    client.upload(DATA_DIR, f"/usr/local/airflow/dags/extract/{crawl_time}")
    # os.system(f"rm {filename}")
