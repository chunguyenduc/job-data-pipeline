from hdfs import InsecureClient
import requests
import os

DATA_DIR = "job"


def upload_to_hdfs(filename):
    client = InsecureClient("http://namenode:9870", user="root")
    if client.content(DATA_DIR, strict=False) is None:
        client.makedirs(DATA_DIR)

    # client.makedirs("temp")
    # client.set_permission(DATA_DIR, permission=777)
    client.upload(DATA_DIR, filename)

    # As a context manager:
    # with client.write("/records.jsonl", encoding="utf-8") as writer:
