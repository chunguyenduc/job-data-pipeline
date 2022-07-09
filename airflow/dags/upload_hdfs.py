from hdfs import InsecureClient
import requests
import os

DATA_DIR = "job"


def upload_to_hdfs(filename):
    client = InsecureClient("http://namenode:9870", user="root")
    if client.content(DATA_DIR, strict=False) is None:
        client.makedirs(DATA_DIR)

    client.upload(DATA_DIR, filename)
