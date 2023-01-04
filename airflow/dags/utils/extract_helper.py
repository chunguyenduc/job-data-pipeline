import csv
import json
import logging
from datetime import datetime, timedelta
from typing import List

import pandas as pd

JOB_FIELD = ["id", "title", "company", "city",
             "url", "created_date", "created_time"]
JOB_SKILL_FIELD = ["id", "skill", "created_date", "created_time"]

DAG_PATH = "/opt/airflow/dags"
PREFIX_JOB = "job"
PREFIX_JOB_SKILL = "job_skill"
FORMAT_CSV = "csv"
FORMAT_JSON = "json"


def get_created_time(time_now: datetime, distance_time: str) -> datetime:
    """
    Get created time from distance time.
    ie: 5h -> now-5h
    """
    # Process distance time
    distance_time = distance_time.strip("\n")
    created = time_now

    # case minute
    if distance_time[-1] == "m":
        minute = int(distance_time[:-1])
        minute_subtracted = timedelta(minutes=minute)
        created = time_now - minute_subtracted
    # case hour
    elif distance_time[-1] == "h":
        hour = int(distance_time[:-1])
        hour_subtracted = timedelta(hours=hour)
        created = time_now - hour_subtracted
    # case day
    elif distance_time[-1] == "d":
        day = int(distance_time[:-1])
        day_subtracted = timedelta(days=day)
        created = time_now - day_subtracted
    return created


def get_filename(crawl_time: str, prefix: str, format_type: str) -> str:
    return f"{DAG_PATH}/{prefix}-{crawl_time}.{format_type}"


def write_data_to_csv(data, crawl_time: str, prefix: str, columns: List) -> None:
    filename = get_filename(crawl_time, prefix, FORMAT_CSV)
    try:
        with open(filename, 'w') as file:
            writer = csv.DictWriter(file, fieldnames=columns)
            writer.writeheader()
            for d in data:
                writer.writerow(d)
    except IOError:
        logging.error("I/O error: %s", filename)


def get_id(url: str) -> str:
    url_no_param = url[:url.find("?")]
    return url_no_param.split("/")[-1]
