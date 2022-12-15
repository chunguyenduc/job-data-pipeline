from datetime import datetime, timedelta
from typing import List, Tuple
import pandas as pd


JOB_FIELD = ["id", "title", "company", "city", "url", "skill", "created_date"]
JOB_SKILL_FIELD = ["id", "skill", "created_date"]

DAG_PATH = "/opt/airflow/dags"
PREFIX_JOB = "job"
PREFIX_JOB_SKILL = "job_skill"

FORMAT = "csv"


def get_data_to_csv(
        job_id: str,
        title: str,
        company: str,
        city: str,
        url: str,
        created_date: str,
        skills: List[str]) -> pd.DataFrame:
    df_job = pd.DataFrame(columns=JOB_FIELD)
    # df_job_skill = pd.DataFrame(columns=JOB_SKILL_FIELD)
    # for s in skills:
    #     df_add_job_skill = pd.DataFrame(
    #         [[job_id, s.strip(), created_date]],
    #         columns=JOB_SKILL_FIELD
    #     )
    #     df_job_skill = pd.concat(
    #         [df_job_skill, df_add_job_skill], ignore_index=True
    #     )

    df_job = pd.DataFrame(
        [[job_id, title, company, city, url, skills, created_date]],
        columns=JOB_FIELD
    )
    return df_job


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


def get_filename(crawl_time: str, prefix: str) -> str:
    return f"{DAG_PATH}/{prefix}-{crawl_time}.{FORMAT}"


def write_data_to_csv(df: pd.DataFrame, crawl_time: str, prefix: str) -> None:
    df.to_csv(get_filename(crawl_time, prefix), index=False)


def get_id(url: str) -> str:
    url_no_param = url[:url.find("?")]
    return url_no_param.split("/")[-1]
