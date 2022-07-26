from datetime import datetime, timedelta
from urllib.parse import urljoin

import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess

crawl_time = datetime.now().strftime("%d%m%y-%H%M")

JOB_FIELD = ["id", "title", "company", "city", "url", "created_date"]
JOB_SKILL_FIELD = ["id", "skill"]

DAG_PATH = "/opt/airflow/dags"
PREFIX_JOB = "job"
PREFIX_JOB_SKILL = "job_skill"

FORMAT = "csv"


class JobSpider(scrapy.Spider):
    name = "job"
    base_url = "https://itviec.com"

    def start_requests(self):
        urls = [
            f"https://itviec.com/it-jobs?page={page}&query=&source=search_job"
            for page in range(1, 3)
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)
        self.df_job = pd.DataFrame(columns=JOB_FIELD)
        self.df_job_skill = pd.DataFrame(columns=JOB_SKILL_FIELD)

    def parse(self, response):

        first_group = response.css("div#search-results")
        jobs = first_group.css("div#jobs")
        job_content = jobs.css("div.job_content")

        job_body = job_content.css("div.job__body")
        job_bottom = job_content.css("div.job-bottom")
        job_logo = job_content.css("div.logo")

        for (body, bottom, logo) in zip(job_body, job_bottom, job_logo):
            title = body.css("h3 a::text").get()
            skills = bottom.css("a span::text").getall()
            city = body.css("div.city div::text").get()
            url = urljoin(self.base_url, body.css("h3 a::attr(href)").get())
            id = get_id(url)
            company = logo.css("img::attr(alt)").get()[:-11]
            distance_time = bottom.css("div.distance-time-job-posted span::text").get()
            created_date = self.get_created_time(distance_time).strftime("%Y%m%d")
            for s in skills:
                df_add_job_skill = pd.DataFrame(
                    [[id, s.strip()]], columns=JOB_SKILL_FIELD
                )
                self.df_job_skill = pd.concat(
                    [self.df_job_skill, df_add_job_skill], ignore_index=True
                )
            df_add = pd.DataFrame(
                [[id, title, company, city, url, created_date]], columns=JOB_FIELD
            )
            self.df_job = pd.concat([self.df_job, df_add], ignore_index=True)
        print(self.df_job.head())
        print(self.df_job_skill.head())
        self.df_job.to_csv(get_filename(PREFIX_JOB), index=False)
        self.df_job_skill.to_csv(get_filename(PREFIX_JOB_SKILL), index=False)

    def get_created_time(self, distance_time):
        """
        Get created time from distance time.
        ie: 5h -> now-5h
        """
        # Process distance time
        distance_time = distance_time.strip("\n")
        time_now = datetime.now()

        # case minute
        if distance_time[-1] == "m":
            minute = int(distance_time[:-1])
            minute_subtracted = timedelta(minutes=minute)
            created = time_now - minute_subtracted
            return created
        # case hour
        elif distance_time[-1] == "h":
            hour = int(distance_time[:-1])
            hour_subtracted = timedelta(hours=hour)
            created = time_now - hour_subtracted
            return created
        # case day
        elif distance_time[-1] == "d":
            day = int(distance_time[:-1])
            day_subtracted = timedelta(days=day)
            created = time_now - day_subtracted
            return created
        return time_now


def get_filename(prefix: str) -> str:
    return f"{DAG_PATH}/{prefix}-{crawl_time}.{FORMAT}"


def get_id(url: str) -> str:
    url_no_param = url = url[: url.find("?")]
    id = url_no_param.split("-")[-1]
    return id


def crawl_data():
    process = CrawlerProcess()
    process.crawl(JobSpider)
    process.start()
    return crawl_time
