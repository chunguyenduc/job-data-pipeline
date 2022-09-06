import logging
from datetime import datetime
from urllib.parse import urljoin

import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess
from utils.extract_helper import JOB_FIELD, JOB_SKILL_FIELD
from utils.extract_helper import get_data_to_csv, get_created_time, \
    get_id, write_data_to_csv


crawl_time = datetime.now().strftime("%d%m%y-%H%M")


class JobSpider(scrapy.Spider):
    """Crawler by Scrapy
    """
    name = "job"
    base_url = "https://itviec.com"

    def __init__(self):
        self.df_job = pd.DataFrame(columns=JOB_FIELD)
        self.df_job_skill = pd.DataFrame(columns=JOB_SKILL_FIELD)

    def start_requests(self):
        urls = [
            f"https://itviec.com/it-jobs?page={page}&query=&source=search_job"
            for page in range(1, 2)
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

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
            job_id = get_id(url)
            company = logo.css("img::attr(alt)").get()[:-11]
            distance_time = bottom.css(
                "div.distance-time-job-posted span::text").get()
            created_date = get_created_time(
                distance_time).strftime("%Y-%m-%d")

            self.df_job, self.df_job_skill = get_data_to_csv(
                job_id, title, company, city, url, created_date, skills)
        logging.info(self.df_job.head())
        logging.info(self.df_job_skill.head())
        write_data_to_csv(self.df_job, crawl_time)
        write_data_to_csv(self.df_job_skill, crawl_time)


def crawl_data():
    """Crawl data then output to csv

    Returns:
        None
    """
    process = CrawlerProcess()
    process.crawl(JobSpider)
    process.start()
    return crawl_time
