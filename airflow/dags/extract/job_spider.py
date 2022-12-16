import json
import logging
from datetime import datetime
from urllib.parse import urljoin

import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess
from utils.extract_helper import (JOB_FIELD, PREFIX_JOB, get_created_time,
                                  get_data_to_csv, get_id, write_data_to_json)

crawl_time = datetime.now().strftime("%d%m%y-%H%M")


class JobSpider(scrapy.Spider):
    """Crawler by Scrapy
    """
    name = "job"
    base_url = "https://itviec.com"

    def __init__(self):
        self.df_job = pd.DataFrame(columns=JOB_FIELD)
        self.data = []

    def start_requests(self):
        urls = [
            "https://itviec.com/it-jobs"
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
                datetime.now(), distance_time).strftime("%Y-%m-%d")
            skills_cleaned = [s.replace("\n", "") for s in skills]
            row_job = get_data_to_csv(
                job_id, title, company, city,
                url, created_date, skills_cleaned)
            self.df_job = pd.concat(
                [self.df_job, row_job], ignore_index=True
            )
            self.data.append({
                "id": job_id,
                "title": title,
                "company": company,
                "city": city,
                "url": url,
                "created_date": created_date,
                "skills": skills_cleaned
            })

        logging.info(self.data)
        write_data_to_json(self.data, crawl_time, PREFIX_JOB)


def crawl_data():
    """Crawl data then output to csv

    Returns:
        None
    """
    process = CrawlerProcess()
    process.crawl(JobSpider)
    process.start()
    return crawl_time
