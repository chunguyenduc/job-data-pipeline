import logging
from datetime import datetime
from urllib.parse import urljoin

import scrapy
from scrapy.crawler import CrawlerProcess
from utils.extract_helper import (PREFIX_JOB, PREFIX_JOB_SKILL, JOB_FIELD,
                                  JOB_SKILL_FIELD, get_created_time, get_id, write_data_to_csv)

crawl_time = datetime.now().strftime("%d%m%y-%H%M")


class JobSpider(scrapy.Spider):
    """Crawler by Scrapy
    """
    name = "job"
    base_url = "https://itviec.com"

    def __init__(self):
        self.data = []
        self.data_skills = []

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
            created_time = get_created_time(
                datetime.now(), distance_time)
            created_date = created_time.strftime("%Y-%m-%d")
            skills = [s.replace("\n", "") for s in skills]
            self.data.append({
                "id": job_id,
                "title": title,
                "company": company,
                "city": city,
                "url": url,
                "created_date": created_date,
                "created_time": created_time
            })
            for skill in skills:
                self.data_skills.append({
                    "id": job_id,
                    "skill": skill,
                    "created_date": created_date,
                    "created_time": created_time
                })

        logging.info(self.data)
        logging.info(self.data_skills)
        if len(self.data) == 0 or len(self.data_skills) == 0:
            raise Exception("No data crawled")
        write_data_to_csv(self.data, crawl_time, PREFIX_JOB, columns=JOB_FIELD)
        write_data_to_csv(self.data_skills, crawl_time,
                          PREFIX_JOB_SKILL, columns=JOB_SKILL_FIELD)


def crawl_data():
    """Crawl data then output to csv

    Returns:
        None
    """
    process = CrawlerProcess()
    process.crawl(JobSpider)
    process.start()

    return crawl_time
