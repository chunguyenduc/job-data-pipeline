import datetime
import sys
from urllib.parse import urljoin

import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess

time_crawl = sys.argv[1]

JOB_FIELD = ["title", "company", "city", "skills", "url", "created_at"]
PREFIX = "job"
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
        self.df = pd.DataFrame(columns=JOB_FIELD)

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
            company = logo.css("img::attr(alt)").get()[:-11]
            distance_time = bottom.css("div.distance-time-job-posted span::text").get()
            created_at = self.get_created_time(distance_time)
            skill_items = []
            for s in skills:
                skill_items.append(s)
            df_add = pd.DataFrame(
                [[title, company, city, skills, url, created_at]], columns=JOB_FIELD
            )
            self.df = pd.concat([self.df, df_add], ignore_index=True)
        print(self.df.head())

        self.df.to_csv(self.get_filename(), index=False)

    def get_created_time(self, distance_time):
        """
        Get created time from distance time.
        ie: 5h -> now-5h
        """
        # Process distance time
        distance_time = distance_time.strip("\n")
        time_now = datetime.datetime.now()
        print("Time now: ", time_now)

        # case minute
        if distance_time[-1] == "m":
            minute = int(distance_time[:-1])
            minute_subtracted = datetime.timedelta(minutes=minute)
            created = time_now - minute_subtracted
            return created
        # case hour
        elif distance_time[-1] == "h":
            hour = int(distance_time[:-1])
            hour_subtracted = datetime.timedelta(hours=hour)
            created = time_now - hour_subtracted
            return created
        # case day
        elif distance_time[-1] == "d":
            day = int(distance_time[:-1])
            day_subtracted = datetime.timedelta(days=day)
            created = time_now - day_subtracted
            return created
        return time_now

    def get_filename(self):
        return f"{PREFIX}-{time_crawl}.{FORMAT}"


process = CrawlerProcess(
    settings={
        "FEEDS": {
            "items.json": {"format": "json"},
        },
    }
)

process.crawl(JobSpider)
process.start()
