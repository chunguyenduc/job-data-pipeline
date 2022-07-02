import datetime
from urllib.parse import urljoin

import pandas as pd
import scrapy

JOB_FIELD = ["title", "company", "skills", "url", "created_at"]
FILE_NAME = "job"
FORMAT = "csv"


class JobSpider(scrapy.Spider):
    name = "job"
    base_url = "https://itviec.com"

    def start_requests(self):
        urls = [
            "https://itviec.com/it-jobs?page=%s&query=&source=search_job" % page
            for page in range(1, 3)
        ]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)
        self.df = pd.DataFrame(columns=JOB_FIELD)

    def parse(self, response):
        first_group = response.xpath('//div[@id="search-results"]')
        jobs = first_group.xpath('.//div[@id="jobs"]')
        job_content = jobs.xpath('.//div[@class="job_content"]')

        job_body = job_content.xpath('.//div[@class="job__body"]')
        job_bottom = job_content.xpath('.//div[@class="job-bottom"]')
        job_logo = job_content.xpath('.//div[@class="logo"]')

        for (body, bottom, logo) in zip(job_body, job_bottom, job_logo):
            title = body.xpath(".//h3/a/text()").get()
            skills = bottom.xpath(".//a/span/text()").getall()
            city = body.xpath('.//div[@class="city"]/div/text()').get()
            url = urljoin(self.base_url, body.xpath(".//h3/a/@href").get())
            company = logo.xpath(".//img/@alt").get()[:-11]
            distance_time = bottom.xpath(
                './/div[@class="distance-time-job-posted"]/span/text()'
            ).get()
            created_at = self.get_created_time(distance_time)
            skill_items = []
            for s in skills:
                skill_items.append(s)
            df_add = pd.DataFrame(
                [[title, company, skills, url, created_at]], columns=JOB_FIELD
            )
            self.df = pd.concat([self.df, df_add], ignore_index=True)
        print(self.df.head())

        self.df.to_csv(self.create_file(), index=False)

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

    def create_file(self):
        return "{}-{}.{}".format(
            FILE_NAME, datetime.datetime.now().strftime("%d%m%y-%I%M"), FORMAT
        )
