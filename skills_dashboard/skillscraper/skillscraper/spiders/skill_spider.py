import scrapy
from scrapy import item
from skillscraper.items import SkillscraperItem, Skill
from urllib.parse import urljoin
import datetime
# scrapy crawl skills


class SkillsSpider(scrapy.Spider):
    name = "skills"

    def start_requests(self):
        urls = ['https://itviec.com/it-jobs?page=%s&query=&source=search_job' %
                page for page in range(1, 3)]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)
        # self.f = open('xi_kiu', 'w')
        self.base_url = "https://itviec.com"
    def parse(self, response):
        first_group = response.xpath('//div[@id="search-results"]')

        jobs = first_group.xpath('.//div[@id="jobs"]')

        job_content = jobs.xpath('.//div[@class="job_content"]')

        job_body = job_content.xpath('.//div[@class="job__body"]')
        job_bottom = job_content.xpath('.//div[@class="job-bottom"]')
        job_logo = job_content.xpath('.//div[@class="logo"]')
        for (body, bottom, logo) in zip(job_body, job_bottom, job_logo):
            title = body.xpath('.//h2/a/text()').get()
            skills = bottom.xpath('.//a/span/text()').getall()
            city = body.xpath('.//div[@class="city"]/div/text()').get()
            url = body.xpath('.//h2/a/@href').get()
            skills = self.format_skills(skills)
            company = logo.xpath('.//img/@alt').get()[:-11]
            distance_time = bottom.xpath('.//div[@class="distance-time-job-posted"]/span/text()').get()
            print(distance_time)
            created_at = self.get_created_time(distance_time)
            skill_items = []
            for s in skills:
                # skill = Skill()
                # skill['name'] = s
                skill_items.append(s)

            item = SkillscraperItem()
            item['title'] = title
            item['skills'] = skill_items
            item['city'] = city
            item['company'] = company
            item['url'] = urljoin(self.base_url, url)
            item['site'] = 'ITVIEC'
            item['created_at'] = created_at

            # self.f.write("{}: {}\n".format(title, skills))
            yield item

    def format_skills(self, skills_list):
        """
            Process skills list from scrape output
        """
        res = []
        for skill in skills_list:
            skill = skill.strip('\n')
            res.append(skill)
        return res

    def get_created_time(self, distance_time):
        """
            Get created time from distance time.
            ie: 5h -> now-5h
        """
        # Process distance time
        distance_time = distance_time.strip('\n')
        time_now = datetime.datetime.now()
        # case minute
        if distance_time[-1] == 'm':
            minute = int(distance_time[:-1])
            minute_subtracted = datetime.timedelta(minutes = minute)
            created = time_now - minute_subtracted
            return created
        # case hour
        elif distance_time[-1] == 'h':
            hour = int(distance_time[:-1])
            hour_subtracted = datetime.timedelta(hours = hour)
            created = time_now - hour_subtracted
            return created
        # case day
        elif distance_time[-1] == 'd':
            day = int(distance_time[:-1])
            day_subtracted = datetime.timedelta(days = day)
            created = time_now - day_subtracted
            return created
        return time_now


class SkillsSpiderTopDev(scrapy.Spider):
    name = "skills_topdev"

    def start_requests(self):
        urls = ['https://topdev.vn/viec-lam-it']

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)
        # self.f = open('xi_kiu', 'w')
        self.base_url = 'https://topdev.vn'
    def parse(self, response):
        # list_job = response.xpath('.//div[@class="list__job"]').get()
        # print('List Job: ', list_job)
        # with open('topdev_list_job.html', 'w') as f:
        #     f.write(list_job)
        list_job = response.xpath('.//div[@class="list__job"]')
        scroll_jobs = list_job.xpath('.//div[@id="scroll-it-jobs"]')

        job_content = scroll_jobs.xpath('.//div[@class="cont"]')
        job_bottom = scroll_jobs.xpath('.//div[@class="job-bottom mb-0"]')
        title_list = []
        for (content, bottom) in zip(job_content, job_bottom):

            title = content.xpath('.//h3/a/text()').get()
            company = content.xpath('.//div[@class="clearfix"]/p/text()').get()
            print(title, company)
            title_list.append(title)

        print(title_list)