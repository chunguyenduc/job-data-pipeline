import scrapy
from skillscraper.items import SkillscraperItem
from urllib.parse import urljoin
import datetime


def format_skills(skills_list):
    """
        Process skills list from scrape output
    """
    res = []
    for skill in skills_list:
        skill = skill.strip()
        res.append(skill)
    return res


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
            title = body.xpath('.//h3/a/text()').get()
            skills = bottom.xpath('.//a/span/text()').getall()
            city = body.xpath('.//div[@class="city"]/div/text()').get()
            url = body.xpath('.//h3/a/@href').get()
            skills = format_skills(skills)
            company = logo.xpath('.//img/@alt').get()[:-11]
            distance_time = bottom.xpath(
                './/div[@class="distance-time-job-posted"]/span/text()').get()
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

    # def format_skills(self, skills_list):
    #     """
    #         Process skills list from scrape output
    #     """
    #     res = []
    #     for skill in skills_list:
    #         skill = skill.strip('\n')
    #         res.append(skill)
    #     return res

    def get_created_time(self, distance_time):
        """
            Get created time from distance time.
            ie: 5h -> now-5h
        """
        # Process distance time
        distance_time = distance_time.strip('\n')
        time_now = datetime.datetime.now()
        print('Time now: ', time_now)

        # case minute
        if distance_time[-1] == 'm':
            minute = int(distance_time[:-1])
            minute_subtracted = datetime.timedelta(minutes=minute)
            created = time_now - minute_subtracted
            return created
        # case hour
        elif distance_time[-1] == 'h':
            hour = int(distance_time[:-1])
            hour_subtracted = datetime.timedelta(hours=hour)
            created = time_now - hour_subtracted
            return created
        # case day
        elif distance_time[-1] == 'd':
            day = int(distance_time[:-1])
            day_subtracted = datetime.timedelta(days=day)
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
        self.city_in_english = {
            "Hồ Chí Minh": "Ho Chi Minh",
            "Hà Nội": "Ha Noi",
            "Đà Nẵng": "Da Nang",
        }

    def parse(self, response):
        # list_job = response.xpath('.//div[@class="list__job"]').get()
        # print('List Job: ', list_job)
        # with open('topdev_list_job.html', 'w') as f:
        #     f.write(list_job)
        list_job = response.xpath('.//div[@class="list__job"]')
        scroll_jobs = list_job.xpath('.//div[@id="scroll-it-jobs"]')

        job_content = scroll_jobs.xpath('.//div[@class="cont"]')
        job_bottom = scroll_jobs.xpath('.//div[@class="job-bottom mb-0"]')
        job_ago = scroll_jobs.xpath('.//p[@class="job-ago"]')
        for (content, bottom, ago) in zip(job_content, job_bottom, job_ago):

            title = content.xpath('.//h3/a/text()').getall()[-1].strip()
            company = content.xpath('.//div[@class="clearfix"]/p/text()')\
                .getall()[0].strip()
            url = content.xpath('.//h3/a/@href').get()
            # url = urljoin(self.base_url, url)
            city = content.xpath('.//div[@class="clearfix"]/p/text()')\
                .getall()[2].split(', ')[-1].strip()
            city_eng = ''
            if city in self.city_in_english:
                city_eng = self.city_in_english[city]
            else:
                city_eng = city
            skills = bottom.xpath('.//a/span/text()').getall()
            skills = format_skills(skills)
            what = ago.xpath('text()').get().strip()
            created_at = self.get_created_time(what)
            # print('Log: ', title, company, url, city)
            print('{} | {} | {} | {} | {} | {} '.format(
                city_eng, company, url, title, skills, created_at))
            item = SkillscraperItem()
            item['title'] = title
            item['skills'] = skills
            item['city'] = city_eng
            item['company'] = company
            item['url'] = urljoin(self.base_url, url)
            item['site'] = 'TOPDEV'
            item['created_at'] = created_at

            # print(city_eng)
            # title_list.append(title)
            yield item

        # print(title_list)
    def get_created_time(self, distance_time):
        """
            Get created time from distance time.
            ie: 5 giờ trước -> now-5h
                5 ngày trước -> now-5d
        """
        # Process distance time
        distance_time = distance_time.strip('\n')
        time_now = datetime.datetime.now()
        print('Time now: ', time_now)
        # print(distance_time[:-10])
        # case hour
        if distance_time[-10:] == ' giờ trước':
            hour = int(distance_time[:-10])
            hour_subtracted = datetime.timedelta(hours=hour)
            # print(hour)
            created = time_now - hour_subtracted
            return created
        # case day
        elif distance_time[-11:] == ' ngày trước':
            day = int(distance_time[:-11])
            day_subtracted = datetime.timedelta(days=day)
            created = time_now - day_subtracted
            return created
        return time_now
