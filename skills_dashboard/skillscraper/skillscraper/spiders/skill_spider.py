import scrapy
from scrapy import item
from skillscraper.items import SkillscraperItem, Skill
from urllib.parse import urljoin
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
