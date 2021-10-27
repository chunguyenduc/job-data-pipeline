# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field
import scrapy

class Skill(scrapy.Item):
    name = Field()

class SkillscraperItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = Field()
    url = Field()
    skills = Field()
    city = Field()


