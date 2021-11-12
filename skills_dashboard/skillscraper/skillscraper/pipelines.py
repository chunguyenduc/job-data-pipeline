# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo
from pymongo import errors
from . import settings
from scrapy.exceptions import DropItem
import logging


class SkillscraperPipeline:
    def process_item(self, item, spider):
        return item


class MongoDBPipeline(object):
    def __init__(self):
        connection = pymongo.MongoClient(
            settings.MONGODB_SERVER,
            settings.MONGODB_PORT
        )
        self.db = connection[settings.MONGODB_DB]
        self.collection = self.db[settings.MONGODB_COLLECTION]

    def process_item(self, item, spider):
        valid = True
        for data in item:
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))
        if valid:
            old_item = {
                'title': item['title'],
                'city': item['city'],
                'company': item['company'],
                'url': item['url'],
                'site': item['site']
            }
            new_item = {
                # "$set": old_item,
                "$setOnInsert": dict(item)
            }

            self.collection.update_one(old_item, new_item, upsert=True)
            logging.info('Job added: {}\n'.format(item))

        return item
