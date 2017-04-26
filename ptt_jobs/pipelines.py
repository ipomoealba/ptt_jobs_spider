# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo

from ptt_jobs import settings
from scrapy.conf import settings
from scrapy import log

class PttJobsPipeline(object):

    def __init__(self):
        connection = pymongo.MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        self.db = connection[settings['MONGODB_DB']]
        self.collection = self.db['tmp']

    def process_item(self, item, spider):
        self.collection = self.db[str(item['date']).split(' ')[0]]
        valid = True
        for data in item:
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))
        if valid:
            if self.collection.find({'date': item['date']}).count() > 0:
                log.msg("[!] the Data has in the collection %s" %
                        item['title'], level=log.DEBUG, spider=spider)
            else:

                self.collection.insert(dict(item))
                log.msg("[!] NewData added to MongoDB database!",
                        level=log.DEBUG, spider=spider)
        return item
