# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class PttJobsItem(scrapy.Item):
    classify = scrapy.Field()
    title = scrapy.Field()
    date = scrapy.Field()
    author = scrapy.Field()
    author_ip = scrapy.Field()
    url = scrapy.Field()
    content = scrapy.Field()
    sheeee = scrapy.Field()
    push = scrapy.Field()
    arrow = scrapy.Field()
    comments = scrapy.Field()
