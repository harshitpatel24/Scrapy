# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class AzcrawlItem(scrapy.Item):
	# define the fields for your item here like:
	# name = scrapy.Field()
	source_id= scrapy.Field()
	product_id = scrapy.Field()
	product_name = scrapy.Field()
	review_id = scrapy.Field()
	review_title = scrapy.Field()
	review_text = scrapy.Field()
	review_date = scrapy.Field()
	stars = scrapy.Field()
	author_name = scrapy.Field()
	comments_count = scrapy.Field()