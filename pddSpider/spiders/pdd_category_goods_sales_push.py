# -*- coding: utf-8 -*-
# 获取首页分类下的产品列表
import scrapy
import json, time, sys, random, pyssdb
from spider.items import CategoryGoodsItem

class PddCategoryGoodsSalesPushSpider(scrapy.Spider):
	name  = 'pdd_category_goods_sales_push'

	custom_settings = {
		# 'LOG_FILE':'',
		# 'LOG_LEVEL':'DEBUG',
		# 'LOG_ENABLED':True,
		'DOWNLOAD_DELAY':0.01,
		}
	def __init__(self):
		self.ssdb = pyssdb.Client('172.16.0.5', 8888);

	def start_requests(self):
		yield scrapy.Request('http://www.baidu.com', callback=self.parse)

	def parse(self, response):
		hash_name  = 'pdd_category_goods_sales_hash' 
		is_end 	   = False
		start_key  = ''
		item = CategoryGoodsItem()

		while not is_end:
			goods_list = self.ssdb.hscan(hash_name, '', '', 10)
			if not goods_list:
				is_end = True
				continue

			for i in goods_list:
				i = json.loads( i.decode('utf-8') )

				if type(i) != dict:
					self.ssdb.hdel(hash_name, i)
					continue

				item['goods_lists'] = i
				yield item
