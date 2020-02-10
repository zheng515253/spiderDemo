# -*- coding: utf-8 -*-
import scrapy
import json, time, sys, random, re, pyssdb
from spider.items import GoodsSalesItem

'''获取产品子料号信息'''
class PddGoodsSkuSpider(scrapy.Spider):
	name 	  = 'pdd_goods_sku'
	list_name = 'pdd_sync_sku_goods_id_list'

	limit			= 1
	custom_settings = {
		 'DOWNLOADER_MIDDLEWARES':{'spider.middlewares.BuildHeaderMiddleware': 100},
		 'DOWNLOADER_MIDDLEWARES':{'spider.middlewares.ProxyMiddleware': 101},
		 'DOWNLOAD_TIMEOUT':5,
		 'RETRY_TIMES':10,
		 # 'LOG_FILE':'',
		 # 'LOG_LEVEL':'DEBUG',
		 # 'LOG_ENABLED':True,
		}

	#handle_httpstatus_list = [400]

	def __init__(self, hash_num = 0, process_nums = 1):
		self.hash_num = int(hash_num) ##当前脚本号
		self.process_nums   = int(process_nums) ##脚本总数

		self.ssdb 	= pyssdb.Client('172.16.0.5', 8888)

	def start_requests(self):
		goods_nums 		= 	self.limit * int(self.process_nums) ##一次查询的数量
		is_end 			=	False

		while not is_end:
			goods_list 	=	self.ssdb.qpop_front(self.list_name, goods_nums)
			if  not goods_list: ##没有数据返回
				is_end 	=	True
				continue

			if type(goods_list) == bytes:
				goods_list = [goods_list]
			
			for goods in goods_list:

				goods = json.loads(goods.decode('utf-8'))
				goods_id = goods['goods_id']
				time  = goods['time']

				# if (goods_id % self.process_nums) != self.hash_num:
				# 	continue

				##url = 'http://apiv4.yangkeduo.com/api/turing/mall/query_cat_goods?category_id=0&type=0&sort_type=_sales&mall_id='+str(mall_id)+'&page_no='+str(page)+'&page_size=500'
				#url = 'http://apiv3.yangkeduo.com/api/turing/mall/query_cat_goods?category_id=0&type=0&mall_id='+str(mall_id)+'&page_no='+str(page)+'&page_size=500&sort_type=_sales&anticontent='+self.anti_content+'&pdduid='+str(self.log_user_id)
				url = self.build_url(goods_id)
				headers = self.make_headers()
				meta = {'time':time, 'goods_id':goods_id}
				yield scrapy.Request(url, meta=meta, callback=self.parse, headers=headers, dont_filter=True, errback=self.errback_httpbin)
			
	def parse(self, response):
		pass
		time	  = response.meta['time'] ##产品集合
		goods_id  = response.meta['goods_id'] ##店铺ID

		goods_data = response.body.decode('utf-8') ##bytes转换为str
		goods_data = json.loads(goods_data)
		
		if 'goods_id' in goods_data.keys():
			item = GoodsSalesItem()
			goods_data['time'] = time
			item['goods_list'] = goods_data
			item['mall_id']    = goods_id
			yield item


	def build_url(self, goods_id):
		#url = 'http://apiv3.yangkeduo.com/api/turing/mall/query_cat_goods?category_id=0&type=0&mall_id='+str(mall_id)+'&page_no='+str(page)+'&page_size='+str(page_size)+'&sort_type=_sales&anticontent='+pdd_sign+'&pdduid='+str(self.get_uid())
		#url = 'http://apiv4.yangkeduo.com/v4/goods/'+str(goods_id)
		url = 'http://apiv4.yangkeduo.com/api/oakstc/v14/goods/'+str(goods_id)
		return url

	def errback_httpbin(self, failure):
		request = failure.request
		response = failure.value.response
		if response.status == 400:
			return
		#headers = self.make_headers()
		#meta = {'proxy':self.proxy}
		meta = request.meta
		yield scrapy.Request(request.url, meta=meta, callback=self.parse, headers=request.headers,dont_filter=True,errback=self.errback_httpbin)

	def make_headers(self):
		chrome_version = str(random.randint(59, 63)) + '.0.' + str(random.randint(1000, 3200)) + '.94'
		headers = {
			# "Host":"yangkeduo.com",
			# "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			# "Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
			# "Accept-Encoding":"gzip, deflate",
			# "Host":"yangkeduo.com",
			# "Referer":"http://mobile.yangkeduo.com/",
			# "Connection":"keep-alive",
			# 'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/'+chrome_version+' Safari/537.36',
			"Host": "mobile.yangkeduo.com",
			"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
			"Accept-Encoding": "gzip, deflate",
			"Host": "mobile.yangkeduo.com",
			"Referer": "Android",
			"Connection": "keep-alive",
			'User-Agent': 'android Mozilla/5.0 (Linux; Android 6.1; 4G Build/MRA58K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/44.0.2403.119 Mobile Safari/537.36  phh_android_version/3.39.0 phh_android_build/228842 phh_android_channel/gw_pc',
		}

		ip = str(random.randint(100, 200)) + '.' + str(random.randint(1, 255)) + '.' + str(
			random.randint(1, 255)) + '.' + str(random.randint(1, 255))
		headers['CLIENT-IP'] = ip
		headers['X-FORWARDED-FOR'] = ip
		return headers