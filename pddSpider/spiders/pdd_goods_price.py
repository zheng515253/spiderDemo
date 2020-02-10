# -*- coding: utf-8 -*-
import scrapy
import json, time, sys, random, re, pyssdb, pddSign, setting
from spider.items import GoodsSalesItem

goods_list = []
'''获取店铺内产品销量信息'''
class PddGoodsPriceSpider(scrapy.Spider):
	name = 'pdd_goods_price'
	mall_id_hash 	= 'pdd_mall_id_hash'
	hash_num 		= 0
	process_nums 	= 1

	limit			= 1000
	log_user_id     = 0
	redis_conn      = ''
	custom_settings = {
		'DOWNLOADER_MIDDLEWARES':{'spider.middlewares.ProxyMiddleware': 100},
		# 'LOG_FILE':'',
		# 'LOG_LEVEL':'DEBUG',
		# 'LOG_ENABLED':True,
		'DOWNLOAD_TIMEOUT':5,
		'RETRY_TIMES':10,
		#'DOWNLOAD_DELAY':0.015,
		}

	def __init__(self, hash_num = 0, process_nums = 1):
		self.hash_num = int(hash_num) ##当前脚本号
		self.process_nums   = int(process_nums) ##脚本总数

		self.pageSize = 1000 ##每次抓取的产品数 最大只返回500
		self.ssdb 	= pyssdb.Client('172.16.0.5', 8888)
		self.pdd_class = pddSign.pddSign()
		

	def start_requests(self):
		mall_nums 		= 	self.limit * int(self.process_nums) ##一次查询的数量
		is_end 			=	False
		start_mall_id 	=	'' ##起始查询的店铺key
		while not is_end:
			mall_ids 	=	self.ssdb.hkeys(self.mall_id_hash, start_mall_id, '', mall_nums)
			if  not mall_ids: ##没有数据返回
				is_end 	=	True
				continue
			self.log_user_id = self.get_uid();

			for mall_id in mall_ids:
				mall_id = int( mall_id.decode('utf-8') )
				start_mall_id = mall_id
				# if mall_id < 2400000: ##ID小于300W则跳过（300W之前的用老脚本抓取）
				# 	continue

				if mall_id % self.process_nums != self.hash_num:
					continue
					
				goods_list=[]
				page = 1

				headers = self.make_headers()
				##url = 'http://apiv4.yangkeduo.com/api/turing/mall/query_cat_goods?category_id=0&type=0&sort_type=_sales&mall_id='+str(mall_id)+'&page_no='+str(page)+'&page_size=500'
				#url = 'http://apiv3.yangkeduo.com/api/turing/mall/query_cat_goods?category_id=0&type=0&mall_id='+str(mall_id)+'&page_no='+str(page)+'&page_size=500&sort_type=_sales&anticontent='+self.anti_content+'&pdduid='+str(self.log_user_id)
				url = self.build_url(mall_id, page, 500)

				meta = {'page':page, 'mall_id':mall_id, 'goods_list':goods_list}
				yield scrapy.Request(url, meta=meta, callback=self.parse, headers=headers,dont_filter=True,errback=self.errback_httpbin)
			
	def parse(self, response):
		pass
		goods_list=response.meta['goods_list'] ##产品集合
		mall_id  = response.meta['mall_id'] ##店铺ID
		page 	 = response.meta['page'] ##每返回一次页面数据 记录页数

		mall_goods = response.body.decode('utf-8') ##bytes转换为str
		mall_goods = json.loads(mall_goods)
		mall_goods = mall_goods['goods']
		if 'goods_list' not in mall_goods.keys():
			return None

		goods_len  = len(mall_goods['goods_list'])

		if goods_len > 0:
			goods_list = goods_list + mall_goods['goods_list'] ##合并产品列表

		if goods_len > self.pageSize - 100:
			page += 1
			##继续采集下一页面
			##url = 'http://apiv4.yangkeduo.com/api/turing/mall/query_cat_goods?category_id=0&type=0&sort_type=_sales&mall_id='+str(mall_id)+'&page_no='+str(page)+'&page_size=500'
			#url  ='http://apiv3.yangkeduo.com/api/turing/mall/query_cat_goods?category_id=0&type=0&mall_id='+str(mall_id)+'&page_no='+str(page)+'&page_size=500&sort_type=_sales&anti_content='+self.anti_content+'&pdduid='+str(self.log_user_id) 
			url = self.build_url(mall_id, page, 500)
			meta = {'page':page, 'mall_id':mall_id, 'goods_list':goods_list}
			headers = self.make_headers()
			yield scrapy.Request(url, meta=meta, callback=self.parse, headers=headers,dont_filter=True,errback=self.errback_httpbin)
		else:
			if goods_list:
				item = GoodsSalesItem()
				item['goods_list'] = goods_list
				item['mall_id']    = mall_id
				yield item

	'''生成headers头信息'''
	def make_headers(self):
		chrome_version   = str(random.randint(59,63))+'.0.'+str(random.randint(1000,3200))+'.94'
		headers = {
			# "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			# "Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
			# "Accept-Encoding":"gzip, deflate",
			"Referer":"Android",
			# "Connection":"keep-alive",
			'User-Agent':setting.setting().get_default_user_agent(),
			#'User-Agent':'Mozilla/5.0 (iPhone; CPU iPhone OS 11_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15F79 ===  iOS/11.4 Model/iPhone9,1 BundleID/com.xunmeng.pinduoduo AppVersion/4.15.0 AppBuild/1807251632 cURL/7.47.0',
			#'AccessToken':'TCEQ2DM4MRHLIDZVEB6MFP3JOENREVWSO2IH77PI3MUV4Q6GGF3A1017c59',
			"AccessToken": "",
			"Cookie": "api_uid="
		}
		
		# ip = str(random.randint(100, 200))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))
		# headers['CLIENT-IP'] 	=	ip
		# headers['X-FORWARDED-FOR']=	ip
		return headers

	def get_uid(self):
		#uids = ['2222222222', '3333333333','4444444444', '555555555', '6666666666','7777777777','8888888888', '9999999999']
		#return str( random.randint(1,10) ) + str( random.choice(uids) )
		uid = '13652208' + str(random.randint(20,99))
		return uid

	def build_url(self, mall_id,page=1, page_size=500):
		# pdd_sign = self.get_pdd_sign()
		#url = 'http://apiv3.yangkeduo.com/api/turing/mall/query_cat_goods?category_id=0&type=0&mall_id='+str(mall_id)+'&page_no='+str(page)+'&page_size='+str(page_size)+'&sort_type=_sales&anticontent='+pdd_sign+'&pdduid='+str(self.get_uid())
		url = 'http://apiv4.yangkeduo.com/api/turing/mall/query_mall_home_info?mall_id='+str(mall_id)+'&pdduid=&type=1'
		return url

	def get_pdd_sign(self):
		
		return self.pdd_class.messagePack()
		# a = self.redis_conn.get('pddcmb.main:crawler_message_pack:'+str(random.randint(0, 999)) )
		# a = a.split(':')
		# a = a[2].strip('";')
		# return a
		
	def errback_httpbin(self, failure):
		request = failure.request
		#headers = self.make_headers()
		#meta = {'proxy':self.proxy}
		# meta = request.meta
		# yield scrapy.Request(request.url, meta=meta, callback=self.parse, headers=request.headers,dont_filter=True,errback=self.errback_httpbin)