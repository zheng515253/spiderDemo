# -*- coding: utf-8 -*-
import logging

import scrapy
import json, time, sys, random, re, pyssdb, pddSign, os, datetime, setting, redis, logging
from spider.items import GoodsSalesItem
from scrapy.utils.project import get_project_settings

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectionRefusedError, DNSLookupError
from twisted.web._newclient import ResponseFailed, ResponseNeverReceived
from scrapy.utils.response import response_status_message # 获取错误代码信息

goods_list = []
'''获取店铺内产品销量信息'''
class PddGoodsSalesV11Spider(scrapy.Spider):
	name = 'pdd_goods_sales_v11'
	# mall_id_hash 	= 'pdd_mall_id_hash'
	# fail_hash  		= 'pdd_mall_id_fail_hash'

	alias_name = 'pdd_query_mall_home_info'

	'''待抓取产品销量的店铺列表'''
	mall_id_hash 	= 'pdd_crawl_mall_id_hash'
	mall_id_list 	= 'pdd_crawl_mall_id_list:'
	list_name 	= ''
	hash_num 		= 0
	process_nums 	= 1
	limit			= 1000
	log_user_id     = 0
	redis_conn      = ''

	proxy_start_time= 0
	proxy_ip_list   = []

	custom_settings = {
		'DOWNLOADER_MIDDLEWARES': {'spider.middlewares.ProxyMiddleware': 543},
		# 'LOG_FILE':'',
		# 'LOG_LEVEL':'DEBUG',
		# 'LOG_ENABLED':True,
		'DOWNLOAD_DELAY':0,
		'DOWNLOAD_TIMEOUT': 5,  # 超时时间
		'RETRY_ENABLED': False,  # 是否重试,
		'RETRY_HTTP_CODECS':[403,429],
		'CONCURRENT_REQUESTS': 60
	}

	def __init__(self, hash_num = 0, process_nums = 1):
		self.today = datetime.date.today()
		self.hash_num = int(hash_num) ##当前脚本号
		self.process_nums   = int(process_nums) ##脚本总数
		self.list_name = self.mall_id_list + str(self.today)
		self.pageSize = 1000 ##每次抓取的产品数 最大只返回500
		self.ssdb_client = pyssdb.Client(get_project_settings().get('SSDB_HOST'), 8888)
		#创建连接池
		pool = redis.ConnectionPool(host=get_project_settings().get('PROXY_REDIS_HOST'),port=6379,db=10,password='20A3NBVJnWZtNzxumYOz',decode_responses=True)
		#创建链接对象
		self.redis_client=redis.Redis(connection_pool=pool)

	def start_requests(self):
		mall_nums 		= 	self.limit * int(self.process_nums) ##一次查询的数量
		is_end 			=	False
		while not is_end:
			mall_ids 	=	self.ssdb_client.qpop_front(self.list_name, 100)
			if  not mall_ids: ##没有数据返回
				is_end 	=	True
				continue
			self.log_user_id = self.get_uid();

			for mall_id in mall_ids:
				if type(mall_id) != int:
					mall_id = int( mall_id.decode('utf-8') )

				goods_list=[]
				page = 1

				headers = self.make_headers()
				url = self.build_url(mall_id, page, 500)

				meta = {'page':page, 'mall_id':mall_id, 'goods_list':goods_list}
				yield scrapy.Request(url, meta=meta, callback=self.parse, headers=headers ,dont_filter=True,errback=self.errback_httpbin)

	def parse(self, response):
		pass
		goods_list=response.meta['goods_list'] ##产品集合
		mall_id  = response.meta['mall_id'] ##店铺ID
		page 	 = response.meta['page'] ##每返回一次页面数据 记录页数
		proxy 	 = response.meta['proxy'] ##使用原始代理

		mall_goods = response.body.decode('utf-8') ##bytes转换为str
		# self.save_mall_log(mall_id, mall_goods)

		mall_goods = json.loads(mall_goods)
		mall_goods = mall_goods['goods']

		if 'goods_list' not in mall_goods.keys():
			#self.ssdb_client.hset(self.fail_hash, mall_id, mall_id)
			return None

		goods_len  = len(mall_goods['goods_list'])

		if goods_len > 0:
			goods_list = goods_list + mall_goods['goods_list'] ##合并产品列表
		else:
			return None

		if goods_len > self.pageSize - 100:
			page += 1
			##继续采集下一页面
			url = self.build_url(mall_id, page, 500)
			meta = {'page':page, 'mall_id':mall_id, 'goods_list':goods_list, 'proxy':proxy}
			headers = self.make_headers()
			yield scrapy.Request(url, meta=meta, callback=self.parse, headers=headers, dont_filter=True,errback=self.errback_httpbin)
		else:
			if goods_list:
				item = GoodsSalesItem()
				item['goods_list'] = goods_list
				item['mall_id']    = mall_id
				#print(item)
				yield item

	'''生成headers头信息'''
	def make_headers(self):
		chrome_version   = str(random.randint(59,63))+'.0.'+str(random.randint(1000,3200))+'.94'
		# headers = {
		# 	"Host":"mobile.yangkeduo.com",
		# 	"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
		# 	"Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
		# 	"Accept-Encoding":"gzip, deflate",
		# 	"Host":"yangkeduo.com",
		# 	"Referer":"http://yangkeduo.com/goods.html?goods_id=442573047&from_subject_id=935&is_spike=0&refer_page_name=subject&refer_page_id=subject_1515726808272_1M143fWqjQ&refer_page_sn=10026",
		# 	"Connection":"keep-alive",
		# 	'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/'+chrome_version+' Safari/537.36',
		# }
		# 
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
		url = 'http://api.yangkeduo.com/api/turing/mall/query_mall_home_info?mall_id='+str(mall_id)+'&type=1&pdduid='
		return url

	def get_proxy_ip(self, refresh):
		now_time = int(time.time())
		if now_time - self.proxy_start_time >= 2:
			self.proxy_ip_list = self.get_ssdb_proxy_ip()
			self.proxy_start_time = now_time

		if len(self.proxy_ip_list) <= 0:
			self.proxy_ip_list = self.get_ssdb_proxy_ip()

		if len(self.proxy_ip_list) <= 0:
			return ''

		ip = random.choice(self.proxy_ip_list)

		return 'http://'+ip

	def get_ssdb_proxy_ip(self):
		ips = self.redis_client.hkeys('proxy_ip_hash_fy')
		res = []
		for index in range(len(ips)):
			if index % self.process_nums != self.hash_num:
				continue
			res.append(ips[index])
		# print(res)
		if res:
			return res
		else:
			return []

	def errback_httpbin(self, failure):
		request = failure.request
		if failure.check(HttpError):
			response = failure.value.response
			logging.debug( 'errback <%s> %s , response status:%s' % (request.url, failure.value, response_status_message(response.status)) )
			self.err_after(request.meta)
		elif failure.check(ResponseFailed):
			logging.debug('errback <%s> ResponseFailed' % request.url)
			self.err_after(request.meta, True)
 
		elif failure.check(ConnectionRefusedError):
			logging.debug('errback <%s> ConnectionRefusedError' % request.url)
			self.err_after(request.meta, True)
 
		elif failure.check(ResponseNeverReceived):
			logging.debug('errback <%s> ResponseNeverReceived' % request.url)
			self.err_after(request.meta)
 
		elif failure.check(TCPTimedOutError, TimeoutError):
			logging.debug('errback <%s> TimeoutError' % request.url)
			self.err_after(request.meta, True)
		else:
			logging.debug('errback <%s> OtherError' % request.url)
			self.err_after(request.meta)

	def err_after(self, meta, remove = False):
		proxy_ip = meta["proxy"]
		proxy_ip = proxy_ip.replace("http://", "").encode("utf-8")

		# if remove and proxy_ip in self.proxy_ip_list:
		# 	index = self.proxy_ip_list.index(proxy_ip)
		# 	del self.proxy_ip_list[index]

		# self.get_proxy_ip(True)

		# 推回队列
		mall_id = meta['mall_id']
		self.ssdb_client.qpush_back(self.list_name, str(mall_id))  # 失败关键词重新放入队列

	'''保存店铺产品原始数据'''
	def save_mall_log(self, mall_id, mall_info):
		date = time.strftime('%Y-%m-%d')

		file_path = '/data/spider/logs/mall_log'
		if not os.path.exists(file_path):
			os.makedirs(file_path)

		file_name = file_path+'/'+date+'.log'
		with open(file_name, "a+") as f:
			f.write(mall_info+"\r\n")
