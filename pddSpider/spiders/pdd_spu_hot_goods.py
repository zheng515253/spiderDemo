# -*- coding: utf-8 -*-
from urllib import request

import scrapy
import json, time, sys, random, re, setting, pyssdb, redis, logging
from scrapy.utils.project import get_project_settings
from spider.items import GoodsSalesItem

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectionRefusedError, DNSLookupError
from twisted.web._newclient import ResponseFailed, ResponseNeverReceived
from scrapy.utils.response import response_status_message # 获取错误代码信息

'''获取产品子料号信息'''
class PddHotGoodsAndSPU(scrapy.Spider):
	name 	  = 'pdd_hot_goods_and_spu'
	list_name = 'pdd_hot_goods_check_list'

	proxy_start_time= 0
	proxy_ip_list   = []
	current_proxy = ''
	proxy_count = 0

	limit			= 1
	custom_settings = {
		# 'LOG_FILE': '',
		# 'LOG_LEVEL': 'DEBUG',
		# 'LOG_ENABLED': True,
		'RETRY_ENABLED': False,
		'DOWNLOAD_TIMEOUT': 5,
		# 'RETRY_TIMES': 20,
		'RETRY_HTTP_CODECS':[403,429],
		'DOWNLOAD_DELAY': 0.1,
		'CONCURRENT_REQUESTS': 60
	}

	#handle_httpstatus_list = [400]

	def __init__(self, hash_num = 0, process_nums = 1):
		self.ssdb_client = pyssdb.Client(get_project_settings().get('SSDB_HOST'), 8888)
		self.hash_num = int(hash_num) ##当前脚本号
		self.process_nums   = int(process_nums) ##脚本总数
		# #创建连接池
		pool = redis.ConnectionPool(host=get_project_settings().get('PROXY_REDIS_HOST'),port=6379,db=10,password='20A3NBVJnWZtNzxumYOz',decode_responses=True)
		#创建链接对象
		self.redis_client=redis.Redis(connection_pool=pool)

	def start_requests(self):
		goods_nums 		= 	self.limit * int(self.process_nums) ##一次查询的数量
		is_end 			=	False
		while not is_end:
			goods_list 	=	self.ssdb_client.qpop_front(self.list_name, goods_nums)
			if  not goods_list: ##没有数据返回
				is_end 	=	True
				continue

			if type(goods_list) == bytes:
				goods_list = [goods_list]
			
			for goods_id in goods_list:
				goods_id = json.loads(goods_id.decode('utf-8'))
				url = self.build_url(goods_id)
				headers = self.make_headers()
				meta = {'goods_id': goods_id, 'proxy': self.get_proxy_ip(False)}
				yield scrapy.Request(url, meta=meta, callback=self.parse, headers=headers, dont_filter=True, errback=self.errback_httpbin)

	def parse(self, response):
		goods_data = response.body.decode('utf-8') ##bytes转换为str
		goods_data = json.loads(goods_data)
		if 'goods_id' in goods_data.keys():
			item = GoodsSalesItem()
			item['goods_list'] = goods_data
			yield item

	def build_url(self, goods_id):
		goods_base_url = 'http://apiv4.yangkeduo.com/api/oakstc/'
		version_url = 'v14' if random.randint(0, 1) > 0 else 'v15'
		return goods_base_url + version_url + '/goods/' + str(goods_id)
	
	'''生成headers头信息'''
	def make_headers(self):
		# chrome_version   = str(random.randint(59,63))+'.0.'+str(random.randint(1000,3200))+'.94'
		headers = {
			# "Host":"yangkeduo.com",
			# "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			# "Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
			# "Accept-Encoding":"gzip, deflate",
			# "Host":"yangkeduo.com",
			# "Referer":"http://mobile.yangkeduo.com/",
			# "Connection":"keep-alive",
			# 'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/'+chrome_version+' Safari/537.36',
			# "Host":"mobile.yangkeduo.com",
			# "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			# "Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
			# "Accept-Encoding":"gzip, deflate",
			# "Host":"mobile.yangkeduo.com",
			"Referer":"Android",
			# "Connection":"keep-alive",
			"Cookie":'api_uid=',
			"AccessToken":"",
			'User-Agent':setting.setting().get_android_user_agent(),
		}

		# ip = str(random.randint(100, 200))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))
		# headers['CLIENT-IP'] 	=	ip
		# headers['X-FORWARDED-FOR']=	ip
		return headers

	def get_proxy_ip(self, refresh):
		if self.current_proxy != '' and not refresh and self.proxy_count < 50:
			ip = self.current_proxy
			self.proxy_count += 1
		else:
			self.proxy_count = 0
			now_time = int(time.time())
			if now_time - self.proxy_start_time >= 2:
				self.proxy_ip_list = self.get_ssdb_proxy_ip()
				self.proxy_start_time = now_time

			if len(self.proxy_ip_list) <= 0:
				self.proxy_ip_list = self.get_ssdb_proxy_ip()

			if len(self.proxy_ip_list) <= 0:
				return ''
			# print('proxy_count', len(self.proxy_ip_list))

			ip = random.choice(self.proxy_ip_list)
			self.current_proxy = ip

		logging.debug(json.dumps({
			'ip' : ip,
			'count': self.proxy_count
		}))

		return 'http://'+ip
	
	def get_ssdb_proxy_ip(self):
		ips = self.redis_client.hkeys('proxy_ip_hash_fy')
		res = []
		for index in range(len(ips)):
			if index % self.process_nums != self.hash_num:
				continue
			res.append(ips[index])
		if res:
			return res
		else:
			return []

	def errback_httpbin(self, failure):
		request = failure.request
		if failure.check(HttpError):
			response = failure.value.response
			# print( 'errback <%s> %s , response status:%s' % (request.url, failure.value, response_status_message(response.status)) )
			self.err_after(request.meta)
		elif failure.check(ResponseFailed):
			# print('errback <%s> ResponseFailed' % request.url)
			self.err_after(request.meta, True)
 
		elif failure.check(ConnectionRefusedError):
			# print('errback <%s> ConnectionRefusedError' % request.url)
			self.err_after(request.meta, True)
 
		elif failure.check(ResponseNeverReceived):
			# print('errback <%s> ResponseNeverReceived' % request.url)
			self.err_after(request.meta)
 
		elif failure.check(TCPTimedOutError, TimeoutError):
			# print('errback <%s> TimeoutError' % request.url)
			self.err_after(request.meta, True)
		else:
			# print('errback <%s> OtherError' % request.url)
			self.err_after(request.meta)

	def err_after(self, meta, remove = False):
		proxy_ip = meta["proxy"]
		proxy_ip = proxy_ip.replace("http://", "").encode("utf-8")

		if remove and proxy_ip in self.proxy_ip_list:
			index = self.proxy_ip_list.index(proxy_ip)
			del self.proxy_ip_list[index]

		self.get_proxy_ip(True)

		# 失败重新退回队列 // 考虑失败干掉不可用的商品
		goods_id = meta['goods_id']
		self.ssdb_client.qpush_back(self.list_name, str(goods_id))
