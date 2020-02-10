"""
分类树
"""
import scrapy
import json
import pyssdb
import logging
import redis
import time
import os
from scrapy.utils.project import get_project_settings

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectionRefusedError, DNSLookupError
from twisted.web._newclient import ResponseFailed, ResponseNeverReceived
from scrapy.utils.response import response_status_message # 获取错误代码信息

class Proxy16yCheck(scrapy.Spider):
	name = 'proxy_16y_check'
	hash_num = 0
	process_nums = 1
   
	url = 'http://httpbin.org/ip'
	proxy_key = 'proxy_ip_hash_16y'
	custom_settings = {
		# 'LOG_FILE':'',
		# 'LOG_LEVEL':'DEBUG',
		# 'LOG_ENABLED':True,
		'RETRY_ENABLED': False,
		'DOWNLOAD_TIMEOUT': 5,
		'CONCURRENT_REQUESTS': 100
	}

	def __init__(self, hash_num = 0, process_nums = 1):
		self.hash_num = int(hash_num) ##当前脚本号
		self.process_nums   = int(process_nums) ##脚本总数
		#创建连接池
		pool = redis.ConnectionPool(host=get_project_settings().get('PROXY_REDIS_HOST'),port=6379,db=10,password=get_project_settings().get('PROXY_REDIS_AUTH'),decode_responses=True)
		#创建链接对象
		self.redis_client=redis.Redis(connection_pool=pool)

	def start_requests(self):
		# 首页抓取分类，一个链接
		url = self.url
		ips = self.redis_client.hkeys(self.proxy_key)
		for index in range(len(ips)):
			meta = {'proxy': 'http://' + ips[index]}
			yield scrapy.Request(url, meta=meta, callback=self.parse, headers={}, dont_filter=True, errback=self.errback_httpbin)

	def parse(self, response):
		meta = response.meta
		proxy_ip = meta["proxy"]
		proxy_ip = proxy_ip.replace("http://", "").encode("utf-8")
		content = response.body.decode('utf-8')
		logs = json.dumps({
			'log_type': 'success',
			'proxy': str(proxy_ip),
			'result': str(content),
			'remove': False
		})
		self.save_log(logs)

	def errback_httpbin(self, failure):
		request = failure.request
		if failure.check(HttpError):
			response = failure.value.response
			logging.debug( 'errback <%s> %s , response status:%s' % (request.url, failure.value, response_status_message(response.status)) )
			self.err_after(request.meta)
		elif failure.check(ResponseFailed):
			logging.debug('errback <%s> ResponseFailed' % request.url)
			self.err_after(request.meta)
 
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
		logs = json.dumps({
			'log_type': 'err_after',
			'proxy': str(proxy_ip),
			'result': 'fail', 
			'remove': remove
		})
		self.save_log(logs)
		if remove:
			self.redis_client.hdel(self.proxy_key, str(proxy_ip))

	def save_log(self, logs):
		logging.debug(logs)
		date = time.strftime('%Y-%m-%d')
		file_path = '/data/spider/logs/proxy_16y_check'
		if not os.path.exists(file_path):
			os.makedirs(file_path)

		data = '[' + str(time.strftime('%Y-%m-%d %H:%M:%S')) + '] ' + logs

		file_name = file_path+'/'+date+'.log'
		with open(file_name, "a+") as f:
			f.write(data+"\r\n")