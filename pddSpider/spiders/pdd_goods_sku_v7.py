# -*- coding: utf-8 -*-
import datetime
import string

import scrapy
import json, time, sys, random, re, pyssdb, os,setting, logging, redis
# from spider.items import CategoryGoodsItem
from spider.items import GoodsSalesItem
from scrapy.utils.project import get_project_settings

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectionRefusedError, DNSLookupError
from twisted.web._newclient import ResponseFailed, ResponseNeverReceived
from scrapy.utils.response import response_status_message # 获取错误代码信息

'''获取店铺内产品销量信息'''
class PddGoodsSkuV6Spider(scrapy.Spider):
	name = 'pdd_goods_sku_v7'
	alias_name = 'get_pdd_goods_info'
	goods_list = 'pdd_sync_sku_goods_id_list'
	hash_num 		= 0
	process_nums 	= 1

	success_count = 0
	error_count = 0
	proxy_start_time= 0
	proxy_ip_list   = []
	current_proxy = ''
	proxy_count = 0



	custom_settings = {
		# 'DOWNLOADER_MIDDLEWARES':{'spider.middlewares.SpiderDownloaderMiddleware':543},
		'DOWNLOADER_MIDDLEWARES': {'spider.middlewares.ProxyMiddleware': 543},
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

	def __init__(self, hash_num = 0, process_nums = 1):
		self.ssdb_client = pyssdb.Client(get_project_settings().get('SSDB_HOST'), 8888)
		#创建连接池
		pool = redis.ConnectionPool(host=get_project_settings().get('PROXY_REDIS_HOST'),port=6379,db=10,password='20A3NBVJnWZtNzxumYOz',decode_responses=True)
		#创建链接对象
		self.redis_client=redis.Redis(connection_pool=pool)
		self.hash_num = int(hash_num) ##当前脚本号
		self.process_nums   = int(process_nums) ##脚本总数

	def start_requests(self):
		is_end = False
		end_flag = False
		while not is_end or not end_flag:

			wait_check_goods = self.ssdb_client.qpop_front(self.goods_list,60)

			if type(wait_check_goods) == bool:
				if not end_flag:
					end_flag = True
					time.sleep(5)
				else:
					is_end = True
				continue
			elif type(wait_check_goods)==bytes:
				wait_check_goods = [wait_check_goods]

			is_end = False
			end_flag = False
			for goods in wait_check_goods:
				goods = json.loads(goods.decode('utf-8'))

				goods_id = goods['goods_id']
				time_ = goods['time']

				headers = self.make_headers()
				url = self.build_url()
				form_data = {'goods_id': goods_id}
				meta = {'goods_id': goods_id, 'time': time_}

				yield scrapy.Request(url, method='POST', body=json.dumps(form_data), meta=meta, callback=self.parse,
									 headers=headers,dont_filter=True, errback=self.errback_httpbin)

	def parse(self, response):

		time = response.meta['time']  ##产品集合
		goods_id = response.meta['goods_id']  ##店铺ID

		goods_data = response.body.decode('utf-8')  ##bytes转换为str

		# logging.debug(goods_data)

		goods_data = json.loads(goods_data)

		if 'goods_id' in goods_data["goods"].keys():
			item = GoodsSalesItem()
			goods_data['time'] = time
			item['goods_list'] = goods_data
			item['mall_id'] = goods_id
			yield item

	def build_url(self):
		url = 'https://api.pinduoduo.com/api/oak/integration/render'
		return url

	'''生成headers头信息'''
	def make_headers(self):
		chrome_version = str(random.randint(59, 63)) + '.0.' + str(random.randint(1000, 3200)) + '.94'
		headers = {
			# "Host": "apiv4.yangkeduo.com",
			# "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			# "Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
			# "Accept-Encoding":"gzip, deflate",
			# "Host":"yangkeduo.com",
			# "Connection":"keep-alive",
			# 'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko)
			#  Chrome/'+chrome_version+' Safari/537.36',
			'AccessToken': '',
			'Content-Type': 'application/json',
			'Referer': 'Andriod',
			"ETag": self.get_ETag(),
			'X-PDD-QUERIES': 'width=720&height=1356&net=1&brand=4G&model=4G&osv=6.0&appv=4.49.2&pl=2',

		}

		ip = str(random.randint(100, 200)) + '.' + str(random.randint(1, 255)) + '.' + str(
			random.randint(1, 255)) + '.' + str(random.randint(1, 255))
		headers['CLIENT-IP'] = ip
		headers['X-FORWARDED-FOR'] = ip
		return headers

	def save_goods_log(self, goods_id, goods_info):
		date = time.strftime('%Y-%m-%d')

		file_path = '/data/spider/log/goods_sales_check_log'
		if not os.path.exists(file_path):
			os.makedirs(file_path)

		file_name = file_path+'/'+date+'.log'
		with open(file_name, "a+") as f:
			f.write(goods_info+"\r\n")

	def get_ETag(self):
		return ''.join(random.sample(string.ascii_letters + string.digits, 8))

	def get_proxy_ip(self, refresh):
		if self.current_proxy != '' and not refresh and self.proxy_count < 30:
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
		ips = self.redis_client.hkeys('proxy_ip_hash_fy_v2')
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
			self.error_count += 1
			response = failure.value.response
			# print( 'errback <%s> %s , response status:%s' % (request.url, failure.value, response_status_message(response.status)) )
			self.err_after(request.meta)
		elif failure.check(ResponseFailed):
			self.error_count += 1
			# print('errback <%s> ResponseFailed' % request.url)
			self.err_after(request.meta, True)

		elif failure.check(ConnectionRefusedError):
			self.error_count += 1
			# print('errback <%s> ConnectionRefusedError' % request.url)
			self.err_after(request.meta, True)

		elif failure.check(ResponseNeverReceived):
			self.error_count += 1
			# print('errback <%s> ResponseNeverReceived' % request.url)
			self.err_after(request.meta)

		elif failure.check(TCPTimedOutError, TimeoutError):
			self.error_count += 1
			# print('errback <%s> TimeoutError' % request.url)
			self.err_after(request.meta, True)
		else:
			self.error_count += 1
			# print('errback <%s> OtherError' % request.url)
			self.err_after(request.meta)

	def err_after(self, meta, remove = False):
		proxy_ip = meta["proxy"]
		proxy_ip = proxy_ip.replace("http://", "").encode("utf-8")

		# if remove and proxy_ip in self.proxy_ip_list:
		# 	index = self.proxy_ip_list.index(proxy_ip)
		# 	del self.proxy_ip_list[index]

		# self.get_proxy_ip(True)

		# 失败重新退回队列 // 考虑失败干掉不可用的商品
		goods_id = meta['goods_id']
		# meta = {'proxy': self.get_proxy_ip(False), 'goods_id': goods_id, 'time': meta['time']}
		url = self.build_url(str(goods_id))
		form_data = {'goods_id': goods_id}
		headers = self.make_headers()
		yield scrapy.Request(url, method='POST', body=json.dumps(form_data), meta=meta, callback=self.parse,
							 headers=headers, dont_filter=True, errback=self.errback_httpbin)

	def save_success_log(self, success_info):
		date = time.strftime('%Y-%m-%d')

		file_path = '/data/spider/log/success_log'
		if not os.path.exists(file_path):
			os.makedirs(file_path)

		file_name = file_path + '/' + date + '.log'
		with open(file_name, "a+") as f:
			f.write(success_info + "\r\n")
