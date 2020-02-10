# -*- coding: utf-8 -*-
import scrapy
import json, time, sys, random, re, setting, pyssdb, redis, logging
from scrapy.utils.project import get_project_settings
from spider.items import GoodsSalesItem

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectionRefusedError, DNSLookupError
from twisted.web._newclient import ResponseFailed, ResponseNeverReceived
from scrapy.utils.response import response_status_message # 获取错误代码信息

'''获取产品子料号信息'''
class PddGoodsSkuV5Spider(scrapy.Spider):
	name 	  = 'pdd_goods_sku_v5'
	list_name = 'pdd_sync_sku_goods_id_list'

	gateway_url = 'http://gateway.91cyt.com/api/curl'

	limit			= 100
	custom_settings = {
		# 'LOG_FILE': '',
		# 'LOG_LEVEL': 'DEBUG',
		# 'LOG_ENABLED': True,
		'RETRY_ENABLED': False,
		'DOWNLOAD_TIMEOUT': 5,
		# 'RETRY_TIMES': 20,
		'RETRY_HTTP_CODECS':[403,422,429],
		'DOWNLOAD_DELAY': 0.1,
		'CONCURRENT_REQUESTS': 60
	}

	#handle_httpstatus_list = [400]

	def __init__(self, hash_num = 0, process_nums = 1):
		self.ssdb_client = pyssdb.Client(get_project_settings().get('SSDB_HOST'), 8888)
		self.hash_num = int(hash_num) ##当前脚本号
		self.process_nums   = int(process_nums) ##脚本总数

	def start_requests(self):
		goods_nums 		= 	self.limit ##一次查询的数量
		is_end 			=	False

		while not is_end:
			goods_list 	=	self.ssdb_client.qpop_front(self.list_name, goods_nums)

			if type(goods_list) == bool:
				is_end = True
				continue
			elif type(goods_list)==bytes:
				goods_list = [goods_list]

			for goods in goods_list:

				goods = json.loads(goods.decode('utf-8'))
				goods_id = goods['goods_id']
				time  = goods['time']

				headers = self.make_headers()
				meta = {'time':time, 'goods_id':goods_id}

				form_data = {
					'name': 'getGoodsInfo',
					'method': 'GET',
					'domain': 'http://apiv4.yangkeduo.com',
					'uri': self.build_uri(goods_id),
					'headers': json.dumps(headers),
				}

				logging.debug(json.dumps(form_data))

				yield scrapy.FormRequest(url=self.gateway_url, formdata=form_data, meta=meta, headers={},
									 callback=self.parse, dont_filter=True, errback=self.errback_httpbin)

	def parse(self, response):
		time	  = response.meta['time'] ##产品集合
		goods_id  = response.meta['goods_id'] ##店铺ID

		goods_data = response.body.decode('utf-8') ##bytes转换为str

		logging.debug(goods_data)

		goods_data = json.loads(goods_data)

		if 'goods_id' in goods_data.keys():
			item = GoodsSalesItem()
			goods_data['time'] = time
			item['goods_list'] = goods_data
			item['mall_id']    = goods_id
			yield item

	def build_uri(self, goods_id):
		goods_base_url = '/api/oakstc/'
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
		# 失败重新退回队列 // 考虑失败干掉不可用的商品
		goods_id = meta['goods_id']
		url = self.build_url(goods_id)
		yield scrapy.Request(request.url, meta=meta, callback=self.parse, headers=request.headers,dont_filter=True,errback=self.errback_httpbin)
