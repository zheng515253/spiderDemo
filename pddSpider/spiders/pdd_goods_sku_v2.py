# -*- coding: utf-8 -*-
import scrapy
import json, time, sys, random, re, pyssdb, setting
from urllib import parse, request
from spider.items import GoodsSalesItem

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectionRefusedError, DNSLookupError
from twisted.web._newclient import ResponseFailed, ResponseNeverReceived
from scrapy.utils.response import response_status_message # 获取错误代码信息

'''获取产品子料号信息'''
class PddGoodsSkuV2Spider(scrapy.Spider):
	name 	  = 'pdd_goods_sku_v2'
	list_name = 'pdd_sync_sku_goods_id_list'

	hash_num 		= 0
	process_nums 	= 1

	proxy_start_time= 0
	proxy_ip_list   = []

	current_proxy = ''
	proxy_count = 0
	limit			= 100
	custom_settings = {
		# 'LOG_FILE':'',
		# 'LOG_LEVEL':'DEBUG',
		# 'LOG_ENABLED':True,
		'RETRY_ENABLED': False,  # 是否重试
		'DOWNLOAD_TIMEOUT': 2,  # 超时时间
		# 'RETRY_TIMES': 20,
		'CONCURRENT_REQUESTS': 1,
		'DOWNLOAD_DELAY': 0,
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
			if  type(goods_list) == bool: ##没有数据返回
				is_end 	=	True
				continue
			elif type(goods_list) == bytes:
				goods_list = [goods_list]

			for goods in goods_list:
				detail = json.loads(goods.decode('utf-8'))
				goods_id = detail['goods_id']
				time  = detail['time']
				headers = self.make_headers(goods_id)
				meta = {'proxy':self.get_proxy_ip(False),'time':time, 'goods_id':goods_id}
				# print(meta)
				yield scrapy.Request(self.get_goods_url(goods_id), meta=meta, callback=self.parse, headers=headers, dont_filter=True, errback=self.errback_httpbin)

	def get_goods_url(self, goods_id):
		return 'http://mobile.yangkeduo.com/goods.html?goods_id='+str(goods_id)

	def parse(self, response):
		pass
		meta = response.meta
		html = response.body.decode('utf-8') ##bytes转换为str
		# print(1, html)
		regex_content = re.search('window\.rawData= (.*)\;\s*\<\/script\>', html)
		if not regex_content:
			self.err_after(meta)
			return None
		rawData = json.loads( regex_content.group(1) )
		if 'initDataObj' not in rawData.keys():
			self.err_after(meta)
			return None
		initDataObj = rawData['initDataObj'] ##str转为字典
		# print(2,initDataObj)
		if 'needLogin' in initDataObj.keys():
			self.err_after(meta, True)
			return None
		if 'goods' not in initDataObj.keys():
			self.err_after(meta)
			return None
		goods_data = initDataObj['goods'] ##str转为字典
		# print(goods_data)
		if 'skus' not in goods_data.keys():
			self.err_after(meta)
			return None
		time	  = response.meta['time'] ##产品集合
		goods_id  = response.meta['goods_id'] ##店铺ID
		skus = []
		for sku in goods_data['skus']:
			spec = ''
			for specX in sku['specs']:
				spec += str(specX['spec_value'])
			skuDetail = {
				'sku_id': sku['skuID'],
				'thumb_url': sku['thumbUrl'],
				'quantity': sku['quantity'],
				'is_onsale': sku['isOnSale'],
				'spec': spec,
				'normal_price': sku['normalPrice'],
				'group_price': sku['groupPrice'],
				'specs': sku['specs'],
				'weight': 0,
			}
			skus.append(skuDetail)
		# print(skus)
		item = GoodsSalesItem()
		goods_data['sku'] = skus
		goods_data['time'] = time
		item['goods_list'] = goods_data
		item['mall_id']    = goods_id
		yield item

	def get_proxy_ip(self, refresh):
		if not refresh and self.proxy_count < 50 and self.current_proxy != '' :
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
			ip = ip.decode('utf-8')
			self.current_proxy = ip

		# print(ip, self.proxy_count)

		return 'http://'+ip

	def get_ssdb_proxy_ip(self):
		ips = self.ssdb.hkeys('proxy_ip_hash', '', '', 1000)
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
		# print(failure)
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

		meta['proxy'] = self.get_proxy_ip(True)
		goods_id = meta['goods_id']
		headers = self.make_headers(goods_id)
		yield scrapy.Request(self.get_goods_url(goods_id), meta=meta, callback=self.parse, headers=headers,dont_filter=True,errback=self.errback_httpbin)

	'''生成headers头信息'''
	def make_headers(self, goods_id):
		headers = {
			"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			"Accept-Encoding": "gzip, deflate",
			"Accept-Language": "zh-CN,zh;q=0.9",
			"Connection": "keep-alive",
			"Referer":'http://mobile.yangkeduo.com/goods.html?goods_id='+str(goods_id),
			"Host": "mobile.yangkeduo.com",
			"Upgrade-Insecure-Requests": "1",
			"Cookie": '_nano_fp=XpdYX0gJlpgon0dxl9_Vfj_vkXR1TNT9PuWUpMqq; api_uid=' + str( self.ssdb.get('pdd_api_uid') ),
			'User-Agent':setting.setting().get_web_user_agent(),
		}
		return headers