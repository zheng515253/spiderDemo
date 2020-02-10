# -*- coding: utf-8 -*-
import redis
import scrapy
import json, time, sys, random, pyssdb, re

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectionRefusedError, DNSLookupError
from twisted.web._newclient import ResponseFailed, ResponseNeverReceived
from scrapy.utils.response import response_status_message # 获取错误代码信息

from spider.items import SpiderItem
from scrapy.utils.project import get_project_settings
#from mq.pdd_goods_mq import pdd_goods_mq

'''从队列同步产品信息'''
class PddSyncGoodsSpider(scrapy.Spider):
	name 		= 'pdd_sync_goods'
	goods_list 	= []
	ssdb 		= ''
	queue_name 	= 'pdd_sync_goods_list'
	proxy_start_time = 0
	proxy_ip_list = []
	redis_conn = ''

	current_proxy = ''
	proxy_count = 0
	endpoint = 'http://apiv4.yangkeduo.com'  # 网址
	test_pool_hash = 'sync_goods_qu_chong_hash'

	def __init__(self):
		self.ssdb_client = pyssdb.Client(get_project_settings().get('SSDB_HOST'), 8888)
		pool = redis.ConnectionPool(host=get_project_settings().get('PROXY_REDIS_HOST'), port=6379, db=10,
									password='20A3NBVJnWZtNzxumYOz', decode_responses=True)
		# 创建链接对象
		self.redis_client = redis.Redis(connection_pool=pool)

	def start_requests(self):
		search 	=	True
		while search:
			headers = self.make_headers()

			goods_ids 	=	self.ssdb_client.qpop_front(self.queue_name, 1)
			if goods_ids == True or goods_ids == None:
				time.sleep(30)
				continue

			if type(goods_ids) == bytes: ##只有1条数据时返回的是bytes格式，转换成list
				goods_ids = [goods_ids]

			for goods_id in goods_ids:
				# time.sleep(0.1)
				goods_id = goods_id.decode('utf-8')
				
				# 判断重复数据
				#if (self.ssdb_client.hget(self.test_pool_hash, goods_id)):
				#	continue
				#else:
				#	self.ssdb_client.hset(self.test_pool_hash, goods_id, 1)
				
				
				# url = 'http://mobile.yangkeduo.com/goods.html?goods_id='+str(goods_id)
				url = self.endpoint + '/api/oakstc/v14/goods/' + str(goods_id)
				# print(goods_id)
				meta = {'proxy': self.get_proxy_ip(False), 'goods_id': goods_id}
				yield scrapy.Request(url, callback=self.parse, meta=meta, headers=headers, dont_filter=True, errback=self.errback_httpbin)

		# for i in range(self.start, self.end):
		# 	if i >= self.end - 1: ##判断是否是最后一次循环
		# 		is_end = True

		# 	meta = {'is_end':is_end}

		# 	headers = self.make_headers()
		# 	url = 'http://mobile.yangkeduo.com/goods.html?goods_id='+str(i)

		# 	yield scrapy.Request(url, meta = meta, callback=self.parse, headers=headers,dont_filter=True)

	def parse(self, response):
		pass
		content = response.body.decode('utf-8')
		# print(content)
		a = json.loads(content)  # re.search('window\.rawData= (.*)\;\s*\<\/script\>', content)
		if a:
			goods = a
			goods_data = SpiderItem()
			# print(goods['goods_id'])
			goods_data['goods_id'] = goods['goods_id']
			goods_data['mall_id'] = goods['mall_id']
			goods_data['goods_type'] = goods['goods_type']
			goods_data['category1'] = str(goods['cat_id_1'])
			goods_data['category2'] = str(goods['cat_id_2'])
			goods_data['category3'] = str(goods['cat_id_3'])
			goods_data['goods_name'] = goods['goods_name']
			goods_data['market_price'] = float(goods['market_price'] / 100)  # 单位：元，下同
			goods_data['max_group_price'] = float(goods['max_on_sale_group_price'] / 100)
			goods_data['min_group_price'] = float(goods['min_on_sale_group_price'] / 100)
			goods_data['max_normal_price'] = float(goods['max_on_sale_normal_price'] / 100)
			goods_data['min_normal_price'] = float(goods['min_on_sale_normal_price'] / 100)
			goods_data['thumb_url'] = goods['thumb_url']
			goods_data['publish_date'] = goods['created_at']
			goods_data['total_sales'] = int(goods['sales'])  # 总销量
			goods_data['is_on_sale'] = goods['is_onsale']

			# ##获取核算价
			goods_data['price'] = goods_data['min_group_price']
			goods_data['total_amount'] = float(goods_data['total_sales'] * float(goods_data['price']))  # 总销售额

			yield goods_data


	'''/**获取核算价 先按照销量最高的价格 若销量为0 则为价格最低的作为核算价**/'''	
	def get_goods_price(self, goods_skus, goods_sold_num):
		#print(goods_skus[0])
		if goods_sold_num:
			goods_skus.sort(key=lambda x:-x['sold_quantity'])
		else:
			goods_skus.sort(key=lambda x:x['group_price'])

		if goods_skus:
			return goods_skus[0]['group_price']
		else:
			return 0

	'''生成headers头信息'''
	def make_headers(self):
		chrome_version   = str(random.randint(59,63))+'.0.'+str(random.randint(1000,3200))+'.94'
		headers = {
			# "Host": "apiv4.yangkeduo.com",
			# "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			# "Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
			# "Accept-Encoding":"gzip, deflate",
			# "Host":"yangkeduo.com",
			"Referer":"Andriod",
			# "Connection":"keep-alive",
			# 'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/'+chrome_version+' Safari/537.36',
			'User-Agent': 'android Mozilla/5.0 (Linux; Android 6.1; 4G Build/MRA58K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/44.0.2403.119 Mobile Safari/537.36  phh_android_version/3.39.0 phh_android_build/228842 phh_android_channel/gw_pc',
			# 'AccessToken': 'UWOSVXQYG3MD6XMABMWJUAKJR37R72BESHUVPLN5ZAU74LP2HXZQ100b64e',
			# 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
		}
		
		ip = str(random.randint(100, 200))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))
		headers['CLIENT-IP'] 	=	ip
		headers['X-FORWARDED-FOR']=	ip
		return headers
	'''代理'''
	def get_proxy_ip(self, refresh):
		if not refresh and self.proxy_count < 100:
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

		return 'http://'+ip

	def get_ssdb_proxy_ip(self):
		ips = self.redis_client.hkeys('proxy_ip_hash_fy')
		res = []
		for index in range(len(ips)):
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

	def err_after(self, meta, remove=False):
		proxy_ip = meta["proxy"]
		proxy_ip = proxy_ip.replace("http://", "").encode("utf-8")

		if remove and proxy_ip in self.proxy_ip_list:
			index = self.proxy_ip_list.index(proxy_ip)
			del self.proxy_ip_list[index]

		self.get_proxy_ip(True)

		# 推回队列
		goods_id = meta['goods_id']
		self.ssdb_client.qpush_back(self.queue_name, str(goods_id))  # 失败重新放入队列
