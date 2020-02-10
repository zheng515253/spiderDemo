# -*- coding: utf-8 -*-
import datetime
import string

import scrapy
import json, time, sys, random, re, pyssdb, os,setting, logging, redis
from spider.items import CategoryGoodsItem
# from spider.items import GoodsSalesItem
from scrapy.utils.project import get_project_settings

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectionRefusedError, DNSLookupError
from twisted.web._newclient import ResponseFailed, ResponseNeverReceived
from scrapy.utils.response import response_status_message # 获取错误代码信息

'''获取店铺内产品销量信息'''
class PddGoodsQuantityV1Spider(scrapy.Spider):
	name = 'pdd_goods_quantity_v1'
	alias_name = 'get_pdd_goods_info'
	goods_list = 'pdd_spider_goods_quantity_list'
	hash_num 		= 0
	process_nums 	= 1

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
			for goods_id in wait_check_goods:
				goods_id = goods_id.decode('utf-8').strip()

				headers = self.make_headers()
				url = self.build_url()
				form_data = {'goods_id': goods_id}
				meta = {'goods_id': goods_id}

				yield scrapy.Request(url, method='POST', body=json.dumps(form_data), meta=meta, callback=self.parse,
									 headers=headers,dont_filter=True, errback=self.errback_httpbin)

	def parse(self, response):
		goods_id = response.meta['goods_id']  ##店铺ID

		goods_data = response.body.decode('utf-8')  ##bytes转换为str

		goods_data = json.loads(goods_data)

		if 'goods_id' in goods_data["goods"].keys():
			item = CategoryGoodsItem()
			detail, skus, galleries = self.make_goods_quantity_data(goods_data, int(time.time()))
			logging.debug(json.dumps({
				'detail': detail,
				'skus': skus,
				'galleries': galleries
			}))
			item['goods_lists'] = {
				'detail': detail,
				'skus': skus,
				'galleries': galleries
			}
			yield item

	def make_goods_quantity_data(self, goods_info, spider_time):
		min_group_price = float(goods_info['price']['min_on_sale_group_price'] / 100)
		goods = goods_info['goods']
		goods_detail = {
			'goods_name': goods_info['goods']['goods_name'],
			'cat_id_1': str(goods_info['goods']['cat_id_1']),
			'cat_id_2': str(goods_info['goods']['cat_id_2']),
			'cat_id_3': str(goods_info['goods']['cat_id_3']),
			'goods_id': goods_info['goods']['goods_id'],
			'total_sales': int(goods_info['goods']['sold_quantity']),
			'mall_id': goods_info['goods']['mall_id'],
			'is_on_sale': goods_info['goods']['is_onsale'],
			'thumb_url': goods_info['goods']['thumb_url'],
			'quantity': goods_info['goods']['quantity'],
			'global_sold_quantity': goods_info['goods']['global_sold_quantity'],
			'is_pre_sale': goods_info['goods']['is_pre_sale'],
			'pre_sale_time': goods_info['goods']['pre_sale_time'],
			'has_promotion': goods_info['goods']['has_promotion'],
			'goods_property': goods_info['goods']['goods_property'] if 'goods_property' in goods_info['goods'].keys() else [],
			'service_promise': goods_info['service_promise'],
			'min_group_price': min_group_price,
			'spider_time': spider_time,
		}
		skus = []
		for sku_detail in goods_info['sku']:
			skus.append({
				'sku_id': sku_detail['sku_id'],
				'goods_id': sku_detail['goods_id'],
				'quantity': sku_detail['quantity'],
				'init_quantity': sku_detail['init_quantity'],
				'sold_quantity': sku_detail['sold_quantity'],
				'thumb_url': sku_detail['thumb_url'],
				'specs': sku_detail['specs'],
				'group_price': sku_detail['group_price'],
				'spider_time': spider_time
			})
		galleries = []
		for gallery_detail in goods['gallery']:
			galleries.append({
				'id': gallery_detail['id'],
				'goods_id': gallery_detail['goods_id'],
				'url': gallery_detail['url'],
				'width': gallery_detail['width'],
				'height': gallery_detail['height'],
				'priority': gallery_detail['priority'],
				'type': gallery_detail['type'],
				'spider_time': spider_time
			})
		return goods_detail, skus, galleries

	def build_url(self):
		url = 'https://api.pinduoduo.com/api/oak/integration/render'
		return url

	'''生成headers头信息'''
	def make_headers(self):
		chrome_version = str(random.randint(59, 63)) + '.0.' + str(random.randint(1000, 3200)) + '.94'
		headers = {
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

		# 失败重新退回队列 // 考虑失败干掉不可用的商品
		goods_id = meta['goods_id']
		url = self.build_url(str(goods_id))
		form_data = {'goods_id': goods_id}
		headers = self.make_headers()
		yield scrapy.Request(url, method='POST', body=json.dumps(form_data), meta=meta, callback=self.parse,
							 headers=headers, dont_filter=True, errback=self.errback_httpbin)
