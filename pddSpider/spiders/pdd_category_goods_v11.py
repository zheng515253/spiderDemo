# -*- coding: utf-8 -*-
# 获取首页分类下的产品列表
import scrapy
import json, time, sys, random, pddSign, urllib,setting, pyssdb,string,os,uuid, redis, logging
from spider.items import CategoryGoodsItem
from scrapy.utils.project import get_project_settings

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectionRefusedError, DNSLookupError
from twisted.web._newclient import ResponseFailed, ResponseNeverReceived
from scrapy.utils.response import response_status_message # 获取错误代码信息

class PddCategoryGoodsV11Spider(scrapy.Spider):
	name  = 'pdd_category_goods_v11'

	alias_name = 'category_goods'

	queue_name  = 'queue_pdd_category_goods_list'
	pagesise = 50
	realPagesise = 20
	
	proxy_start_time= 0
	proxy_ip_list   = []
	current_proxy = ''
	proxy_count = 0

	custom_settings = {
		'DOWNLOADER_MIDDLEWARES': {'spider.middlewares.ProxyMiddleware': 543},
		# 'LOG_FILE': '',
		# 'LOG_LEVEL': 'DEBUG',
		# 'LOG_ENABLED': True,
		'RETRY_ENABLED': False,
		'DOWNLOAD_TIMEOUT': 5,
		# 'RETRY_TIMES': 20,
		'RETRY_HTTP_CODECS':[403,429],
		'DOWNLOAD_DELAY': 0.01,
		'CONCURRENT_REQUESTS': 10
	}

	def __init__(self, hash_num=0, process_nums=1, save_logs_flag = False):
		self.ssdb_client = pyssdb.Client(get_project_settings().get('SSDB_HOST'), 8888)
		self.hash_num = int(hash_num)  ##当前脚本号
		self.process_nums = int(process_nums)  ##脚本总数
		#创建连接池
		pool = redis.ConnectionPool(host=get_project_settings().get('PROXY_REDIS_HOST'),port=6379,db=10,password='20A3NBVJnWZtNzxumYOz',decode_responses=True)
		#创建链接对象
		self.redis_client=redis.Redis(connection_pool=pool)
		self.save_logs_flag = save_logs_flag
		self.anti_token_key = get_project_settings().get('PDD_ANTI_TOKEN_KEY')

	def start_requests(self):
		is_end	=	False
		while not is_end:
			category = self.ssdb_client.qpop_front(self.queue_name, 1)

			if not category: ##没有数据返回
				is_end = True
				continue

			detail = json.loads( category.decode('utf-8') )
			if type(detail) != int: ##当值是subject_id 时
				# print (detail);
				cat_id      = detail['subject_id']
				opt_type    = detail['opt_type']
				offset 		= 0 if 'offset' not in detail.keys() else detail['offset']
				flip 		= '' if 'flip' not in detail.keys() else detail['flip']

				url = self.build_url(opt_type, cat_id, offset, flip)
				meta = {'cat_id':cat_id,'opt_type':opt_type,'offset':offset,'category':detail, 'flip':flip}
				logging.debug(url)
				headers = self.make_headers()
				yield scrapy.Request(url, meta=meta, callback=self.parse, headers=headers, dont_filter=True, errback=self.errback_httpbin)

	def parse(self, response):
		logging.debug(response.meta)
		meta = response.meta
		offset = response.meta['offset']
		cat_id = response.meta['cat_id']
		opt_type = response.meta['opt_type']
		receive_info = response.body
		data = json.loads(receive_info.decode('utf-8'))
		self.save_goods_log(cat_id, json.dumps({
			'cat_id': cat_id,
			'offset': offset,
			'receive_info': data
		}))
		goods_lists = []
		if 'goods_list' not in data.keys():
			self.err_after(meta, True)
			return False
		flip = data['flip']
		if len(data['goods_list']) > 0:
			i = 0
			for goods_data in data['goods_list']:
				i += 1
				rank = offset + i
				goods_data['rank'] = rank
				goods_data['subject_id'] = cat_id
				goods_data['type'] = 2
				goods_lists.append(goods_data)

			offset += i
			item = CategoryGoodsItem()
			item['goods_lists'] = goods_lists
			yield item
			if i >= self.realPagesise and offset < 1000 - self.realPagesise:
				url = self.build_url(opt_type, cat_id, offset, flip)
				meta['offset'] = offset
				meta['flip'] = flip
				headers = self.make_headers()
				yield scrapy.Request(url, meta=meta, callback=self.parse, headers=headers, dont_filter=True, errback=self.errback_httpbin)

	'''生成headers头信息'''
	def make_headers(self):
		# chrome_version   = str(random.randint(59,63))+'.0.'+str(random.randint(1000,3200))+'.94'
		headers = {
			"Referer":"Android",
			"ETag":'LRruqcTa',
			"X-PDD-QUERIES": 'width=720&height=1356&net=1&brand=4G&model=4G&osv=6.0&appv=4.49.2&pl=2',
			"Content-Type":"application/json;charset=UTF-8",
			'User-Agent':'android Mozilla/5.0 (Linux; Android 6.0; 4G Build/MRA58K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/55.0.2883.91 Mobile Safari/537.36  phh_android_version/4.51.0 phh_android_build/deb7f0aa693a9c2817697e7d4ce2f8132839d0eb phh_android_channel/qihu360',
			'p-appname': 'pinduoduo',
			'PDD-CONFIG': '00102',
			'anti-token': self.redis_client.rpop(self.anti_token_key),
		}
		return headers

	def build_url(self, opt_type, cat_id, offset, flip):
		url = 'http://api.pinduoduo.com/v4/operation/'+str(cat_id)+'/groups?opt_type='+str(opt_type)+'&offset='+str(offset)+'&size='+str(self.pagesise)+'&sort_type=DEFAULT&&pdduid='
		if flip.strip()!='':
			url+='&flip='+flip
		return url

	def get_proxy_ip(self, refresh):
		if self.current_proxy != '' and not refresh and self.proxy_count < 100:
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
			# ip = ip.decode('utf-8')
			self.current_proxy = ip

		logging.debug(json.dumps({
			'ip': ip,
			'count': self.proxy_count
		}))

		return 'http://'+ip
		# return
		# if old_proxy:
		# 	time.sleep(random.randint(1,10))
		# 	if old_proxy != self.proxy:
		# 		return

		# url = 'http://proxy.publictank.com/api/proxy/provider'
		# data = {'provider':4}
		# req = request.Request(url=url, data=parse.urlencode(data).encode('utf-8'))
		# res = request.urlopen(req)
		# res = res.read()
		
		# content = json.loads(res.decode('utf-8'))
		# if content:
		# 	self.proxy = 'http://'+content['ip']+':'+content['port']
	
	def get_ssdb_proxy_ip(self):
		# ips = self.ssdb_client.hkeys('proxy_ip_hash', '', '', 1000)
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
		logging.debug(request.meta['proxy'])
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
		# proxy_ip = meta["proxy"]
		# proxy_ip = proxy_ip.replace("http://", "").encode("utf-8")

		# if remove and proxy_ip in self.proxy_ip_list:
		# 	index = self.proxy_ip_list.index(proxy_ip)
		# 	del self.proxy_ip_list[index]

		# self.get_proxy_ip(True)

		category = meta['category']
		category['offset'] = meta['offset']
		category['flip'] = meta['flip']
		self.ssdb_client.qpush_back(self.queue_name, json.dumps(category))  # 失败重新放入队列

	def save_goods_log(self, cat_id, data):
		logging.debug(data)
		if not self.save_logs_flag:
			return None
		date = time.strftime('%Y-%m-%d')

		file_path = '/data/spider/logs/category_log/'+time.strftime('%Y-%m-%d')
		if not os.path.exists(file_path):
			os.makedirs(file_path)

		file_name = file_path+'/'+str(cat_id)+'.log'
		with open(file_name, "a+") as f:
			f.write(data+"\r\n")
