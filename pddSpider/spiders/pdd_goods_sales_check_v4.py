# -*- coding: utf-8 -*-
import scrapy
import json, time, sys, random, re, pyssdb, os,setting, logging, redis
from spider.items import CategoryGoodsItem
from scrapy.utils.project import get_project_settings

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectionRefusedError, DNSLookupError
from twisted.web._newclient import ResponseFailed, ResponseNeverReceived
from scrapy.utils.response import response_status_message # 获取错误代码信息

'''获取店铺内产品销量信息'''
class PddGoodsSalesCheckV4Spider(scrapy.Spider):
	name = 'pdd_goods_sales_check_v4'

	goods_list = 'pdd_goods_sales_check_list'
	hash_num 		= 0
	process_nums 	= 1

	proxy_start_time= 0
	proxy_ip_list   = []
	current_proxy = ''
	proxy_count = 0

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
			wait_check_goods = self.ssdb_client.qpop_front(self.goods_list, 60)

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
				goods_id = goods_id.decode('utf-8').strip();
				
				goods_url = self.build_url(str(goods_id))
				headers = self.make_headers()
				meta = {'proxy': self.get_proxy_ip(False), 'goods_id': goods_id}
				# print(goods_id)
				yield scrapy.Request(goods_url, meta=meta, callback=self.parse, headers=headers, dont_filter=True, errback=self.errback_httpbin)
			time.sleep(1) # 休眠1s

	def parse(self, response):
		pass
		receive_info = response.body.decode('utf-8') ##bytes转换为str
		goods_info = json.loads(receive_info)
		goods_id   = goods_info['goods_id']
		#self.save_goods_log(goods_id, receive_info)
		item = CategoryGoodsItem()
		item['goods_lists'] = {
								'goods_id':goods_id,
								'goods_sales':goods_info['sales'],
								'mall_id':goods_info['mall_id'],
								'goods_price':goods_info['min_on_sale_group_price']
							   }
		yield item

	def build_url(self, goods_id):
		goods_base_url = 'http://apiv4.yangkeduo.com/api/oakstc/'
		version_url = 'v14' if random.randint(0, 1) > 0 else 'v15'
		return goods_base_url + version_url + '/goods/' + goods_id

	'''生成headers头信息'''
	def make_headers(self):
		chrome_version   = str(random.randint(59,63))+'.0.'+str(random.randint(1000,3200))+'.94'
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

	def save_goods_log(self, goods_id, goods_info):
		date = time.strftime('%Y-%m-%d')

		file_path = '/data/spider/log/goods_sales_check_log'
		if not os.path.exists(file_path):
			os.makedirs(file_path)

		file_name = file_path+'/'+date+'.log'
		with open(file_name, "a+") as f:
			f.write(goods_info+"\r\n")


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
			self.current_proxy = ip

		logging.debug(json.dumps({
			'ip' : ip,
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
		self.ssdb_client.qpush(self.goods_list, str(goods_id))
