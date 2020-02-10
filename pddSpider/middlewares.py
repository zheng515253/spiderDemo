# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
import os

import scrapy
from scrapy import signals
import random, base64, sys, json, pyssdb, time, redis, datetime
PY3 = sys.version_info[0] >= 3


def base64ify(bytes_or_str):
	if PY3 and isinstance(bytes_or_str, str):
		input_bytes = bytes_or_str.encode('utf8')
	else:
		input_bytes = bytes_or_str

	output_bytes = base64.urlsafe_b64encode(input_bytes)
	if PY3:
		return output_bytes.decode('ascii')
	else:
		return output_bytes

class SpiderSpiderMiddleware(object):
	# Not all methods need to be defined. If a method is not defined,
	# scrapy acts as if the spider middleware does not modify the
	# passed objects.

	@classmethod
	def from_crawler(cls, crawler):
		# This method is used by Scrapy to create your spiders.
		s = cls()
		crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
		return s

	def process_spider_input(self, response, spider):
		# Called for each response that goes through the spider
		# middleware and into the spider.

		# Should return None or raise an exception.
		return None

	def process_spider_output(self, response, result, spider):
		# Called with the results returned from the Spider, after
		# it has processed the response.

		# Must return an iterable of Request, dict or Item objects.
		for i in result:
			yield i

	def process_spider_exception(self, response, exception, spider):
		# Called when a spider or process_spider_input() method
		# (from other spider middleware) raises an exception.

		# Should return either None or an iterable of Response, dict
		# or Item objects.
		pass

	def process_start_requests(self, start_requests, spider):
		# Called with the start requests of the spider, and works
		# similarly to the process_spider_output() method, except
		# that it doesn’t have a response associated.

		# Must return only requests (not items).
		for r in start_requests:
			yield r

	def spider_opened(self, spider):
		spider.logger.info('Spider opened: %s' % spider.name)


class SpiderDownloaderMiddleware(object):
	# Not all methods need to be defined. If a method is not defined,
	# scrapy acts as if the downloader middleware does not modify the
	# passed objects.

	basic_token_redis_key = 'pdd_spider_token_basic'
	day_token_redis_key = ''
	currentToken = ''
	currentCount = ''

	@classmethod
	def from_crawler(cls, crawler):
		# This method is used by Scrapy to create your spiders.
		s = cls(
			redis_host=crawler.settings.get('PROXY_REDIS_HOST'),
			redis_auth=crawler.settings.get('PROXY_REDIS_AUTH'),
		)
		crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
		return s

	def __init__(self, redis_host, redis_auth):
		self.today = datetime.date.today()
		pool = redis.ConnectionPool(host=redis_host,port=6379,db=10,password=redis_auth,decode_responses=True)
		#创建链接对象
		self.redis_client=redis.Redis(connection_pool=pool)

	def process_request(self, request, spider):
		# Called for each request that goes through the downloader
		# middleware.
		try:
			spider_type = spider.alias_name
		except Exception as e:
			spider_type = spider.name

		token = self.randomToken(spider_type)

		request.headers['AccessToken'] = token

		# Must either:
		# - return None: continue processing this request
		# - or return a Response object
		# - or return a Request object
		# - or raise IgnoreRequest: process_exception() methods of
		#   installed downloader middleware will be called
		return None

	def randomToken(self, spider_type):
		day_token_redis_flag    = self.basic_token_redis_key + ':' + str(spider_type) + ':flag:' + str(self.today)
		day_token_redis_collect = self.basic_token_redis_key + ':' + str(spider_type) + ':collect:' + str(self.today) 
		day_token_redis_detail  = self.basic_token_redis_key + ':' + str(spider_type) + ':hash:' + str(self.today)
		exist_key = self.redis_client.exists(day_token_redis_flag)
		if not exist_key:
			self.redis_client.delete(day_token_redis_collect)
			array = self.redis_client.smembers(self.basic_token_redis_key)
			for item in array:
				self.redis_client.sadd(day_token_redis_collect, item)
			self.redis_client.set(day_token_redis_flag, 1)
		token = ''
		while True:
			queue_len = self.redis_client.scard(day_token_redis_collect)
			if queue_len > 0:
				token = self.redis_client.srandmember(day_token_redis_collect)
				if self.remove_token(day_token_redis_detail, token):
					break
				self.redis_client.srem(day_token_redis_collect, token)
			else:
				break
		return token

	def remove_token(self, day_token_redis_detail, token):
		token_detail = self.redis_client.hget(day_token_redis_detail, token)
		if not token_detail:
			token_detail = self.redis_client.hset(day_token_redis_detail, token, json.dumps({'count':1}))
			return True
		else:
			token_detail = json.loads(token_detail)
			if token_detail['count'] > 500:
				return False
			else:
				token_detail['count'] += 1
				self.redis_client.hset(day_token_redis_detail, token, json.dumps(token_detail))
				self.currentCount=token_detail['count']
				return True

	def oneToken(self, spider_type):
		day_token_redis_flag   = self.basic_token_redis_key + ':' + str(spider_type) + ':flag:' + str(self.today)
		day_token_redis_queue  = self.basic_token_redis_key + ':' + str(spider_type) + ':queue:' + str(self.today) 
		day_token_redis_detail = self.basic_token_redis_key + ':' + str(spider_type) + ':hash:' + str(self.today)
		if self.currentToken and self.auth_token(day_token_redis_detail, self.currentToken):
			pass
		else:
			exist_key = self.redis_client.exists(day_token_redis_flag)
			if not exist_key:
				self.redis_client.delete(day_token_redis_queue)
				array = self.redis_client.smembers(self.basic_token_redis_key)
				for item in array:
					self.redis_client.lpush(day_token_redis_queue, item)
				self.redis_client.set(day_token_redis_flag, 1)
			queue_len = self.redis_client.llen(day_token_redis_queue)
			if queue_len > 0:
				self.currentToken = self.redis_client.rpop(day_token_redis_queue)
				self.auth_token(self.currentToken)
			else:
				self.currentToken = ''
		spider.logger.info(json.dumps({
			'where': 'end',
			'token': self.currentToken,
			'count': self.currentCount
		}))
		return self.currentToken

	def auth_token(self, day_token_redis_detail, token):
		token_detail = self.redis_client.hget(day_token_redis_detail, token)
		if not token_detail:
			token_detail = self.redis_client.hset(day_token_redis_detail, token, json.dumps({'count':1}))
			self.currentCount=1
			return True
		else:
			token_detail = json.loads(token_detail)
			if token_detail['count'] > 500:
				return False
			else:
				token_detail['count'] += 1
				self.redis_client.hset(day_token_redis_detail, token, json.dumps(token_detail))
				self.currentCount=token_detail['count']
				return True

	def process_response(self, request, response, spider):
		# Called with the response returned from the downloader.

		# Must either;
		# - return a Response object
		# - return a Request object
		# - or raise IgnoreRequest
		return response

	def process_exception(self, request, exception, spider):
		# Called when a download handler or a process_request()
		# (from other downloader middleware) raises an exception.

		# Must either:
		# - return None: continue processing this exception
		# - return a Response object: stops process_exception() chain
		# - return a Request object: stops process_exception() chain
		pass

	def spider_opened(self, spider):
		spider.logger.info('Spider opened: %s' % spider.name)

'''生成header请求中间件'''
class BuildHeaderMiddleware(object):
	def process_request(self, request, spider):
		headers = self.make_headers()
		for key in headers.keys():
			request.headers[key] = headers[key]

	def make_headers(self):
		user_agent = self.get_user_agent()
		headers = {
			"Host":"yangkeduo.com",
			"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			"Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
			"Accept-Encoding":"gzip, deflate",
			"Host":"yangkeduo.com",
			"Referer":"http://yangkeduo.com/goods.html?goods_id=442573047&from_subject_id=935&is_spike=0&refer_page_name=subject&refer_page_id=subject_1515726808272_1M143fWqjQ&refer_page_sn=10026",
			"Connection":"keep-alive",
			'User-Agent':user_agent,
		}
		ip = str(random.randint(100, 200))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))
		headers['CLIENT-IP'] 	=	ip
		headers['X-FORWARDED-FOR']=	ip
		return headers

	def get_user_agent(self):
		chrome_version   = str(random.randint(59,63))+'.0.'+str(random.randint(1000,3200))+'.94'
		user_agent = 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/'+chrome_version+' Safari/537.36'
		return user_agent

'''代理IP中间件'''
class ProxyMiddleware(object):
	ssdb_host = ''
	ssdb_client = ''
	proxy_start_time= 0
	proxy_ip_list   = []

	proxy_redis_detail = 'proxy_redis_detail'
	proxy_ip_hash      = 'proxy_ip_hash_fy'

	@classmethod
	def from_crawler(cls, crawler):
		s = cls(
			redis_host=crawler.settings.get('PROXY_REDIS_HOST'),
			redis_auth=crawler.settings.get('PROXY_REDIS_AUTH'),
		)
		crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
		return s

	def __init__(self, redis_host, redis_auth):
		pool = redis.ConnectionPool(host=redis_host, port=6379, db=10, password=redis_auth, decode_responses=True)
		# 创建链接对象
		self.redis_client = redis.Redis(connection_pool=pool)

	def spider_opened(self, spider):
		pass

	def close_spider(self, spider):
		pass

	def process_request(self, request, spider):
		# # 代理服务器
		# proxyHost = "p5.t.16yun.cn"
		# proxyPort = "6445"

		# # 代理隧道验证信息
		# proxyUser = "16SQAAKI"
		# proxyPass = "739747"

		# request.meta['proxy'] = "http://{0}:{1}".format(proxyHost,proxyPort)

		# # 添加验证头
		# encoded_user_pass = base64ify(proxyUser + ":" + proxyPass)
		# request.headers['Proxy-Authorization'] = 'Basic ' + encoded_user_pass                    

		# # 设置IP切换头(根据需求)
		# tunnel = random.randint(1,10000)
		# request.headers['Proxy-Tunnel'] = str(tunnel)

		# headers = self.make_headers()
		# for key in headers.keys():
		#   request.headers[key] = headers[key]
		# request.meta['proxy'] = "http://{0}:{1}".format(proxyHost,proxyPort)

		try:
			spider_type = spider.alias_name
		except Exception as e:
			spider_type = spider.name

		request.meta['proxy'] = self.get_proxy_ip(spider_type)

	'''随机获取一个代理IP'''
	def get_proxy_ip(self, alias_name):
		now_time = int(time.time())
		if now_time - self.proxy_start_time >= 2:
			self.proxy_ip_list = self.get_ssdb_proxy_ip()
			self.proxy_start_time = now_time

		if len(self.proxy_ip_list) <= 0:
			self.proxy_ip_list = self.get_ssdb_proxy_ip()

		if len(self.proxy_ip_list) <= 0:
			time.sleep(5)
			return ''
		proxy = random.choice(self.proxy_ip_list)
		proxy = json.loads(proxy)
		ip = proxy['proxy']
		get_time = proxy['time']
		#如果ip获取时间超过3分钟 清理掉ip 并获取新的ip
		if now_time - get_time >= 165:
			self.redis_client.hdel(self.proxy_ip_hash, ip)
			self.redis_client.hdel(self.proxy_redis_detail, ip)
			return self.get_proxy_ip(alias_name)
		proxy_detail = self.redis_client.hget(self.proxy_redis_detail, ip)
		#如果在detail里面不存在ip 说明没被用过
		if not proxy_detail:
			#告诉redis  这个ip用了一次
			self.redis_client.hset(self.proxy_redis_detail, ip, json.dumps({
				'count': {alias_name: 1}
			}))
		#如果在detail里面存在ip 说明用过
		else:
			proxy_detail = json.loads(proxy_detail)
			#如果ip已经被用50次
			if max(proxy_detail['count'].values()) >= 40:
				#清理ip 重新获取
				self.redis_client.hdel(self.proxy_ip_hash, ip)
				self.redis_client.hdel(self.proxy_redis_detail, ip)
				return self.get_proxy_ip(alias_name)
			#如果用的次数少于50次
			else:
				#如果ip对这个接口使用过
				if alias_name in proxy_detail['count'].keys():
					proxy_detail['count'][alias_name] += 1
					#将这个接口的使用次数加1 存于redis
					self.redis_client.hset(self.proxy_redis_detail, ip, json.dumps(proxy_detail))
				# 如果ip对这个接口没有使用过 告诉redis 这个接口用了一次
				else:
					proxy_detail['count'][alias_name] = 1
					self.redis_client.hset(self.proxy_redis_detail, ip, json.dumps(proxy_detail))
		# self.save_count_log(json.dumps({
		# 	'ip': ip,
		# 	'proxy_detail': proxy_detail
		# }))
		return 'http://' + ip

	'''从ssdb读取代理IP列表'''
	def get_ssdb_proxy_ip(self):
		ips = self.redis_client.hvals(self.proxy_ip_hash)
		res = []
		for index in range(len(ips)):
			res.append(ips[index])
		if res:
			return res
		else:
			return []

	def save_count_log(self, info):
		date = time.strftime('%Y-%m-%d')

		file_path = '/data/spider/logs/proxy_fy_log/'
		if not os.path.exists(file_path):
			os.makedirs(file_path)

		data = '[' + str(time.strftime('%Y-%m-%d %H:%M:%S')) + '] ' + info

		file_name = file_path + '/' + date + '.log'
		with open(file_name, "a+") as f:
			f.write(data + "\r\n")

'''代理IP中间件'''
class Proxy2808Middleware(object):
	ssdb_host = ''
	ssdb_client = ''
	proxy_start_time= 0
	proxy_ip_list   = []

	proxy_redis_detail = 'proxy_2808_redis_detail'
	proxy_ip_hash      = 'proxy_ip_hash_2808_new'

	@classmethod
	def from_crawler(cls, crawler):
		s = cls(
			redis_host=crawler.settings.get('PROXY_REDIS_HOST'),
			redis_auth=crawler.settings.get('PROXY_REDIS_AUTH'),
		)
		crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
		return s

	def __init__(self, redis_host, redis_auth):
		pool = redis.ConnectionPool(host=redis_host, port=6379, db=10, password=redis_auth, decode_responses=True)
		# 创建链接对象
		self.redis_client = redis.Redis(connection_pool=pool)

	def spider_opened(self, spider):
		pass

	def close_spider(self, spider):
		pass

	def process_request(self, request, spider):
		try:
			spider_type = spider.alias_name
		except Exception as e:
			spider_type = spider.name

		request.meta['proxy'] = self.get_proxy_ip(spider_type)

	'''随机获取一个代理IP'''
	def get_proxy_ip(self, alias_name):
		now_time = int(time.time())
		if now_time - self.proxy_start_time >= 2:
			self.proxy_ip_list = self.get_ssdb_proxy_ip()
			self.proxy_start_time = now_time

		if len(self.proxy_ip_list) <= 0:
			self.proxy_ip_list = self.get_ssdb_proxy_ip()

		if len(self.proxy_ip_list) <= 0:
			time.sleep(5)
			return ''
		proxy = random.choice(self.proxy_ip_list)
		proxy = json.loads(proxy)
		ip = proxy['proxy']
		get_time = proxy['time']
		#如果ip获取时间超过3分钟 清理掉ip 并获取新的ip
		if now_time - get_time >= 165:
			self.redis_client.hdel(self.proxy_ip_hash, ip)
			self.redis_client.hdel(self.proxy_redis_detail, ip)
			return self.get_proxy_ip(alias_name)
		proxy_detail = self.redis_client.hget(self.proxy_redis_detail, ip)
		#如果在detail里面不存在ip 说明没被用过
		if not proxy_detail:
			#告诉redis  这个ip用了一次
			self.redis_client.hset(self.proxy_redis_detail, ip, json.dumps({
				'count': {alias_name: 1}
			}))
		#如果在detail里面存在ip 说明用过
		else:
			proxy_detail = json.loads(proxy_detail)
			#如果ip已经被用50次
			if max(proxy_detail['count'].values()) >= 20:
				#清理ip 重新获取
				self.redis_client.hdel(self.proxy_ip_hash, ip)
				self.redis_client.hdel(self.proxy_redis_detail, ip)
				return self.get_proxy_ip(alias_name)
			#如果用的次数少于50次
			else:
				#如果ip对这个接口使用过
				if alias_name in proxy_detail['count'].keys():
					proxy_detail['count'][alias_name] += 1
					#将这个接口的使用次数加1 存于redis
					self.redis_client.hset(self.proxy_redis_detail, ip, json.dumps(proxy_detail))
				# 如果ip对这个接口没有使用过 告诉redis 这个接口用了一次
				else:
					proxy_detail['count'][alias_name] = 1
					self.redis_client.hset(self.proxy_redis_detail, ip, json.dumps(proxy_detail))
		# self.save_count_log(json.dumps({
		# 	'ip': ip,
		# 	'proxy_detail': proxy_detail
		# }))
		return 'http://' + ip

	'''从ssdb读取代理IP列表'''
	def get_ssdb_proxy_ip(self):
		ips = self.redis_client.hvals(self.proxy_ip_hash)
		res = []
		for index in range(len(ips)):
			res.append(ips[index])
		if res:
			return res
		else:
			return []

	def save_count_log(self, info):
		date = time.strftime('%Y-%m-%d')

		file_path = '/data/spider/logs/proxy_2808_log/'
		if not os.path.exists(file_path):
			os.makedirs(file_path)

		data = '[' + str(time.strftime('%Y-%m-%d %H:%M:%S')) + '] ' + info

		file_name = file_path + '/' + date + '.log'
		with open(file_name, "a+") as f:
			f.write(data + "\r\n")

'''代理IP中间件'''
class Proxy16yMiddleware(object):
	ssdb_host = ''
	ssdb_client = ''
	proxy_start_time= 0
	proxy_ip_list   = []

	proxy_redis_detail = 'proxy_16y_redis_detail'
	proxy_ip_hash      = 'proxy_ip_hash_16y'

	@classmethod
	def from_crawler(cls, crawler):
		s = cls(
			redis_host=crawler.settings.get('PROXY_REDIS_HOST'),
			redis_auth=crawler.settings.get('PROXY_REDIS_AUTH'),
		)
		crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
		return s

	def __init__(self, redis_host, redis_auth):
		pool = redis.ConnectionPool(host=redis_host, port=6379, db=10, password=redis_auth, decode_responses=True)
		# 创建链接对象
		self.redis_client = redis.Redis(connection_pool=pool)

	def spider_opened(self, spider):
		pass

	def close_spider(self, spider):
		pass

	def process_request(self, request, spider):
		try:
			spider_type = spider.alias_name
		except Exception as e:
			spider_type = spider.name

		request.meta['proxy'] = self.get_proxy_ip(spider_type)

	'''随机获取一个代理IP'''
	def get_proxy_ip(self, alias_name):
		now_time = int(time.time())
		if now_time - self.proxy_start_time >= 2:
			self.proxy_ip_list = self.get_ssdb_proxy_ip()
			self.proxy_start_time = now_time

		if len(self.proxy_ip_list) <= 0:
			self.proxy_ip_list = self.get_ssdb_proxy_ip()

		if len(self.proxy_ip_list) <= 0:
			time.sleep(5)
			return ''
		proxy = random.choice(self.proxy_ip_list)
		proxy = json.loads(proxy)
		ip = proxy['proxy']
		get_time = proxy['time']
		#如果ip获取时间超过3分钟 清理掉ip 并获取新的ip
		if now_time - get_time >= 165:
			self.redis_client.hdel(self.proxy_ip_hash, ip)
			self.redis_client.hdel(self.proxy_redis_detail, ip)
			return self.get_proxy_ip(alias_name)
		proxy_detail = self.redis_client.hget(self.proxy_redis_detail, ip)
		#如果在detail里面不存在ip 说明没被用过
		if not proxy_detail:
			#告诉redis  这个ip用了一次
			self.redis_client.hset(self.proxy_redis_detail, ip, json.dumps({
				'count': {alias_name: 1}
			}))
		#如果在detail里面存在ip 说明用过
		else:
			proxy_detail = json.loads(proxy_detail)
			#如果ip已经被用50次
			if max(proxy_detail['count'].values()) >= 20:
				#清理ip 重新获取
				self.redis_client.hdel(self.proxy_ip_hash, ip)
				self.redis_client.hdel(self.proxy_redis_detail, ip)
				return self.get_proxy_ip(alias_name)
			#如果用的次数少于50次
			else:
				#如果ip对这个接口使用过
				if alias_name in proxy_detail['count'].keys():
					proxy_detail['count'][alias_name] += 1
					#将这个接口的使用次数加1 存于redis
					self.redis_client.hset(self.proxy_redis_detail, ip, json.dumps(proxy_detail))
				# 如果ip对这个接口没有使用过 告诉redis 这个接口用了一次
				else:
					proxy_detail['count'][alias_name] = 1
					self.redis_client.hset(self.proxy_redis_detail, ip, json.dumps(proxy_detail))
		# self.save_count_log(json.dumps({
		# 	'ip': ip,
		# 	'proxy_detail': proxy_detail
		# }))
		return 'http://' + ip

	'''从ssdb读取代理IP列表'''
	def get_ssdb_proxy_ip(self):
		ips = self.redis_client.hvals(self.proxy_ip_hash)
		res = []
		for index in range(len(ips)):
			res.append(ips[index])
		if res:
			return res
		else:
			return []

	def save_count_log(self, info):
		date = time.strftime('%Y-%m-%d')

		file_path = '/data/spider/logs/proxy_16y_log/'
		if not os.path.exists(file_path):
			os.makedirs(file_path)

		data = '[' + str(time.strftime('%Y-%m-%d %H:%M:%S')) + '] ' + info

		file_name = file_path + '/' + date + '.log'
		with open(file_name, "a+") as f:
			f.write(data + "\r\n")


'''阿布云隧道代理IP中间件'''
class AbProxyMiddleware(object):
	# 代理服务器
	proxyServer = "http://http-dyn.abuyun.com:9020"
	# 代理隧道验证信息
	proxyUser = "HX9O44N08H564FSD"
	proxyPass = "2E4C269712855E50"
	proxyAuth = "Basic " + base64.urlsafe_b64encode(bytes((proxyUser + ":" + proxyPass), "ascii")).decode("utf8")

	# @classmethod
	# def from_crawler(cls, crawler):
	# 	s = cls()
	# 	crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
	# 	return s

	# def __init__(self):
	# 	pass

	# def spider_opened(self, spider):
	# 	spider.logger.info('Spider opened: %s' % spider.name)

	# def close_spider(self, spider):
	# 	pass

	def process_request(self, request, spider):
		request.meta["proxy"] = self.proxyServer
		request.headers["Proxy-Authorization"] = self.proxyAuth

class GoodsMiddleware(object):
	def process_spider_output(self, response, result,spider):

		content 	= response.body.decode('utf-8')
		if content:
			goods_data	= json.loads(content)

		else:
			yield None








	
