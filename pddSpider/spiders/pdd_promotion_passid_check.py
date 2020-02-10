# -*- coding: utf-8 -*-
import redis
import scrapy
import json, time, sys, random, re, pyssdb, logging, urllib.request, urllib.parse, pddSign, hashlib, hmac, os
import base64
from Crypto.Cipher import PKCS1_v1_5 as Cipher_pkcs1_v1_5
from Crypto.Cipher import AES
from Crypto.PublicKey import RSA
from spider.items import CategoryItem
from scrapy.utils.project import get_project_settings
from urllib import error


'''获取店铺信息及销量记录'''
class PddMallKeywordsV5Spider(scrapy.Spider):
	name = 'pdd_promotion_passid_check'
	laravel_cookie=''
	url = 'https://mms.pinduoduo.com/venus/api/subway/analysis/queryKeywordRank'
	key = 'eNgWuwpQ84MZVTQbmdtHWTEwYXFibzDNXPSMP+B5VsA='
	fail_nums = 5
	# 76个店铺id 和 passId
	cookie = ''
	username = '18682498600'
	password = 'Zwmyhj*660730'
	category_info = {}
	size = 10  # 页码
	max_page = 30  # 最大抓取页数
	mall_pass_id_key = "PDDMMS_PASSID_COLLECTION_HASH"
	mall_pass_queue_key = 'pddmms_passid_hash'
	days = [1, 3, 7, 15]
	rankTypes = [1, 2]
	mall_pass_dict = dict()
	custom_settings = {
		'DOWNLOAD_TIMEOUT': 5,
		# 'LOG_FILE':'',
		# 'LOG_LEVEL':'DEBUG',
		# 'LOG_ENABLED':True,
		'DOWNLOAD_DELAY':0.1,
	}

	def __init__(self):
		self.ssdb_client = pyssdb.Client(get_project_settings().get('SSDB_HOST'), 8888)
		self.key = base64.b64decode(self.key)
		pool = redis.ConnectionPool(host=get_project_settings().get('TOKEN_REDIS_HOST'), port='6379', db=10, password=get_project_settings().get("PROXY_REDIS_AUTH"))
		self.redis_client = redis.Redis(connection_pool=pool)
		self.get_pdd_login_info()
		if not self.cookie:
			return False
		self.pdd_class = pddSign.pddSign()

	def start_requests(self):
		self.get_mall_id_duodian()
		self.get_mall_id_dianba()
		cat_id_1_list = self.mall_pass_dict.keys()
		if not cat_id_1_list:
			logging.debug(cat_id_1_list)
			return
		for key, value in cat_id_1_list:
			for i in value:
				mall_id = i["mall_id"]
				pass_id = i["pass_id"]
				logging.debug("mall_pass_id:" + str(mall_id) + ";" + str(pass_id))
				cookie = 'PASS_ID' + '=' + pass_id + ";"
				logging.debug(self.cookie)
				href = 'https://mms.pinduoduo.com/exp/tools/dataAnalysis'
				yield scrapy.Request("https://mms.pinduoduo.com/venus/api/subway/analysis/queryCategoryInfo",
					method="POST", body=json.dumps({
						"mallId": mall_id,
						'crawlerInfo': self.pdd_class.messagePackV2('0al', href)
					}), meta={"mallId": mall_id, "cookie": cookie, "passId": pass_id, "cat_id_1": key}, headers={
						'Content-Type': 'application/json',
						'Cookie': cookie,
						'Referer': 'https://mms.pinduoduo.com/exp/tools/dataAnalysis',
					}, callback=self.parse_category_info, errback=self.err_parse)

	def parse_category_info(self, response):
		mall_id = response.meta["mallId"]
		pass_id = response.meta['passId']
		cat_id_1 = response.meta["cat_id_1"]
		result = json.loads(response.body.decode('utf-8'))
		if result["success"] == True:
			log = json.dumps({'mall_id': str(mall_id), "pass_id": str(pass_id)})
			self.save_log(log)
		elif result["success"] == False:
			log = json.dumps({'mall_id': str(mall_id), "pass_id": str(pass_id), "result": result})
			self.save_log(log)
			self.redis_client.hdel(self.mall_pass_id_key, mall_id)

	def err_parse(self, failure):
		request = failure.request
		meta = request.meta
		mall_id = meta["mallId"]
		pass_id = meta['passId']
		cookie = meta['cookie']
		logging.debug(request)
		log = json.dumps({'mall_id': str(mall_id), "pass_id": str(pass_id), 'request': "false"})
		self.save_request_error_log(log)
		params_dict = {"mallId": mall_id, "passId": pass_id}
		self.redis_client.lpush(self.mall_pass_queue_key, json.dumps(params_dict))
		href = 'https://mms.pinduoduo.com/exp/tools/dataAnalysis'
		yield scrapy.Request("https://mms.pinduoduo.com/venus/api/subway/analysis/queryCategoryInfo",
				 method="POST", body=json.dumps({"mallId": mall_id,
				'crawlerInfo': self.pdd_class.messagePackV2('0al', href)}),
				meta={"mallId": mall_id, "cookie": cookie, "passId": pass_id}, headers={
				'Content-Type': 'application/json',
				'Cookie': cookie,
				'Referer': 'https://mms.pinduoduo.com/exp/tools/dataAnalysis',
			}, callback=self.parse_category_info)

	def build_query_data(self, cat, page=1, page_size=10, spanDays=1, RankType=1):
		query_data = {'catId': ''}
		query_data['catId'] = cat['cat_id_1']
		if cat['level'] == 2:
			query_data['catId'] = cat['cat_id_2']
		if cat['level'] == 3:
			query_data['catId'] = cat['cat_id_2']
		query_data['dateRange'] = spanDays
		query_data['mallId'] = cat['mallId']
		# query_data['pageSize'] = page_size
		query_data['keywordRankType'] = RankType
		query_data['beginDate'] = ''
		query_data['endDate'] = ''
		href = 'https://mms.pinduoduo.com/exp/tools/dataAnalysis'
		query_data['crawlerInfo'] = self.pdd_class.messagePackV2('0al', href)
		return query_data

	'''生成headers头信息'''
	def make_headers(self, cookie):
		chrome_version = str(random.randint(59, 63)) + '.0.' + str(random.randint(1000, 3200)) + '.94'
		headers = {
			"Host": "mms.pinduoduo.com",
			"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
			"Accept-Encoding": "gzip, deflate",
			"Referer": "https://mms.pinduoduo.com/exp/tools/dataAnalysis",
			"Connection": "keep-alive",
			'Content-Type': 'application/json',
			'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/' + chrome_version + ' Safari/537.36',
			'Cookie': cookie,
		}

		ip = str(random.randint(100, 200)) + '.' + str(random.randint(1, 255)) + '.' + str(
			random.randint(1, 255)) + '.' + str(random.randint(1, 255))
		headers['CLIENT-IP'] = ip
		headers['X-FORWARDED-FOR'] = ip
		return headers

	def get_all_category(self):
		category_hash = 'pdd_mall_category_hash'
		category_data = self.ssdb_client.hgetall(category_hash)
		if not category_data:
			return False

		return category_data

	'''获取拼多多后台登录信息'''
	def get_pdd_login_info(self):
		login_token = self.get_access_token()
		self.cookie = 'PASS_ID=' + login_token + ';'

	def create_sra_public_key(self, password):
		url = "https://mms.pinduoduo.com/earth/api/queryPasswordEncrypt"
		request = urllib.request.Request(url=url)
		login_data = urllib.request.urlopen(request)
		login_data = json.loads(login_data.read().decode('utf-8'))
		public_key = login_data['result']['publicKey']
		public_key2 = """-----BEGIN PUBLIC KEY-----
		%s
		-----END PUBLIC KEY-----"""%(public_key)
		ras_key = RSA.importKey(public_key2)
		cipher = Cipher_pkcs1_v1_5.new(ras_key)
		p = cipher.encrypt(password.encode(encoding='utf-8'))
		cipher_text = base64.b64encode(p)
		return str(cipher_text, encoding="utf-8")

	def get_access_token(self):
		self.login_duodian()
		i = 0
		while True:
			url = 'http://47.107.76.156/api/bindMall/pddMmsToken'
			now = int(time.time())
			salt = str(random.randint(100000, 999999))
			token = salt + str(now) + salt
			SIGN = {'time': now, 'salt': salt, 'token': hashlib.md5(token.encode(encoding='UTF-8')).hexdigest()}
			data = {'username': self.username, 'password': self.password}
			data = urllib.parse.urlencode(data).encode('utf-8')
			# data = json.dumps(data).encode('utf-8')
			headers = {'Accept': 'application/vnd.ddgj.v2+json', 'SIGN':self.aes_encrypt(SIGN),'Cookie':self.laravel_cookie}
			logging.debug({
				'type': 'token',
				'SIGN': SIGN,
				'data': data,
				'headers': headers,
			})
			new_url = urllib.request.Request(url, data, headers)
			response = urllib.request.urlopen(new_url).read().decode('utf-8')
			logging.debug(response)
			login_token = json.loads(response)['passId']
			if login_token:
				return login_token
			i += 1
			if i > 5:
				return ''

	def aes_encrypt(self, data):
		key=self.key  #加密时使用的key，只能是长度16,24和32的字符串
		iv = os.urandom(16)
		string = json.dumps(data).encode('utf-8')
		padding = 16 - len(string) % 16
		string += bytes(chr(padding) * padding, 'utf-8')
		value = base64.b64encode(self.mcrypt_encrypt(string, iv))
		iv = base64.b64encode(iv)
		mac = hmac.new(key, iv+value, hashlib.sha256).hexdigest()
		dic = {'iv': iv.decode(), 'value': value.decode(), 'mac': mac}
		return base64.b64encode(bytes(json.dumps(dic), 'utf-8')).decode('utf-8')

	def mcrypt_encrypt(self, value, iv):
		key = self.key
		AES.key_size = 128
		crypt_object = AES.new(key=key, mode=AES.MODE_CBC, IV=iv)
		return crypt_object.encrypt(value)

	def login_duodian(self):
		i = 0
		while True:
			url = 'http://47.107.76.156/api/user/loginV2'
			now = int(time.time())
			salt = str(random.randint(100000, 999999))
			token = salt + str(now) + salt
			SIGN = {'time':now, 'salt':salt, 'token':hashlib.md5(token.encode(encoding='UTF-8')).hexdigest()}
			data = {'username':'dianba','password':'dianba123456'}
			headers = {'Accept':'application/vnd.ddgj.v2+json', 'SIGN':self.aes_encrypt(SIGN)}
			logging.debug({
				'type': 'login',
				'SIGN': SIGN,
				'data': data,
				'headers': headers,
			})
			data = urllib.parse.urlencode(data).encode('utf-8')
			new_url = urllib.request.Request(url, data, headers)
			response = urllib.request.urlopen(new_url)
			res_headers = response.getheaders()
			result_body = response.read().decode('utf-8')
			# print(res_headers, result_body)
			d = {}
			for k, v in res_headers:
				if "Set-Cookie" in k:
					d[k]=v
			self.laravel_cookie = d['Set-Cookie'].split(';')[0]
			if self.laravel_cookie:
				return ''
			i += 1
			if i > 5:
				return ''

	def get_mall_id_dianba(self):
		""" 获取店铺id和pass_id"""
		url = 'http://www.dianba6.com/api/open/pddmms/admalls'
		request_url = urllib.request.Request(url=url)
		response = urllib.request.urlopen(request_url)
		mall_id_dict = eval(response.read().decode("utf-8"))

		for key, value in mall_id_dict.items():
			list_mall = list()
			for i in value:
				dict_mall = dict()
				dict_mall["passId"] = i["passId"]
				dict_mall["mallId"] = i["mallId"]
				list_mall.append(dict_mall)
			try:
				mall_id_list = self.mall_pass_dict[key]
			except Exception:
				mall_id_list = None
			if mall_id_list:
				mall_id_list.extend(list_mall)
				self.mall_pass_dict[key] = mall_id_list
			else:
				self.mall_pass_dict[key] = list_mall
		return self.mall_pass_dict

	def get_mall_id_duodian(self):
		""" 获取多店店铺id和pass_id"""
		url = 'http://47.107.76.156/api/open/pddmms/admallsV2/'
		headers = {"username": 'dianba.main', "password": "sVqk34j2U82nXnhYPDx8nwdUHW693Gdr",
				   'Accept': "application/vnd.ddgj.v2+json"}
		request_url = urllib.request.Request(url=url, headers=headers)
		response = urllib.request.urlopen(request_url)
		mall_id_dict = eval(response.read().decode("utf-8"))
		for key, value in mall_id_dict.items():
			list_mall = list()
			for i in value:
				dict_mall = dict()
				dict_mall["passId"] = i["passId"]
				dict_mall["mallId"] = i["mallId"]
				list_mall.append(dict_mall)
			try:
				mall_id_list = self.mall_pass_dict[key]
			except Exception:
				mall_id_list = None
			if mall_id_list:
				mall_id_list.extend(list_mall)
				self.mall_pass_dict[key] = mall_id_list
			else:
				self.mall_pass_dict[key] = list_mall
		return self.mall_pass_dict

	def save_log(self, content):
		date = time.strftime('%Y-%m-%d')
		file_path = '/data/spider/log/passid_check'
		if not os.path.exists(file_path):
			os.makedirs(file_path)
		# data = '[' + str(time.strftime('%Y-%m-%d %H:%M:%S')) + '] ' + content
		content = content + ","
		file_name = file_path + '/' + date + "check_.log"
		with open(file_name, 'a+') as f:
			f.write(content + "\r\n")

	def save_request_error_log(self, content):
		date = time.strftime('%Y-%m-%d')
		file_path = '/data/spider/log/passid_request_error_check'
		if not os.path.exists(file_path):
			os.makedirs(file_path)
		# data = '[' + str(time.strftime('%Y-%m-%d %H:%M:%S')) + '] ' + content
		content = content + ","
		file_name = file_path + '/' + date + "check_.log"
		with open(file_name, 'a+') as f:
			f.write(content + "\r\n")

	def save_request_log(self, content):
		date = time.strftime('%Y-%m-%d')
		file_path = '/data/spider/log/passid_request_check'
		if not os.path.exists(file_path):
			os.makedirs(file_path)
		# data = '[' + str(time.strftime('%Y-%m-%d %H:%M:%S')) + '] ' + content
		content = content + ","
		file_name = file_path + '/' + date + "check_.log"
		with open(file_name, 'a+') as f:
			f.write(content + "\r\n")