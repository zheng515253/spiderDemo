# -*- coding: utf-8 -*-
import scrapy
import json, time, sys, random, re, pyssdb, logging, urllib.request, urllib.parse, pddSign,hashlib,hmac,os
import base64
from Crypto.Cipher import PKCS1_v1_5 as Cipher_pkcs1_v1_5
from Crypto.Cipher import AES
from Crypto.PublicKey import RSA
from spider.items import CategoryItem
from scrapy.utils.project import get_project_settings
from urllib import error

'''获取店铺信息及销量记录'''
class PddMallKeywordsV3Spider(scrapy.Spider):
	name = 'pdd_promotion_keywords_v3'
	laravel_cookie=''
	url = 'https://mms.pinduoduo.com/venus/api/subway/analysis/queryKeywordRank'
	key = 'eNgWuwpQ84MZVTQbmdtHWTEwYXFibzDNXPSMP+B5VsA='
	cookie = ''
	# cookie = 'PASS_ID=1-vHzAnvDj7RIOTJp2bcNGeI3UlpChrpXot3npunwSHukZSbcMVgWxrVWWHYh1ZfAz7C6CG0PxUYX9QiZPxWYNfg_1375265_1624308'
	fail_nums = 5
	mall_info = {'mall_id': 515006362}
	username = '13084964646'
	password = 'QWEqwe123'
	category_info = {}
	size = 10  # 页码
	max_page = 30  # 最大抓取页数

	custom_settings = {
		'DOWNLOAD_TIMEOUT':5,
		# 'LOG_FILE':'',
		# 'LOG_LEVEL':'DEBUG',
		# 'LOG_ENABLED':True,
		'DOWNLOAD_DELAY':0.1,
	}

	def __init__(self):
		self.ssdb_client = pyssdb.Client(get_project_settings().get('SSDB_HOST'), 8888)
		self.key = base64.b64decode(self.key)
		self.login_duodian()
		self.pdd_class = pddSign.pddSign()

	def start_requests(self):
		url = 'http://47.107.66.170/api/bindMall/pddMmsToken'
		now = int(time.time())
		salt = str(random.randint(100000, 999999))
		token = salt + str(now) + salt
		SIGN = {'time':now, 'salt':salt, 'token':hashlib.md5(token.encode(encoding='UTF-8')).hexdigest()}
		data = {'username':self.username,'password':self.password}
		headers = {'Accept':'application/vnd.ddgj.v2+json', 'SIGN':self.aes_encrypt(SIGN),'Cookie':self.laravel_cookie}
		print(data, headers)
		yield scrapy.Request(url, method="POST", body=json.dumps(data), meta={}, headers=headers, callback=self.parse_duodian_login)

		# all_category = self.get_all_category()
		# if not all_category:
		# 	return False

		# days = [1, 3, 7, 15]
		# rankTypes = [1, 2]

		# for cat in all_category:
		# 	cat = json.loads(cat.decode('utf-8'))
		# 	if type(cat) == int:
		# 		continue

		# 	meta = {'cat': cat, 'page':1, 'rank':0}
		# 	headers = self.make_headers()
		# 	for day in days:
		# 		meta['day'] = day
		# 		for rankType in rankTypes:
		# 			meta['rank_type'] = rankType
		# 			query_data = self.build_query_data(cat,meta['page'],self.size,day, rankType)
		# 			yield scrapy.Request(self.url, method="POST", body=json.dumps(query_data), meta=meta, headers=headers,
		# 								 callback=self.parse)
		pass

	def parse_duodian_login(self, response):
		result = json.loads(response.body.decode('utf-8'))
		logging.debug(json.dumps(result))
		self.cookie = 'PASS_ID=' + result['passId'] + ';'
		yield scrapy.Request("https://mms.pinduoduo.com/earth/api/user/userinfo", 
			method="POST", body=json.dumps({
				"sceneId": 15
			}), meta={}, headers={
				'Content-Type': 'application/json',
				'Cookie': self.cookie,
				'Referer': '',
			}, callback=self.parse_mall_info)

	def parse_mall_info(self, response):
		result = json.loads(response.body.decode('utf-8'))
		logging.debug(json.dumps(result))
		self.mall_info = result['result']['mall']
		logging.debug(json.dumps(self.mall_info))
		href = 'https://mms.pinduoduo.com/exp/tools/dataAnalysis'
		yield scrapy.Request("https://mms.pinduoduo.com/venus/api/subway/analysis/queryCategoryInfo",
			method="POST", body=json.dumps({
				"mallId": self.mall_info['mall_id'],
				'crawlerInfo': self.pdd_class.messagePackV2('0al', href)
			}), meta={}, headers={
				'Content-Type': 'application/json',
				'Cookie': self.cookie,
				'Referer':'https://mms.pinduoduo.com/exp/tools/dataAnalysis',
			}, callback=self.parse_category_info)

	def parse_category_info(self, response):
		result = json.loads(response.body.decode('utf-8'))
		logging.debug(json.dumps(result))
		self.category_info = result['result']
		logging.debug(json.dumps(self.category_info))

	'''抓取关键词热度 丰富度 排名'''
	def parse(self, response):
		result = json.loads(response.body.decode('utf-8'))
		logging.debug(json.dumps(result))
		if 'errorCode' in result.keys() and result['errorCode'] == 1000:
			if result['result'] is None:
				return None
			meta = response.meta
			cat = meta['cat']
			cat_id = cat['cat_id']
			level = cat['level']
			day = meta['day']
			rank_type = meta['rank_type']

			# logging.log(logging.WARNING, str(cat_id)+'+'+str(len(result['result']['items'])))
			for keyword_item in result['result']:
				keyword_data = {'category':cat_id, 'day':day, 'rank_type': rank_type}
				keyword_data['cat_id_1'] = cat['cat_id_1']

				if level == 2 or level == 3:
					keyword_data['cat_id_2'] = cat['cat_id_2']
					if level == 3:
						keyword_data['cat_id_3'] = cat['cat_id_3']

				keyword_data['rank_num'] = keyword_item['rankNum']
				keyword_data['click_num'] = keyword_item['clickNum']
				keyword_data['compete_value'] = keyword_item['competeValue']
				keyword_data['ctr'] = keyword_item['ctr']
				keyword_data['cvr'] = keyword_item['cvr']
				keyword_data['impr_avg_bid'] = keyword_item['imprAvgBid']
				keyword_data['pv'] = keyword_item['pv']
				keyword_data['word'] = keyword_item['word']

				keywordItem = CategoryItem()
				keywordItem['cat_list'] = keyword_data
				yield keywordItem

	def build_query_data(self, cat, page=1, page_size=10, spanDays=1, RankType=1):
		query_data = {'catId': ''}
		query_data['catId'] = cat['cat_id_1']
		if cat['level'] == 2:
			query_data['catId'] = cat['cat_id_2']
		if cat['level'] == 3:
			query_data['catId'] = cat['cat_id_2']
		query_data['dateRange'] = spanDays
		query_data['mallId'] = self.mall_info['mall_id']
		# query_data['pageSize'] = page_size
		query_data['keywordRankType'] = RankType
		query_data['beginDate'] = ''
		query_data['endDate'] = ''
		href = 'https://mms.pinduoduo.com/exp/tools/dataAnalysis'
		query_data['crawlerInfo'] = self.pdd_class.messagePackV2('0al', href)
		return query_data

	'''生成headers头信息'''
	def make_headers(self):
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
			'Cookie': self.cookie,
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

	def create_sra_public_key(self, password):
		url = "https://mms.pinduoduo.com/earth/api/queryPasswordEncrypt"
		# query_data = {}
		# headers = {
		#     'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15G77 pddmt_ios_version/2.2.1 pddmt_ios_build/1809091638 ===  iOS/11.4.1 Model/iPhone9,1 BundleID/com.xunmeng.merchant AppVersion/2.2.1 AppBuild/1809091638',
		#     'Content-type': 'application/json'
		# }
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

	def aes_encrypt(self,data): 
		key=self.key  #加密时使用的key，只能是长度16,24和32的字符串
		iv = os.urandom(16)
		string = json.dumps(data).encode('utf-8')
		padding = 16 - len(string) % 16
		string += bytes(chr(padding) * padding, 'utf-8')
		value = base64.b64encode(self.mcrypt_encrypt(string, iv))
		iv = base64.b64encode(iv)
		mac = hmac.new(key, iv+value, hashlib.sha256).hexdigest()
		dic = {'iv': iv.decode(), 'value': value.decode(), 'mac': mac}
		return base64.b64encode(bytes(json.dumps(dic), 'utf-8'))

	def mcrypt_encrypt(self, value, iv):
		key=self.key
		AES.key_size = 128
		crypt_object = AES.new(key=key, mode=AES.MODE_CBC, IV=iv)
		return crypt_object.encrypt(value)

	def login_duodian(self):
		i = 0
		while True:
			url = 'http://47.107.66.170/api/user/login'
			now = int(time.time())
			salt = str(random.randint(100000, 999999))
			token = salt + str(now) + salt
			SIGN = {'time':now, 'salt':salt, 'token':hashlib.md5(token.encode(encoding='UTF-8')).hexdigest()}
			data = {'username':'WTF','password':'jhc123456'}
			data = urllib.parse.urlencode(data).encode('utf-8')
			headers = {'Accept':'application/vnd.ddgj.v2+json', 'SIGN':self.aes_encrypt(SIGN)}
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
