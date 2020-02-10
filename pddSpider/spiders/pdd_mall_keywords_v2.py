# -*- coding: utf-8 -*-
import scrapy
import json, time, sys, random, re, pyssdb, logging, urllib.request, urllib.parse, pddSign,hashlib,hmac,os
import base64
from Crypto.Cipher import PKCS1_v1_5 as Cipher_pkcs1_v1_5
from Crypto.Cipher import AES
from Crypto.PublicKey import RSA
from spider.items import CategoryItem
from scrapy.utils.project import get_project_settings

'''获取店铺信息及销量记录'''
class PddMallKeywordsV2Spider(scrapy.Spider):
	name = 'pdd_mall_keywords_v2'
	laravel_cookie=''
	url = 'https://mms.pinduoduo.com/sydney/api/hotWord/queryHotWord'
	key = 'eNgWuwpQ84MZVTQbmdtHWTEwYXFibzDNXPSMP+B5VsA='
	cookie = ''
	# cookie = 'PASS_ID=1-vHzAnvDj7RIOTJp2bcNGeI3UlpChrpXot3npunwSHukZSbcMVgWxrVWWHYh1ZfAz7C6CG0PxUYX9QiZPxWYNfg_1375265_1624308'
	fail_nums = 5
	keyword_extend_hash = 'pdd_keywords_extend_hash'
	mall_id = 103270888
	username = '17132160414'
	password = 'Qq1835498692'
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
		self.get_pdd_login_info()
		if not self.cookie:
			return False

	def start_requests(self):
		all_category = self.get_all_category()
		if not all_category or not self.cookie:
			return False

		days = [1, 7]

		for cat in all_category:
			cat = json.loads(cat.decode('utf-8'))
			if type(cat) == int:
				continue

			meta = {'cat': cat, 'page':1, 'rank':0}
			headers = self.make_headers()
			for day in days:
				meta['day'] = day
				query_data = self.build_query_data(cat,meta['page'],self.size,day)
				yield scrapy.Request(self.url, method="POST", body=json.dumps(query_data), meta=meta, headers=headers,
									 callback=self.parse)

	'''抓取关键词热度 丰富度 排名'''
	def parse(self, response):
		result = json.loads(response.body.decode('utf-8'))
		logging.debug(json.dumps(result))
		if 'errorCode' in result.keys() and result['errorCode'] == 1000000:
			if result['result']['items'] is None:
				return None
			meta = response.meta
			cat = meta['cat']
			cat_id = cat['cat_id']
			level = cat['level']
			day = meta['day']
			rank = meta['rank']
			page = meta['page']

			# logging.log(logging.WARNING, str(cat_id)+'+'+str(len(result['result']['items'])))
			for keyword_item in result['result']['items']:
				keyword = keyword_item['query']
				rank += 1
				keyword_data = {}
				keyword_data['keyword'] = keyword
				keyword_data['category'] = cat_id
				keyword_data['cat_id_1'] = cat['cat_id_1']

				if level == 2 or level == 3:
					keyword_data['cat_id_2'] = cat['cat_id_2']
					if level == 3:
						keyword_data['cat_id_3'] = cat['cat_id_3']

				keyword_data['hotness'] = keyword_item['heat']
				keyword_data['richness'] = 27
				keyword_data['rank'] = rank
				keyword_data['day'] = day
				

				keywords_data = self.merge_keyword_data(keyword_data, {});
				keywordItem = CategoryItem()
				keywordItem['cat_list'] = keywords_data
				yield keywordItem


			if result['result']['count'] > rank and page<self.max_page:
				page += 1
				meta['page'] = page
				meta['rank'] = rank
				query_data = self.build_query_data(cat, page, self.size, day)
				headers = self.make_headers()
				yield scrapy.Request(self.url, method="POST", body=json.dumps(query_data), meta=meta, headers=headers,
									 callback=self.parse)

	def build_query_data(self, cat, page=1, page_size=10, spanDays=1):
		query_data = {'query': '', 'asc': 'false'}
		query_data['categoryIdOne'] = cat['cat_id_1']
		query_data['categoryIdTwo'] = 'null'
		query_data['categoryIdThree'] = 'null'
		if cat['level'] == 2:
			query_data['categoryIdTwo'] = cat['cat_id_2']

		if cat['level'] == 3:
			query_data['categoryIdTwo'] = cat['cat_id_2']
			query_data['categoryIdThree'] = cat['cat_id_3']
		query_data['spanDays'] = spanDays
		query_data['pageNumber'] = page
		query_data['pageSize'] = page_size
		return query_data

	'''获取关键词趋势 平均出价  竞争'''
	def parse_keyword_extra(self, response):
		keyword_data = response.meta['keyword_data']
		times = response.meta['times']
		query_data = response.meta['query_data']
		get_data = True

		result = json.loads(response.body.decode('utf-8'))
		if 'errorCode' in result.keys() and result['errorCode'] == 1000000:
			pass
			keyword_extend_data = result['result'][0]
			keyword = keyword_extend_data['word']

			keyword_merge_data = self.merge_keyword_data(keyword_data, keyword_extend_data)
			if keyword_merge_data is False:
				get_data = False
			else:
				self.ssdb_client.hset(self.keyword_extend_hash, keyword, json.dumps(keyword_extend_data))
		else:
			get_data = False

		if get_data is False:  ##未获取到数据
			if times < 3:  ##重试次数少于三次
				meta = {'keyword_data': keyword_data, 'times': times + 1, 'query_data': query_data}
				headers = self.make_headers()
				yield scrapy.Request(response.url, method="POST", meta=meta, body=query_data, headers=headers,
									 dont_filter=True, callback=self.parse_keyword_extra)
			else:
				keyword_merge_data = self.merge_keyword_data(keyword_data, {})
				get_data = True

		if get_data is True:
			keywordItem = CategoryItem()
			keywordItem['cat_list'] = keyword_merge_data
			yield keywordItem

	'''生成headers头信息'''
	def make_headers(self):
		chrome_version = str(random.randint(59, 63)) + '.0.' + str(random.randint(1000, 3200)) + '.94'
		headers = {
			"Host": "mms.pinduoduo.com",
			"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			"Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
			"Accept-Encoding": "gzip, deflate",
			"Referer": "https://mms.pinduoduo.com/sycm/search_data/keyword",
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

	'''获取拼多多后台登录信息'''
	def get_pdd_login_info(self):
		login_token = self.get_login_token()
		if login_token:
			self.cookie = 'PASS_ID=' + login_token + ';'

	def merge_keyword_data(self, keyword_data, extend_data):
		pass

		if len(extend_data) > 0:
			if extend_data['trend'] is None and extend_data['heat'] is None and extend_data['compete'] is None and \
					extend_data['avgBid'] is None:
				return False

			trend = extend_data['trend'] if extend_data['trend'] != None else 0
			heat = extend_data['heat'] if extend_data['heat'] != None else 0
			compete = extend_data['compete'] if extend_data['compete'] != None else 0
			avgBid = extend_data['avgBid'] if extend_data['avgBid'] != None else 0

		else:
			trend = 0
			heat = 0
			compete = 0
			avgBid = 0

		keyword_data['trend'] = trend
		keyword_data['heat'] = heat
		keyword_data['compete'] = compete
		keyword_data['avgBid'] = avgBid

		return keyword_data

	def get_login_token(self):  # 获取登录token
		url = 'http://47.107.76.156/api/open/pddmms/passId?username='+self.username+'&password='+self.password
		headers = {"username": 'dianba.main', "password": "sVqk34j2U82nXnhYPDx8nwdUHW693Gdr", 'Accept': "application/vnd.ddgj.v2+json"}
		request_url = urllib.request.Request(url=url, headers=headers)
		response = urllib.request.urlopen(request_url).read().decode('utf-8')
		login_token = json.loads(response)['passId']
		return login_token
