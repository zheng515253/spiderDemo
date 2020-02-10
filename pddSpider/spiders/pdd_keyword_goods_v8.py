# -*- coding: utf-8 -*-
"""
搜索关键字获取产品排名，从MQ队列获取关键字数据
"""
import scrapy
import json, time, random, pyssdb, pddSign, urllib, redis,setting, logging, datetime
from spider.items import KeywordGoodsList
from scrapy.utils.project import get_project_settings

class PddKeywordGoodsV8Spider(scrapy.Spider):
	handle_httpstatus_list = [403]
	FEED_EXPORT_ENCODING = 'utf-8'
	name = 'pdd_keyword_goods_v8'

	alias_name = 'keyword_search'

	list_name = 'pdd_keyword_use_for_goods_list'  # 热搜词SSDB队列名每天需要爬的数据
	retry_list_name = 'pdd_keyword_use_for_goods_list_retry'  # 热搜词SSDB失败队列
	rank_list_name = 'pdd_keyword_use_for_goods_rank_list_new'  # 热搜词SSDB失败队列
	url = 'http://api.pinduoduo.com'
	size = 50  # 页码
	max_page = 5  # 最大抓取页数

	custom_settings = {
		'DOWNLOADER_MIDDLEWARES': {'spider.middlewares.ProxyMiddleware': 543},
		# 'LOG_FILE': '',
		# 'LOG_LEVEL': 'DEBUG',
		# 'LOG_ENABLED': True,
		'RETRY_ENABLED': False,
		'DOWNLOAD_TIMEOUT': 2,
		# 'RETRY_TIMES': 20,
		# 'DOWNLOAD_DELAY': 0.1,
		'RETRY_HTTP_CODECS':[403,429],
		'CONCURRENT_REQUESTS': 150
	}
	p_time = 0
	hash_num = 0
	process_nums = 1

	proxy_start_time= 0
	proxy_ip_list   = []
	current_proxy = ''
	proxy_count = 0

	def __init__(self, hash_num=0, process_nums=1, p_time=0):
		self.hash_num = int(hash_num)
		self.process_nums = int(process_nums)
		today = datetime.date.today()
		self.list_name = self.list_name + ':' + str(today)
		self.retry_list_name = self.retry_list_name + ':' + str(today)
		self.rank_list_name = self.rank_list_name + ':' + str(today)
		self.ssdb_client = pyssdb.Client(get_project_settings().get('SSDB_HOST'), 8888)
		self.p_time = int(time.time())
		if p_time == 0:
			self.p_time = int(time.time())
		else:
			self.p_time = p_time

		# 创建连接池
		pool = redis.ConnectionPool(host=get_project_settings().get('PROXY_REDIS_HOST'), port=6379, db=10, password=get_project_settings().get('PROXY_REDIS_AUTH'), decode_responses=True)
		# 创建链接对象
		self.redis_client = redis.Redis(connection_pool=pool)

	def start_requests(self):
		search = True
		while search:
			hot_keywords = self.ssdb_client.qpop_front(self.list_name, 100*int(self.process_nums))  # 获取队列中的热搜词数据
			if type(hot_keywords) == bool: ##没有数据返回
				search 	=	False
				continue
			elif type(hot_keywords) == bytes:
				hot_keywords = [hot_keywords]

			for hot_keyword in hot_keywords:
				hot_keyword = ''.join(json.loads(hot_keyword.decode('utf-8')))
				if not hot_keyword:
					search = False
					continue
				headers = self.make_headers(hot_keyword)
				page = 1
				meta = {'flip': '', 'page': page, 'keyword': hot_keyword, 'sort': 0}
				url = self.build_search_url(page, self.size, hot_keyword, '')
				yield scrapy.Request(url, meta=meta, callback=self.parse, headers=headers, dont_filter=True, errback=self.errback_httpbin)

	def parse(self, response):
		info = response.body.decode('utf-8')
		info = json.loads(info)
		logging.debug(json.dumps(info))
		if 'items' not in info.keys():
			self.err_after(response.meta)
			return None
		item_info = info['items']
		flip = info['flip']
		keyword = response.meta['keyword']
		sort = response.meta['sort']  # 上一页最后一个产品的排名
		item_list = []
		page = response.meta['page']
		proxy = response.meta['proxy']
		# print('parse_before', sort,len(item_info), keyword)
		# 返回有数据，处理数据
		if len(item_info) > 0:
			for value in item_info:
				sort = sort + 1
				# 判断是否推广
				if 'ad' in value.keys():
					mall_id = value['ad']['mall_id']
					is_ad = 1
					suggest_keyword = ''
				else:
					mall_id = 0
					is_ad = 0
					suggest_keyword = ''
				goods_info = value
				goods_info['keyword'] = keyword
				goods_info['sort'] = sort
				goods_info['p_time'] = self.p_time
				goods_info['mall_id'] = mall_id
				goods_info['is_ad'] = is_ad
				goods_info['suggest_keyword'] = suggest_keyword
				item_list.append(goods_info)
			# 处理单个关键字下所有产品的排名
			item = KeywordGoodsList()
			item['goods_list'] = item_list
			item['page'] = page
			item['keyword'] = keyword
			# print('parse_middle', sort,len(item_info), keyword)
			yield item
			page += 1  # 返回数据，页码加1，未返回数据，重新抓取
			# print('parse_after', sort,len(item_info), keyword)
			if page <= self.max_page:
				url = self.build_search_url(page, self.size, keyword, flip)
				headers = self.make_headers(keyword)
				meta = {'flip': flip, 'proxy': proxy, 'page': page, 'keyword': keyword, 'sort': sort}
				yield scrapy.Request(url, meta=meta, callback=self.parse, headers=headers, dont_filter=True, errback=self.errback_httpbin)
			else:
				self.ssdb_client.qpush_back(self.rank_list_name, json.dumps({'page': page, 'keyword': keyword, 'sort': sort, 'p_time':self.p_time}))  # 失败关键词重新放入队列

	'''生成headers头信息'''
	def make_headers(self, keyword):
		headers = {
			"User-Agent": 'android Mozilla/5.0 (Linux; Android 5.1.1; SM-A530F Build/LMY48Z) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/39.0.0.0 Safari/537.36  phh_android_version/3.11.0 phh_android_build/228842 phh_android_channel/anzhi',
			"AccessToken": "",
			"Referer": 'Android',
		}
		return headers

	def get_proxy_ip(self, refresh):
		if not refresh and self.proxy_count < 30:
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
			'ip': ip,
			'count': self.proxy_count
		}))

		return 'http://'+ip

	def get_ssdb_proxy_ip(self):
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

	'''异常错误处理 删掉失败代理IP'''
	def errback_httpbin(self, failure):
		request = failure.request
		meta = request.meta
		self.err_after(meta)

	def err_after(self, meta):
		keyword = meta['keyword']
		page = meta['page']
		sort = meta['sort']
		flip = meta['flip']
		# proxy_ip = meta['proxy']
		# proxy_ip = proxy_ip.replace("http://", "").encode("utf-8")

		# if proxy_ip in self.proxy_ip_list:
		# 	index = self.proxy_ip_list.index(proxy_ip)
		# 	del self.proxy_ip_list[index]
		# self.get_proxy_ip(True)

		if page > 1:
			self.ssdb_client.qpush_back(self.retry_list_name, json.dumps({'flip': flip, 'page': page, 'keyword': keyword, 'sort': sort, 'p_time': self.p_time}))  # 失败关键词重新放入队列
		else:
			# 第一页失败直接推回队列
			self.ssdb_client.qpush_back(self.list_name, json.dumps(keyword))  # 失败关键词重新放入队列

	# 构造链接
	def build_search_url(self, page, page_size, keyword, flip):
		pdd_sign = pddSign.pddSign()
		sort = 'default'
		requery = 0
		pdduid = 0
		# href = 'http://mobile.yangkeduo.com/search_result.html?search_key='+urllib.parse.quote(keyword)+'&search_src=new&search_met=btn_sort&search_met_track=manual&refer_page_name=search_result&refer_page_id=10015_1533352701631_UaSVb347wR&refer_page_sn=10015'
		# anti_content = pdd_sign.messagePackV2('0al', href)
		search_url = self.url + '/search?page='+str(page)+'&size='+str(page_size)+'&q='+urllib.parse.quote(keyword)
		if flip.strip() != '':
			search_url+='&flip='+flip
		return search_url
