# -*- coding: utf-8 -*-
import scrapy
import json, time, sys, random, re, pyssdb, pddSign, urllib, os, datetime, setting
from spider.items import GoodsSalesItem
from collections import deque
from scrapy.utils.project import get_project_settings

goods_list = []
'''获取店铺内产品销量信息'''
class PddGoodsSalesV3Spider(scrapy.Spider):
	name = 'pdd_goods_sales_v3'
	# mall_id_hash 	= 'pdd_mall_id_hash'
	# fail_hash  		= 'pdd_mall_id_fail_hash'
	
	'''待抓取产品销量的店铺列表'''
	mall_id_hash 	= 'pdd_crawl_mall_id_hash'
	mall_id_list 	= 'pdd_crawl_mall_id_list:'
	list_name		= ''
	hash_num 		= 0
	process_nums 	= 1
	limit			= 1000
	log_user_id     = 0
	redis_conn      = ''
	proxy_start_time= 0
	proxy_ip_list   = []
	proxy_ip_queue  = deque([])
	max_page		= 6  # 最大抓取页数
	#proxy 			= 'http://proxypdd:pTsnATTnj4qAAdvY@120.77.49.132:3128'
	custom_settings = {
		# 'LOG_FILE':'',
		# 'LOG_LEVEL':'DEBUG',
		# 'LOG_ENABLED':True,
		'DOWNLOAD_DELAY':0.015,
		'DOWNLOAD_TIMEOUT': 5,  # 超时时间
		'RETRY_ENABLED': False,  # 是否重试,
		'RETRY_HTTP_CODECS':[403,429],
		'CONCURRENT_REQUESTS': 60
		}

	def __init__(self, hash_num = 0, process_nums = 1):
		self.today = datetime.date.today()
		self.hash_num = int(hash_num) ##当前脚本号
		self.process_nums   = int(process_nums) ##脚本总数
		self.list_name = self.mall_id_list + str(self.today)
		self.pageSize = 1000 ##每次抓取的产品数 最大只返回500
		self.ssdb 	= pyssdb.Client('172.16.0.5', 8888)
		self.pdd_class = pddSign.pddSign()
		# #创建连接池
		# pool = redis.ConnectionPool(host=get_project_settings().get('PROXY_REDIS_HOST'),port=6379,db=0,password='20A3NBVJnWZtNzxumYOz',decode_responses=True)
		# #创建链接对象
		# self.redis_conn=redis.Redis(connection_pool=pool)

	def start_requests(self):
		mall_nums 		= 	self.limit * int(self.process_nums) ##一次查询的数量
		is_end 			=	False
		while not is_end:
			mall_ids 	=	self.ssdb.qpop_front(self.list_name, 600)
			if  not mall_ids: ##没有数据返回
				is_end 	=	True
				continue
			# self.log_user_id = self.get_uid();

			for mall_id in mall_ids:
				if type(mall_id) != int:
					mall_id = int( mall_id.decode('utf-8') )

				goods_list=[]
				page = 1

				headers = self.make_headers()
				url = self.build_url(mall_id, page, 50)

				meta = {'page':page, 'mall_id':mall_id, 'proxy':self.get_proxy_ip()}
				yield scrapy.Request(url, meta=meta, callback=self.parse, headers=headers ,dont_filter=True,errback=self.errback_httpbin)
			time.sleep(1) # 休眠1秒

	def parse(self, response):
		pass
		mall_id  = response.meta['mall_id'] ##店铺ID
		page 	 = response.meta['page'] ##每返回一次页面数据 记录页数
		proxy 	 = response.meta['proxy'] ##使用原始代理

		mall_goods = response.body.decode('utf-8') ##bytes转换为str
		#self.save_mall_log(mall_id, mall_goods)

		mall_goods = json.loads(mall_goods)

		if 'goods_list' not in mall_goods.keys():
			#self.ssdb.hset(self.fail_hash, mall_id, mall_id)
			return None

		goods_len  = len(mall_goods['goods_list'])

		# print(goods_len)

		if goods_len > 0:
			goods_list = mall_goods['goods_list'] ##合并产品列表
		else:
			return None

		if goods_list:
			item = GoodsSalesItem()
			item['goods_list'] = goods_list
			item['mall_id']    = mall_id
			#print(item)
			yield item

		if goods_len >= 30 and page < self.max_page:
			page += 1
			##继续采集下一页面
			url = self.build_url(mall_id, page, 50)
			meta = {'page':page, 'mall_id':mall_id, 'proxy':proxy}
			headers = self.make_headers()
			yield scrapy.Request(url, meta=meta, callback=self.parse, headers=headers, dont_filter=True,errback=self.errback_httpbin)

	'''生成headers头信息'''
	def make_headers(self):
		# chrome_version   = str(random.randint(59,63))+'.0.'+str(random.randint(1000,3200))+'.94'
		# headers = {
		# 	"Host":"mobile.yangkeduo.com",
		# 	"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
		# 	"Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
		# 	"Accept-Encoding":"gzip, deflate",
		# 	"Host":"yangkeduo.com",
		# 	"Referer":"http://yangkeduo.com/goods.html?goods_id=442573047&from_subject_id=935&is_spike=0&refer_page_name=subject&refer_page_id=subject_1515726808272_1M143fWqjQ&refer_page_sn=10026",
		# 	"Connection":"keep-alive",
		# 	'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/'+chrome_version+' Safari/537.36',
		# }
		# 
		headers = {
			# "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			# "Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
			# "Accept-Encoding":"gzip, deflate",
			"Referer":"Android",
			# "Connection":"keep-alive",
			'User-Agent':setting.setting().get_default_user_agent(),
			#'User-Agent':'Mozilla/5.0 (iPhone; CPU iPhone OS 11_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15F79 ===  iOS/11.4 Model/iPhone9,1 BundleID/com.xunmeng.pinduoduo AppVersion/4.15.0 AppBuild/1807251632 cURL/7.47.0',
			#'AccessToken':'TCEQ2DM4MRHLIDZVEB6MFP3JOENREVWSO2IH77PI3MUV4Q6GGF3A1017c59',
			"AccessToken": "",
			"Cookie": "api_uid="
		}
		
		# ip = str(random.randint(100, 200))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))
		# headers['CLIENT-IP'] 	=	ip
		# headers['X-FORWARDED-FOR']=	ip
		return headers

	def get_uid(self):
		#uids = ['2222222222', '3333333333','4444444444', '555555555', '6666666666','7777777777','8888888888', '9999999999']
		#return str( random.randint(1,10) ) + str( random.choice(uids) )
		uid = '13652208' + str(random.randint(20,99))
		return uid

	def build_url(self, mall_id,page=1, page_size=50):
		anti_content = self.pdd_class.messagePackV2('0al', 'http://mobile.yangkeduo.com/mall_page.html?mall_id='+str(mall_id)+'&item_index=0&sp=0')
		url = 'http://apiv4.yangkeduo.com/api/turing/mall/query_cat_goods?category_id=0&type=0&mall_id='+str(mall_id)+'&page_no='+str(page)+'&page_size='+str(page_size)+'&sort_type=default&pdduid=&page_from=39&anticontent='+urllib.parse.quote(anti_content)
		return url

	def get_proxy_ip(self):
		now_time = int(time.time())
		if now_time - self.proxy_start_time >= 5:
			self.proxy_ip_list = self.get_ssdb_proxy_ip()
			self.proxy_start_time = now_time
			temp = []
			for proxy in self.proxy_ip_list:
				temp.append({'last_used_at': 0, 'proxy': proxy})
			self.proxy_ip_queue = deque(temp)

		is_end = False
		ip = ''
		while not is_end:
			proxy = self.proxy_ip_queue.popleft()
			# print(proxy)
			if True or proxy['last_used_at'] < now_time - 3:
				is_end = True
				ip = proxy['proxy']
				proxy['last_used_at'] = now_time
				self.proxy_ip_queue.append(proxy)
				# print(proxy)
				continue
			time.sleep(1) # 休眠1秒
			now_time+=1
			self.proxy_ip_queue.append(proxy)

		# print(self.proxy_ip_queue)

		ip = ip.decode('utf-8')

		return 'http://'+ip
	
	'''从ssdb获取代理IP'''
	def get_ssdb_proxy_ip(self):
		ips = self.ssdb.hkeys('proxy_ip_hash', '', '', 1000)
		machine_type = setting.setting().machine_type()
		res = []
		for index in range(len(ips)):
			# if index % 2 != machine_type:
			# 	continue
			res.append(ips[index])
		if res:
			return res
		else:
			return []

	'''异常错误处理 删掉失败代理IP'''
	def errback_httpbin(self, failure):
		request = failure.request
		meta  = request.meta
		# proxy_ip = meta["proxy"]
		# proxy_ip = proxy_ip.replace("http://", "").encode("utf-8")

		# if proxy_ip in self.proxy_ip_list:
		# 	index = self.proxy_ip_list.index(proxy_ip)
		# 	del self.proxy_ip_list[index]

		# 推回队列
		mall_id = meta['mall_id']
		self.ssdb.qpush_back(self.list_name, str(mall_id))  # 失败关键词重新放入队列

	'''保存店铺产品原始数据'''
	def save_mall_log(self, mall_id, mall_info):
		date = time.strftime('%Y-%m-%d')

		file_path = '/data/spider/log/mall_log'
		if not os.path.exists(file_path):
			os.makedirs(file_path)

		file_name = file_path+'/'+date+'.log'
		with open(file_name, "a+") as f:
			f.write(mall_info+"\r\n")
