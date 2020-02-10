# -*- coding: utf-8 -*-
# 获取首页分类下的产品列表
import scrapy
import json, time, sys, random, pddSign, urllib,setting, pyssdb,string,os,uuid
from spider.items import CategoryGoodsItem
from scrapy.utils.project import get_project_settings

##引入错误处理模块
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError

class PddCategoryGoodsV5Spider(scrapy.Spider):
	name  = 'pdd_category_goods_v5'
	queue_name  = 'queue_pdd_category_goods_list'
	pagesise = 20
	proxy_start_time= 0
	proxy_ip_list   = []
	custom_settings = {
		# 'LOG_FILE': '',
		# 'LOG_LEVEL': 'DEBUG',
		# 'LOG_ENABLED': True,
		'RETRY_ENABLED': False,
		'DOWNLOAD_TIMEOUT': 5,
		# 'RETRY_TIMES': 20,
		'RETRY_HTTP_CODECS':[403,429],
		'DOWNLOAD_DELAY': 1,
		'CONCURRENT_REQUESTS': 10
		}

	def __init__(self, hash_num=0, process_nums=1):
		self.ssdb_client = pyssdb.Client(get_project_settings().get('SSDB_HOST'), 8888)
		self.hash_num = int(hash_num)  ##当前脚本号
		self.process_nums = int(process_nums)  ##脚本总数

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
				offset 		= 0

				url = self.build_url(opt_type, cat_id, offset, '')
				meta = {'cat_id':cat_id,'opt_type':opt_type,'offset':offset, 'proxy':self.get_proxy_ip(),'category':category}
				# print(url)
				headers = self.make_headers()
				yield scrapy.Request(url, meta=meta, callback=self.parse, headers=headers, dont_filter=True, errback=self.errback_httpbin)

	def parse(self, response):
		# print(response.meta)
		meta = response.meta
		offset = response.meta['offset']
		cat_id = response.meta['cat_id']
		opt_type = response.meta['opt_type']
		receive_info = response.body.decode('utf-8')
		# self.save_goods_log(cat_id, offset, receive_info)
		data = json.loads(receive_info)
		goods_lists = []
		# print('parse_before', flip, offset,len(data['goods_list']),cat_id)
		if 'goods_list' in data.keys():
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
				# print('parse_middle',flip,offset,len(data['goods_list']),cat_id)
				yield item
				# print('parse_after',flip,offset,len(data['goods_list']),cat_id)
				if i >= self.pagesise and offset < 900:
					url = self.build_url(opt_type, cat_id, offset, flip)
					meta['offset'] = offset
					headers = self.make_headers()
					yield scrapy.Request(url, meta=meta, callback=self.parse, headers=headers, dont_filter=True, errback=self.errback_httpbin)
		else:
			yield scrapy.Request(response.url, meta=meta, callback=self.parse, headers=response.headers, dont_filter=True, errback=self.errback_httpbin)

	'''生成headers头信息'''
	def make_headers(self):
		chrome_version   = str(random.randint(59,63))+'.0.'+str(random.randint(1000,3200))+'.94'
		headers = {
			# "Host":"mobile.yangkeduo.com",
			# "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			# "Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
			# "Accept-Encoding":"gzip, deflate",
			# "Referer":"http://yangkeduo.com/goods.html?goods_id=442573047&from_subject_id=935&is_spike=0&refer_page_name=subject&refer_page_id=subject_1515726808272_1M143fWqjQ&refer_page_sn=10026",
			# "Connection":"keep-alive",
			# 'User-Agent':'phh_android_version/Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/'+chrome_version+' Safari/537.36',
			#"Host":"mobile.yangkeduo.com",
			#"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			#"Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
			#"Accept-Encoding":"gzip, deflate",
			#"Host":"yangkeduo.com",
			"Referer":"",
			"Cookie":'',
			"AccessToken":"",
			#"Connection":"keep-alive",
			'User-Agent':'',
		}

		# ip = str(random.randint(100, 200))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))
		# headers['CLIENT-IP'] 	=	ip
		# headers['X-FORWARDED-FOR']=	ip
		return headers
	# 代理
	def get_proxy_ip(self):
		now_time = int(time.time())
		if now_time - self.proxy_start_time >= 10:
			self.proxy_ip_list = self.get_ssdb_proxy_ip()
			self.proxy_start_time = now_time

		if len(self.proxy_ip_list) <= 0:
			# print ('no ip')
			return ''

		ip = random.choice(self.proxy_ip_list)
		ip = ip.decode('utf-8')
		# print (ip)
		return 'http://'+ip

	def get_ssdb_proxy_ip(self):
		res = self.ssdb_client.hkeys('proxy_ip_hash', '', '', 1000)
		if res:
			return res
		else:
			return []

	def build_url(self, opt_type, cat_id, offset, flip):
		# pdd_sign = pddSign.pddSign()
		# href = 'http://mobile.yangkeduo.com/catgoods.html?opt_id='+str(cat_id)+'&opt_type='+str(opt_type)+'&opt_name=&opt_g=1&refer_page_name=search&refer_page_id=10031_1539251323283_teZmqPRkL8&refer_page_sn=10031&page_id=10028_1539251490140_R7nIY9glyC&is_back=1&sort_type=DEFAULT&act_status=0&opt_index='
		# anti_content = pdd_sign.messagePackV2('0al', href)
		# url = 'http://apiv4.yangkeduo.com/operation/'+str(cat_id)+'/groups?opt_type='+str(opt_type)+'&offset='+str(offset)+'&size='+str(self.pagesise)+'&sort_type=DEFAULT&pdduid=0&anti_content='+urllib.parse.quote(anti_content)
		url = 'http://apiv4.yangkeduo.com/operation/'+str(cat_id)+'/groups?list_id='+self.get_list_id(cat_id)+'opt_type='+str(opt_type)+'&size='+str(self.pagesise)+'&offset='+str(offset)
		# if flip.strip()!='':
		# 	url+='&flip='+flip
		return url

	def get_list_id(self, opt_id):
		uuid_string = str(uuid.uuid1())
		uuid_string = ''.join(uuid_string.split('-'))
		uuid_string = uuid_string.replace("-", "")
		uuid_string = uuid_string[0:10]
		return str(opt_id) + '_' + uuid_string
		# return str(opt_id) + '_rec_list_catgoods_' + ''.join(random.sample(string.ascii_letters + string.digits, 6))

	def errback_httpbin(self, failure):
		request = failure.request
		#headers = self.make_headers()
		#meta = {'proxy':self.proxy}
		meta = request.meta

		proxy_ip = meta["proxy"]
		proxy_ip = proxy_ip.replace("http://", "").encode("utf-8")

		if proxy_ip in self.proxy_ip_list:
			index = self.proxy_ip_list.index(proxy_ip)
			del self.proxy_ip_list[index]

		category = meta['category']
		self.ssdb_client.qpush_back(self.queue_name, category)  # 失败重新放入队列
		'''
		# log all failures
		self.logger.error(repr(failure))

		# in case you want to do something special for some errors,
		# you may need the failure's type:

		if failure.check(HttpError):
			# these exceptions come from HttpError spider middleware
			# you can get the non-200 response
			response = failure.value.response
			self.logger.error('HttpError on %s', response.url)

		elif failure.check(DNSLookupError):
			# this is the original request
			request = failure.request
			self.logger.error('DNSLookupError on %s', request.url)

		elif failure.check(TimeoutError, TCPTimedOutError):
			request = failure.request
			self.get_proxy_ip(self.proxy)
			headers = self.make_headers()
			meta = {'proxy':self.proxy}
			yield scrapy.Request(request.url, meta=meta, callback=self.parse, headers=headers,errback=self.errback_httpbin)
			#self.logger.error('TimeoutError on %s', request.url)
		'''

	def save_goods_log(self, cat_id, page, data):
		date = time.strftime('%Y-%m-%d')

		file_path = '/data/spider/logs/category_log/'+time.strftime('%Y-%m-%d')
		if not os.path.exists(file_path):
			os.makedirs(file_path)

		file_path += '/'+str(cat_id)
		if not os.path.exists(file_path):
			os.makedirs(file_path)

		file_name = file_path+'/'+str(page)+'.log'
		with open(file_name, "a+") as f:
			f.write(data+"\r\n")