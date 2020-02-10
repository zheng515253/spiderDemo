# -*- coding: utf-8 -*-
import scrapy
import json, time, sys, random, re, pyssdb, os,setting
from spider.items import CategoryGoodsItem
from scrapy.utils.project import get_project_settings

'''获取店铺内产品销量信息'''
class PddGoodsSalesCheckV2Spider(scrapy.Spider):
	name = 'pdd_goods_sales_check_v2'
	ssdb = ''
	goods_list = 'pdd_goods_sales_check_list'
	proxy_start_time = 0
	proxy_ip_list = []
	custom_settings = {
		 'DOWNLOADER_MIDDLEWARES':{'spider.middlewares.AbProxyMiddleware':100},
		 # 'LOG_FILE':'',
		 # 'LOG_LEVEL':'DEBUG',
		 # 'LOG_ENABLED':True,
		 # 'DOWNLOAD_TIMEOUT':30,
		 # 'DOWNLOAD_DELAY':0.2,
		'DOWNLOAD_TIMEOUT':5, #超时时间
		'RETRY_ENABLED':False, ##是否重试
		}
	
	def __init__(self):
		self.ssdb_client = pyssdb.Client(get_project_settings().get('SSDB_HOST'), 8888)

	def start_requests(self):
		is_end = False
		end_flag = False

		while not is_end or not end_flag:
			wait_check_goods = self.ssdb_client.qpop_front(self.goods_list, 20)

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
				meta = {'goods_id': goods_id}
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

	'''异常错误处理 删掉失败代理IP'''
	def errback_httpbin(self, failure):
		request = failure.request
		meta = request.meta

		# 失败重新退回队列 // 考虑失败干掉不可用的商品
		goods_id = meta['goods_id']
		self.ssdb_client.qpush(self.goods_list, str(goods_id))
