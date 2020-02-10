# -*- coding: utf-8 -*-
import scrapy
import json, time, sys, random, pyssdb, re

from scrapy.utils.project import get_project_settings

from spider.items import SpiderItem
#from mq.pdd_goods_mq import pdd_goods_mq

'''获取产品信息'''
class PddGoodsNewSpider(scrapy.Spider):
	name = 'pdd_goods_new'
	hash_name = ''
	ssdb_client = ''
	proxy_start_time = 0
	proxy_ip_list = []
	endpoint = 'http://apiv4.yangkeduo.com'

	def __init__(self, hash_name = ''):
		self.ssdb_client = pyssdb.Client(get_project_settings().get('SSDB_HOST'), 8888)
		self.hash_name = hash_name

	def start_requests(self):
		is_end  = False

		while not is_end:
			data = self.ssdb_client.hkeys(self.hash_name, '', '', 100) ##从ssdb拉取goods_id数据

			if not data:
				time.sleep(30)
				is_end = True
				continue

			for goods_id in data:
				goods_id = goods_id.decode('utf-8')
				
				meta = {'goods_id': goods_id, 'proxy': self.get_proxy_ip()}
				headers = self.make_headers()
				# url = 'http://mobile.yangkeduo.com/goods.html?goods_id='+str(goods_id)
				url = self.endpoint + '/api/oakstc/v14/goods/' + str(goods_id)
				yield scrapy.Request(url, meta=meta, callback=self.parse, headers=headers, dont_filter=True, errback=self.errback_httpbin)

	def parse(self, response):
		pass
		goods_id = response.meta['goods_id']
		self.ssdb_client.hdel(self.hash_name, goods_id)

		content = response.body.decode('utf-8')
		a = json.loads(content)  # re.search('window\.rawData= (.*)\;\s*\<\/script\>', content)
		if a:
			goods = a
			goods_data = SpiderItem()
			goods_data['goods_id'] = goods['goods_id']
			goods_data['mall_id'] = goods['mall_id']
			goods_data['goods_type'] = goods['goods_type']
			goods_data['category1'] = str(goods['cat_id_1'])
			goods_data['category2'] = str(goods['cat_id_2'])
			goods_data['category3'] = str(goods['cat_id_3'])
			goods_data['goods_name'] = goods['goods_name']
			goods_data['market_price'] = float(goods['market_price'] / 100)  # 单位：元，下同
			goods_data['max_group_price'] = float(goods['max_on_sale_group_price'] / 100)
			goods_data['min_group_price'] = float(goods['min_on_sale_group_price'] / 100)
			goods_data['max_normal_price'] = float(goods['max_on_sale_normal_price'] / 100)
			goods_data['min_normal_price'] = float(goods['min_on_sale_normal_price'] / 100)
			goods_data['thumb_url'] = goods['thumb_url']
			goods_data['publish_date'] = goods['created_at']
			goods_data['total_sales'] = int(goods['sales'])  # 总销量
			goods_data['is_on_sale'] = goods['is_onsale']

			# ##获取核算价
			goods_data['price'] = goods_data['min_group_price']
			goods_data['total_amount'] = float(goods_data['total_sales'] * float(goods_data['price']))  # 总销售额

			yield goods_data
			

	"""从图片链接中获取产品的发布日期"""
	def get_goods_publish_date(self, images, content_images, skus_images):
		datetime 	=	[]

		for i in content_images:
			url 	=	i['url']
			if not url:
				continue
			date 	=	url.split('/')[4]
			
			try:
				#转换成时间数组
				timeArray = time.strptime(date, "%Y-%m-%d")
				#转换成时间戳
				timestamp = int(time.mktime(timeArray))
				datetime.append(timestamp)
			except Exception as e:
				continue

		for i in skus_images:
			url 	=	i['thumbUrl']
			if not url:
				continue
			date 	=	url.split('/')[4]
			
			try:
				#转换成时间数组
				timeArray = time.strptime(date, "%Y-%m-%d")
				#转换成时间戳
				timestamp = int(time.mktime(timeArray))
				datetime.append(timestamp)
			except Exception as e:
				continue
			
		if datetime:
			return min(datetime)
		else:
			##返回能获取到的最早的产品发布时间
			return 1489200000

	'''/**获取核算价 先按照销量最高的价格 若销量为0 则为价格最低的作为核算价**/'''	
	def get_goods_price(self, goods_skus, goods_sold_num):
		#print(goods_skus[0])
		if goods_sold_num:
			goods_skus.sort(key=lambda x:-x['soldQuantity'])
		else:
			goods_skus.sort(key=lambda x:x['groupPrice'])

		if goods_skus:
			return goods_skus[0]['groupPrice']
		else:
			return 0

	'''生成headers头信息'''
	def make_headers(self):
		chrome_version   = str(random.randint(59,63))+'.0.'+str(random.randint(1000,3200))+'.94'
		headers = {
			# "Host":"yangkeduo.com",
			# "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			# "Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
			# "Accept-Encoding":"gzip, deflate",
			# "Host":"yangkeduo.com",
			# "Referer":"http://yangkeduo.com",
			# "Connection":"keep-alive",
			# 'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/'+chrome_version+' Safari/537.36',
			#"Host":"mobile.yangkeduo.com",
			"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			"Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
			"Accept-Encoding":"gzip, deflate",
			"Host":"mobile.yangkeduo.com",
			"Referer":"Android",
			"Connection":"keep-alive",
			'User-Agent':'android Mozilla/5.0 (Linux; Android 6.1; 4G Build/MRA58K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/44.0.2403.119 Mobile Safari/537.36  phh_android_version/3.39.0 phh_android_build/228842 phh_android_channel/gw_pc',
		}
		
		ip = str(random.randint(100, 200))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))
		headers['CLIENT-IP'] 	=	ip
		headers['X-FORWARDED-FOR']=	ip
		return headers

	'''代理'''
	def get_proxy_ip(self):
		now_time = int(time.time())
		if now_time - self.proxy_start_time >= 60:
			self.proxy_ip_list = self.get_ssdb_proxy_ip()
			self.proxy_start_time = now_time

		if len(self.proxy_ip_list) <= 0:
			return ''

		ip = random.choice(self.proxy_ip_list)
		ip = ip.decode('utf-8')
		return 'http://' + ip
	'''从ssdb获取代理IP'''
	def get_ssdb_proxy_ip(self):
		res = self.ssdb_client.hkeys('proxy_ip_hash', '', '', 1000)
		if res:
			return res
		else:
			return []
	'''异常错误处理 删掉失败代理IP'''
	def errback_httpbin(self, failure):
		request = failure.request
		meta = request.meta
		proxy_ip = meta['proxy']
		proxy_ip = proxy_ip.replace("http://", "").encode("utf-8")

		if proxy_ip in self.proxy_ip_list:
			index = self.proxy_ip_list.index(proxy_ip)
			del self.proxy_ip_list[index]

		# 失败删除产品ID
		goods_id = meta['goods_id']
		self.ssdb_client.hdel(self.hash_name, goods_id)