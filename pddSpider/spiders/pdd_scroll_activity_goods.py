# -*- coding: utf-8 -*-
# 获取首页滚动活动下的产品信息
import scrapy
import json, time, sys, random, pyssdb
from spider.items import CategoryGoodsItem

class PddScrollActivityGoods(scrapy.Spider):
	name  = 'pdd_scroll_activity_goods'

	def start_requests(self):
		'''先从ssdb获取首页下的分类信息'''
		ssdb_class = pyssdb.Client('172.16.0.5', 8888)

		hash_name  = 'pdd_goods_activity_1' 
		category_data= ssdb_class.hgetall(hash_name)

		'''拉取滚动活动下的产品信息'''
		if type(category_data) == list:
			for data in category_data:
				data = json.loads( data.decode('utf-8') )
				
				if type(data) == dict: ##当获取到的不是subject_id 时
					subject_id 		= data['subject_id']
					activity_type	= data['activity_type']

					goods_lists = []
					page 		= 1

					url 	= self.get_activity_url(subject_id, activity_type, page)
					meta	= {'subject_id':subject_id,'page':page,'goods_lists':goods_lists, 'activity_type':activity_type}

					headers = self.make_headers()
					yield scrapy.Request(url, meta=meta, callback=self.parse, headers=headers)
				
	
	def parse(self, response):
		#print(response.meta)
		page 		= int(response.meta['page'])
		subject_id 	= response.meta['subject_id']
		goods_lists = response.meta['goods_lists']
		activity_type = response.meta['activity_type']

		data = json.loads(response.body.decode('utf-8'))
		#print(len(goods_lists))
		if data['goods_list']:

			i = 1
			for goods_data in data['goods_list']:
				goods_id = goods_data['goods_id']
				rank     = (page - 1) * 500 + i ##计算排名
				i += 1
				price 	 = float(goods_data['group']['price'] / 100)
				goods_lists.append({'goods_id':goods_id,'rank':rank,'subject_id':subject_id,'type':1, 'price':price})

			page += 1
			url = self.get_activity_url(subject_id,activity_type,page)
			meta= {'subject_id':subject_id,'page':page,'goods_lists':goods_lists, 'activity_type':activity_type}
			#print(url)
			headers = self.make_headers()

			yield scrapy.Request(url, meta=meta, callback=self.parse, headers=headers)
		else:
			item = CategoryGoodsItem()
			item['goods_lists'] = goods_lists
			#print(goods_lists)
			yield item

	'''生成headers头信息'''
	def make_headers(self):
		chrome_version   = str(random.randint(59,63))+'.0.'+str(random.randint(1000,3200))+'.94'
		headers = {
			# "Host":"yangkeduo.com",
			# "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			# "Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
			# "Accept-Encoding":"gzip, deflate",
			# "Host":"yangkeduo.com",
			# "Referer":"http://yangkeduo.com/goods.html?goods_id=442573047&from_subject_id=935&is_spike=0&refer_page_name=subject&refer_page_id=subject_1515726808272_1M143fWqjQ&refer_page_sn=10026",
			# "Connection":"keep-alive",
			# 'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/'+chrome_version+' Safari/537.36',
			"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			"Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
			"Accept-Encoding":"gzip, deflate",
			"Host":"yangkeduo.com",
			"Referer":"Android",
			"Connection":"keep-alive",
			'User-Agent':'android Mozilla/5.0 (Linux; Android 6.1; 4G Build/MRA58K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/44.0.2403.119 Mobile Safari/537.36  phh_android_version/3.39.0 phh_android_build/228842 phh_android_channel/gw_pc',
		}
		
		ip = str(random.randint(100, 200))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))
		headers['CLIENT-IP'] 	=	ip
		headers['X-FORWARDED-FOR']=	ip
		return headers

	'''获取产品列表地址'''
	def get_activity_url(self, subject_id, activity_type, page):
		if activity_type == 1:
			url = 'http://apiv3.yangkeduo.com/v2/subject/'+str(subject_id)+'/goods?image_mode=2&page='+str(page)+'&size=500&pdduid=0'
		elif activity_type == 2:
			url = 'http://apiv3.yangkeduo.com/subject/'+str(subject_id)+'/sorted_goods?image_mode=2&sort_type=PRIORITY&page='+str(page)+'&size=500&pdduid=0'

		return url
