# -*- coding: utf-8 -*-
# 获取拼多多活动下的产品列表
import scrapy
import json, time, sys, random, pyssdb, re
from spider.items import CategoryGoodsItem
from urllib import parse as urlparse

class Pdd618Goods(scrapy.Spider):
	name  = 'pdd_618_goods'

	def start_requests(self):
		'''先从ssdb获取首页下的分类信息'''
		ssdb_class = pyssdb.Client('172.16.0.5', 8888)

		hash_names = ['pdd_goods_activity_2']
		for hash_name in hash_names:
			subject_data = ssdb_class.hgetall(hash_name)
			if subject_data:
				for subject_info in subject_data:
					subject_info = json.loads(subject_info.decode('utf-8'))

					if type(subject_info) == dict:
						page= 1
						headers = self.make_headers()
						
						meta = {'subject_info':subject_info, 'page':page, 'goods_list':[]}

						'''拉取活动下产品列表api'''
						page_size = 100 if subject_info['type'] == 1 and subject_info['subject_id'] == 0 else 500
						meta['page_size'] = page_size
						url = self.get_activity_url(subject_info, page, page_size)
						#print(url)
						yield scrapy.Request(url, meta=meta, callback=self.parse, headers=headers)

	def parse(self, response):
		subject_info = response.meta['subject_info']
		page 		 = int(response.meta['page'])
		goods_list   = response.meta['goods_list']
		page_size    = int(response.meta['page_size'])
		rank    = (page - 1) * page_size + 1
		
		content    = json.loads(response.body.decode('utf-8'))

		if 'goods_list' in content.keys() and len(content['goods_list'] ) > 0:
			for goods in content['goods_list']:
				if 'icon' in goods.keys():
					if goods['icon']['url'] == 'http://t00img.yangkeduo.com/t02img/images/2018-05-31/b0a6bc8699c92bf40d9d6d46b63dd49f.png':
						price = float(goods['group']['price']/100)
						goods_id = goods['goods_id']
						rank  = int(goods['cnt'])
						goods_info = self.build_goods_rank_info(goods_id, -618, 1, rank, price)
						#rank += 1
						goods_list.append(goods_info)

			page += 1
			url = self.get_activity_url(subject_info, page, page_size)
			meta= {'subject_info':subject_info, 'page':page, 'page_size':page_size, 'goods_list':goods_list}
			headers = self.make_headers()
			yield scrapy.Request(url, meta=meta, callback=self.parse, headers=headers)
					
		else:
			if goods_list:
				item = CategoryGoodsItem()
				item['goods_lists'] = goods_list
				#print(goods_list)
				yield item
			

	'''获取活动基本信息里的产品'''
	def parse_subject_info(self, response):
		subject_info = response.meta['subject_info']
		subject_id   = subject_info['subject_id']
		subject_type = subject_info['type']

		data = json.loads(response.body.decode('utf-8'))
		if 'mix' in data.keys():
			rank = 1
			goods_list = []
			for info in data['mix']:
				if info['type'] == 99:
					for goods_info in info['value']['picture_layers']:
						if goods_info['jump']:
							for goods_link in goods_info['jump']:
								goods_id = self.get_goods_id_by_url(goods_link)
								if goods_id and goods_id.isdigit():
									goods_info = self.build_goods_rank_info(goods_id, subject_id, subject_type, rank, 0)
									rank += 1
									url = 'http://mobile.yangkeduo.com/goods.html?goods_id='+str(goods_id)
									headers = self.make_headers()
									yield scrapy.Request(url, meta={'goods_info':goods_info}, callback=self.parse_goods_info, headers=headers)
	'''获取产品信息'''
	def parse_goods_info(self, response):
		goods_info = response.meta['goods_info']
		content= response.body.decode('utf-8')
		a = re.search('window\.rawData= (.*)\;\s*\<\/script\>', content)
		if a:
			content = json.loads( a.group(1) )
			if 'goods' not in content.keys():
				return False
			goods = content['goods']
			goods_info['price'] = goods['minOnSaleGroupPrice']

		item = CategoryGoodsItem()
		item['goods_lists'] = [goods_info]
		#print(len(goods_lists), cat_id)
		yield item

	'''生成headers头信息'''
	def make_headers(self):
		chrome_version   = str(random.randint(59,63))+'.0.'+str(random.randint(1000,3200))+'.94'
		headers = {
			"Host":"yangkeduo.com",
			"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			"Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
			"Accept-Encoding":"gzip, deflate",
			"Host":"yangkeduo.com",
			"Referer":"http://yangkeduo.com/goods.html?goods_id=442573047&from_subject_id=935&is_spike=0&refer_page_name=subject&refer_page_id=subject_1515726808272_1M143fWqjQ&refer_page_sn=10026",
			"Connection":"keep-alive",
			'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/'+chrome_version+' Safari/537.36',
		}
		
		ip = str(random.randint(100, 200))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))
		headers['CLIENT-IP'] 	=	ip
		headers['X-FORWARDED-FOR']=	ip
		return headers

	'''生成拼多多产品排名数据格式'''
	def build_goods_rank_info(self, goods_id, subject_id, activity_type, rank, price):
		return {'goods_id':goods_id, 'subject_id':subject_id, 'type':activity_type, 'rank':rank, 'price':price}

	'''获取产品列表地址'''
	def get_activity_url(self, subject_info, page, page_size=500):
		subject_id = subject_info['subject_id']
		subject_type=subject_info['type']
		activity_type=subject_info['activity_type'] if subject_type == 1 else 0

		if subject_type == 1:
			if subject_id == 0: ##首页
				url = 'http://apiv3.yangkeduo.com/v5/goods?page='+str(page)+'&size='+str(page_size)
			else:
				url = 'http://apiv3.yangkeduo.com/api/fiora/subject/goods?subject_id='+str(subject_id)+'&size='+str(page_size)+'&page='+str(page)+'&platform=1&pdduid=13333333333'
		elif subject_type == 2:
			offset = (int(page) - 1) * page_size
			url = 'http://apiv4.yangkeduo.com/operation/'+str(subject_id)+'/groups?opt_type=2&offset='+str(offset)+'&size='+str(page_size)+'&sort_type=DEFAULT&flip=109;9&pdduid=13333333333'

		return url

		# if activity_type == 1:
		# 	url = 'http://apiv3.yangkeduo.com/v2/subject/'+str(subject_id)+'/goods?image_mode=2&page='+str(page)+'&size=500&pdduid=0'
		# elif activity_type == 2:
		# 	url = 'http://apiv3.yangkeduo.com/subject/'+str(subject_id)+'/sorted_goods?image_mode=2&sort_type=PRIORITY&page='+str(page)+'&size=500&pdduid=0'
		
	'''获取链接里面的goods_id'''
	def get_goods_id_by_url(self, goods_url):
		##拆分出URL参数
		url_arr = urlparse.urlparse(goods_url)
		url_arr = urlparse.parse_qs(url_arr.query)
		if url_arr:
			keys = url_arr.keys()
			if 'goods_id' in keys: ##单独活动
				return url_arr['goods_id'][0]
			else:
				return False
		else:
			return False
