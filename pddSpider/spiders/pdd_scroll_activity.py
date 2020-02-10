# -*- coding: utf-8 -*-
# 首页滚动栏活动信息
import scrapy
import json, time, sys, random, urllib, pyssdb
from spider.items import CategoryItem
from urllib import parse as urlparse
##import mq.mq as mq
##import ssdb.ssdbapi

class PddScrollActivitySpider(scrapy.Spider):
	name = 'pdd_scroll_activity'
	activity_list = []

	def start_requests(self):
		headers = self.make_headers()
			  
		urls = ['http://apiv4.yangkeduo.com/api/fiora/bannerindex/query/platform?platform=1&pdduid=0',
				'http://apiv3.yangkeduo.com/api/fiora/bannerindex/query/platform?platform=1&version=2&pdduid=0&is_back=1',
				]
		for url in urls:
			yield scrapy.Request(url, callback=self.parse, headers=headers)

	'''获取二级分类'''
	def parse(self, response):
		pass
		self.activity_list.clear()
		item = CategoryItem()

		result = json.loads(response.body.decode('utf-8'))
		if result['result']:
			for data in result['result']:
				img_url = data['img_url']
				title   = data['title']
				url = data['link_url']

				##拆分出URL参数
				url_arr = urlparse.urlparse(url)
				url_query = url_arr.query
				url_query = urlparse.parse_qs(url_query)
				
				path = ['首页banner轮播', title] ##活动图片
				
				query_keys = url_query.keys()
				if 'subjects_id' in query_keys: ##子页面有下级分类
					subject_id = url_query['subjects_id'][0]
					if int(subject_id) in [12,14]: ##9.9特卖和品牌清仓跳过
						continue

					new_url = 'http://apiv4.yangkeduo.com/subject_collection/'+str(subject_id)
					headers = self.make_headers()
					meta = {'path':path}
					yield scrapy.Request(new_url, meta=meta, callback=self.parse_subjects, headers=headers) ##抓取下级分类
					
				elif 'subject_id' in query_keys: ##子页面无分类
					subject_id = url_query['subject_id'][0]
					info = {'path':path, 'subject_id':subject_id, 'name':title, 'type':1,'activity_type':1,'path_id':[]}
					self.activity_list.append(info)
				else: ##无法抓取 跳过
					continue
			
			item['cat_list'] = self.activity_list
			yield item
	
	def parse_subjects(self, response):
		data_list = []
		result = json.loads(response.body.decode('utf-8'))
		lists = result['list']
		path = response.meta['path']
		
		for data in lists:
			name     = data['subject']
			new_path = [name]
			new_path = path+new_path

			##path.append(data['subject'])
			info = {'subject_id':data['subject_id'],'path':new_path,'name':name,'type':1,'activity_type':2,'path_id':[]}
			data_list.append(info)

		item  = CategoryItem()
		item['cat_list'] = data_list
		# print(item)
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

