# -*- coding: utf-8 -*-
# 拼多多固定活动栏分类
import scrapy
import json, time, sys, random, pyssdb
from spider.items import CategoryItem
from urllib import parse as urlparse

class PddFixActivitySpider(scrapy.Spider):
	name = 'pdd_fix_activity'
	#allowed_domains = ['pdd']

	def start_requests(self):
		subject_list = [
							{'subject_id':14, 'activity_type':2, 'name':'品牌清仓'}, #品牌清仓
							{'subject_id':12, 'activity_type':2, 'name':'9.9特卖'}, #9.9特卖
							{'subject_id':22, 'activity_type':2, 'name':'时尚穿搭'}, #时尚穿搭
							{'subject_id':15, 'activity_type':2, 'name':'爱逛街'}, #爱逛街
							{'subject_id':21, 'activity_type':2, 'name':'名品折扣'}, #名品折扣
							{'subject_id':17, 'activity_type':2, 'name':'食品超市'}, 
							{'subject_id':2, 'activity_type':2, 'name':'海淘'}, 
							{'subject_id':3273, 'activity_type':1, 'name':'1分拼'}, #1分拼
						]
		for subject in subject_list:
			activity_type = subject['activity_type']
			subject_id    = subject['subject_id']

			url = 'http://apiv4.yangkeduo.com/subject_collection/'+str(subject_id)+'?pdduid=0'
			headers = self.make_headers()

			yield scrapy.Request(url, meta=subject, callback=self.parse, headers=headers)
			break

	def parse(self, response):
		meta = response.meta
		sub_list = []
		activity_type		= meta['activity_type']

		data = json.loads(response.body.decode('utf-8'))
		if data['list']:
			for subject_info in data['list']:
				name 		= subject_info['subject']
				subject_id 	= subject_info['subject_id']
				path 		= [meta['name'], name]
				path_id 	= [meta['subject_id'], subject_id]

				info  		= {'subject_id':subject_id,'path':path,'name':name,'type':1,'activity_type':activity_type,'path_id':path_id}
				sub_list.append(info)

				if subject_info['mix']: ##有下级subject
					for child_sub in subject_info['mix']:
						sub_info 	= self.get_child_subject_info(path, path_id, child_sub, activity_type) ##获取子subject信息

						if sub_info:
							for sub in sub_info:
								new_path_id 	= path_id + [int(sub['subject_id'])]
								sub['path_id'] 	= new_path_id
								sub['type']		= 1
								sub['activity_type'] = activity_type

								if sub['name']: ##有subject_name
									if 'banner' in sub:
										new_path 	= path + [sub['banner']]
										del sub['banner']
									else:
										new_path	= path + [sub['name']]

									sub['path']		= new_path
									
									sub_list.append(sub)
								else: ##没有name则需要接口拉取subject信息获取name
									sub['path']		= path
									url 	=	'http://apiv3.yangkeduo.com/subject/'+str(sub['subject_id'])
									yield scrapy.Request(url, meta={'sub_info':sub}, callback=self.curl_sub_info, headers=self.make_headers())

				
			item = CategoryItem()
			item['cat_list'] 	=	sub_list
			yield item

		

	##获取子subject信息
	def get_child_subject_info(self, path, path_id, child_sub, activity_type):
		subject_list 	=	[]
		subject_type 	=   child_sub['type']
		#print(child_sub)
		if subject_type == 99:
			
			picture_list 	= child_sub['value']['picture_layers']
			for picture_info in picture_list:
				jump_url 	=	picture_info['jump']
				if jump_url:

					for url in jump_url:
						if not url:
							continue
						subject_id 	= self.get_subject_by_url(url) ##分离出url里的subject_id
						if subject_id:
							info 		= {'subject_id':subject_id, 'name':''}
							subject_list.append(info)

		elif subject_type == 2 or subject_type == 1:
			info 	= child_sub['value']
			info['name'] 	= info['subject_name']
			del info['subject_name']
			subject_list.append(info)

		elif subject_type == 3:
			subject_id 	=	child_sub['value']['subject_id']
			name 		=	child_sub['value']['subject']
			info 		=	{'subject_id':subject_id, 'name':name}
			subject_list.append(info)
		
		return subject_list
			#if jump_url:
				#subject_id = self.get_subject_by_url(url) ##分离出url里的subject_id

	'''获取subject相关信息'''
	def curl_sub_info(self, response):
		sub_info 	=	response.meta['sub_info']
		message 	=	json.loads(response.body.decode('utf-8'))

		subject_name=	message['subject']
		sub_info['path'] 	= 	sub_info['path'] + [subject_name]
		sub_info['name'] 	=	subject_name

		item  = CategoryItem()
		item['cat_list'] = [sub_info]
		yield item


	def get_subject_by_url(self, url):

		##拆分出URL参数
		url_arr = urlparse.urlparse(url)
		url_query = url_arr.query
		url_query = urlparse.parse_qs(url_query)

		if 'subject_id' in url_query:
			return url_query['subject_id'][0]
		else:
			return False


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
