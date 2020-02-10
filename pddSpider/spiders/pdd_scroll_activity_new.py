# -*- coding: utf-8 -*-
# 首页滚动栏活动信息
import scrapy
import json, time, sys, random, urllib, pyssdb, re
from spider.items import CategoryItem
from urllib import parse as urlparse
##import mq.mq as mq
##import ssdb.ssdbapi

class PddScrollActivityNewSpider(scrapy.Spider):
	name = 'pdd_scroll_activity_new'
	custom_settings = {
			'DOWNLOADER_MIDDLEWARES':{'spider.middlewares.ProxyMiddleware': 100},
			# 'LOG_FILE':'',
			# 'LOG_LEVEL':'DEBUG',
			# 'LOG_ENABLED':True,
			'DOWNLOAD_TIMEOUT':5,
			'RETRY_TIMES':10,
		}

	def start_requests(self):
		urls 	= [
					{'url':'http://apiv4.yangkeduo.com/api/fiora/bannerindex/query/platform?platform=1&pdduid=0', 'urlType':1},
					{'url':'http://apiv3.yangkeduo.com/api/fiora/bannerindex/query/platform?platform=1&version=2', 'urlType':1},
					{'url':'http://mobile.yangkeduo.com/', 'urlType':2},
					{'url':'http://apiv3.yangkeduo.com/api/fiora/home_promotion?start=0&end=1000&pdduid=0', 'urlType':3},
				  ]

		for info in urls:
			headers = self.make_headers()
			url 	= info['url']
			urlType = info['urlType']
			'''1为首页bannel轮播api， 2为拼多多首页 3为首页下产品列表中小活动分类'''
			function = self.parse if urlType   == 1 else self.homePageParse
			if urlType == 1:
				function = self.parse
			elif urlType == 2:
				function = self.homePageParse
			elif urlType == 3:
				function = self.homeSubjectParse

			yield scrapy.Request(url, callback=function, headers=headers)

	'''获取首页滚动banner活动信息'''
	def parse(self, response):
		result = json.loads(response.body.decode('utf-8'))
		if 'result' in result.keys():
			path 	=	['首页滚动banner']
			path_id =   []
			for data in result['result']:
				title 	= data['title']
				subject = self.get_subject_id(data['link_url'])
				if not subject:
					continue

				subject_id = subject['subject_id']
				headers = self.make_headers()
				if subject['has_child']: ##有子活动:
					pass
					new_url = 'http://apiv4.yangkeduo.com/subject_collection/'+str(subject_id)
					meta 	= {'path':path+[title],'path_id':[subject_id]}
					yield scrapy.Request(new_url, meta=meta, callback=self.parse_subject_children, headers=headers,dont_filter=True,errback=self.errback_httpbin) ##抓取下级分类
				else: ##拉取活动信息接口 获取活动信息
					url = 'http://apiv3.yangkeduo.com/subject/'+str(subject_id)
					meta 	= {'path':path, 'path_id':[]}
					yield scrapy.Request(url, meta=meta, callback=self.parse_subject_info, headers=headers,dont_filter=True,errback=self.errback_httpbin)
	
	'''处理拼多多首页类目及固定活动栏信息'''
	def homePageParse(self, response):
		# a = response.xpath('/html').re('window\.rawData= (.*)\;\s*\<\/script\>')
		# if not a:
		# 	return False
		# content = json.loads( a[0] )
		content = response.body.decode('utf-8')
		matches = re.search('\{"props":\{(.*)\"chunks\"\:\[\]\}', content)
		
		content = json.loads(matches.group(0))
		content = content['props']['data']['data']

		##首页前台类目##
		category= content['portalCommon']['operations']
		del category[0]
		for i in category:
			cat_list   = []
			first_name = i['tabName'] ##第一级名称
			first_id   = i['optID'] ##一级ID
			cat 	   = {'subject_id':first_id, 'name':first_name, 'type':2, 'path':first_name, 'path_id':first_id}
			cat_list.append(cat)

			for second in i['cat']:
				second_name = second['optName'] ##第二级名称
				second_id   = second['optID'] ##第二级ID
				cat 	= {'subject_id':second_id, 'name':second_name, 'type':2, 'path':first_name+'>'+second_name, 'path_id':str(first_id)+'>'+str(second_id)}
				cat_list.append(cat)

				url = 'http://apiv4.yangkeduo.com/operation/'+str(second_id)+'/groups?opt_type=2&size=1&offset=0&pdduid=0&is_back=1'
				cat_name = {'first_name':first_name, 'second_name':second_name}
				cat_id	 = {'first_id':first_id, 'second_id':second_id}
				
				headers = self.make_headers()
				yield scrapy.Request(url, meta = {'cat_name':cat_name,'cat_id':cat_id, 'cat_list':cat_list}, callback=self.get_third_category, headers=headers,dont_filter=True,errback=self.errback_httpbin)

		###前台固定活动列表###
		fix_category = content['quickEntrances']
		subject_list = []
		for category in fix_category:
			name    = category['title']
			subject = self.get_subject_id(category['link'])
			
			if not subject:
				continue
			subject_id = subject['subject_id']
			has_child  = subject['has_child']
			path    = [name]
			path_id = [subject_id]
			
			if has_child: ##有子活动:
				pass
				headers = self.make_headers()
				new_url = 'http://apiv4.yangkeduo.com/subject_collection/'+str(subject_id)
				meta 	= {'path':path,'path_id':path_id}
				yield scrapy.Request(new_url, meta=meta, callback=self.parse_subject_children, headers=headers,dont_filter=True,errback=self.errback_httpbin) ##抓取下级分类
			else: ##拉取活动信息接口 获取活动信息
				info = self.build_subject_info(subject_id, name, path, path_id, 1)
				subject_list.append(info)
				#print(subject_list)
		
		item  = CategoryItem()
		item['cat_list'] = subject_list
		#print(item)
		yield item

	'''获取首页产品列表内嵌的小活动信息'''
	def homeSubjectParse(self, response):
		content = json.loads(response.body.decode('utf-8'))
		if 'data' in content.keys():
			subject_list = []
			name 	   = '首页'
			subject_id = 0
			path 	   = [name]
			path_id    = [0]
			info = self.build_subject_info(subject_id, name, path, path_id, 1, 3)
			subject_list.append(info)
			
			for subject in content['data']:
				subject_mix_data = self.get_child_subject_info(subject) ##获取活动下级mix信息
				if not subject_mix_data:
					continue

				for v in subject_mix_data:
					child_name = v['name']
					child_subject_id = v['subject_id']
					if child_name:
						subject_info = self.build_subject_info(child_subject_id, child_name, path + [child_name], path_id + [child_subject_id])
						subject_list.append(subject_info)
					else:
						headers = self.make_headers()
						url = 'http://apiv3.yangkeduo.com/subject/'+str(child_subject_id)
						meta 	= {'path':path, 'path_id':path_id}
						yield scrapy.Request(url, meta=meta, callback=self.parse_subject_info, headers=headers,dont_filter=True,errback=self.errback_httpbin)
			item  = CategoryItem()
			item['cat_list'] = subject_list
			#print(item)
			yield item

		
	def parse_subject_children(self, response):
		subject_list = []
		result = json.loads(response.body.decode('utf-8'))
		lists  = result['list']
		path   = response.meta['path']
		path_id= response.meta['path_id']
		
		for data in lists:
			name     = data['subject']
			subject_id=data['subject_id']
			new_path = path + [name]
			new_path_id = path_id+[subject_id]

			info = self.build_subject_info(subject_id, name, new_path, new_path_id, 1, 2)
			subject_list.append(info)

			if data['mix']: ##有子级活动
				pass
				for mix in data['mix']:
					subject_mix_data = self.get_child_subject_info(mix) ##获取活动下级mix信息
					if not subject_mix_data:
						continue

					for v in subject_mix_data:
						child_name = v['name']
						child_subject_id = v['subject_id']
						if child_name:
							subject_info = self.build_subject_info(child_subject_id, child_name, new_path + [child_name], new_path_id + [child_subject_id])
							subject_list.append(subject_info)
						else:
							headers = self.make_headers()
							url = 'http://apiv3.yangkeduo.com/subject/'+str(child_subject_id)
							meta 	= {'path':new_path, 'path_id':new_path_id}
							yield scrapy.Request(url, meta=meta, callback=self.parse_subject_info, headers=headers,dont_filter=True,errback=self.errback_httpbin)

		item  = CategoryItem()
		item['cat_list'] = subject_list
		#print(item)
		yield item

	'''处理活动信息api'''
	def parse_subject_info(self, response):
		path 	= response.meta['path']
		path_id = response.meta['path_id']
		result  = json.loads(response.body.decode('utf-8'))
		subject_id = result['id']
		name 	= result['subject']
		new_path = path+[name]
		new_path_id = path_id + [subject_id]
		info 	= self.build_subject_info(subject_id, name, new_path, new_path_id, 1, 2)

		item  = CategoryItem()
		item['cat_list'] = [info]
		#print(item)
		yield item
	
	'''获取三级分类'''
	def get_third_category(self, response):
		data = json.loads(response.body.decode('utf-8'))
		if data['opt_infos']:
			item = CategoryItem()
			cat_name	=	response.meta['cat_name']
			cat_id 		=	response.meta['cat_id']
			cat_list 	= 	response.meta['cat_list']
			
			for info in data['opt_infos']:
				name= info['opt_name']
				subject_id  = info['id']

				path 	= cat_name['first_name']+'>'+cat_name['second_name']+'>'+name
				path_id	= str(cat_id['first_id'])+'>'+str(cat_id['second_id'])+'>'+str(subject_id)

				cat = {'subject_id':subject_id, 'name':name, 'type':2, 'path':path, 'path_id':path_id}
				cat_list.append(cat)
				#print(cat)
			item['cat_list'] = cat_list
			#print(item)
			yield item

	'''通过url获取subject_id'''
	def get_subject_id(self, link_url):
		##拆分出URL参数
		url_arr = urlparse.urlparse(link_url)
		url_arr = urlparse.parse_qs(url_arr.query)
		if url_arr:
			keys = url_arr.keys()
			if 'subject_id' in keys: ##单独活动
				subject_id = int(url_arr['subject_id'][0])
				has_child  = 0

			elif 'subjects_id' in keys: ##有子活动
				subject_id = int(url_arr['subjects_id'][0])
				has_child  = 1

			else:
				return False
		else:
			return False

		return {'subject_id':subject_id, 'has_child':has_child}

	'''生成活动信息'''
	def build_subject_info(self, subject_id, title, path, path_id, subjectType = 1, activity_type = 2):
		info = {'subject_id':subject_id,'name':title, 'path':path,'type':subjectType,'activity_type':activity_type,'path_id':path_id}
		return info

	##获取子subject信息
	def get_child_subject_info(self, mix):
		subject_list=   []
		mix_type 	=	mix['type']
		if mix_type == 99:
			pass
			for picture in mix['value']['picture_layers']:
				if picture['jump']:
					for url in picture['jump']:
						subject_id = self.get_subject_id(url)
						if subject_id:
							subject_id = subject_id['subject_id']
							name 	   = ''
							subject_list.append({'subject_id':subject_id, 'name':name})

		elif mix_type == 3:
			name 	=	mix['value']['subject']
			subject_id 	=	mix['value']['subject_id']
			subject_list.append({'subject_id':subject_id, 'name':name})

		elif mix_type == 1 or mix_type == 2:
			name 	= 	mix['value']['subject_name']
			subject_id= mix['value']['subject_id']
			subject_list.append({'subject_id':subject_id, 'name':name})			

		return subject_list

	'''生成headers头信息'''
	def make_headers(self):
		chrome_version   = str(random.randint(59,63))+'.0.'+str(random.randint(1000,3200))+'.94'
		headers = {
			"Host":"mobile.yangkeduo.com",
			"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			"Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
			"Accept-Encoding":"gzip, deflate",
			"Referer":"http://yangkeduo.com/goods.html?goods_id=442573047&from_subject_id=935&is_spike=0&refer_page_name=subject&refer_page_id=subject_1515726808272_1M143fWqjQ&refer_page_sn=10026",
			"Connection":"keep-alive",
			"Upgrade-Insecure-Requests":1,
			'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/'+chrome_version+' Safari/537.36',
			# "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			# "Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
			# "Accept-Encoding":"gzip, deflate",
			# "Host":"yangkeduo.com",
			# "Referer":"Android",
			# "Connection":"keep-alive",
			# 'User-Agent':'android Mozilla/5.0 (Linux; Android 6.1; 4G Build/MRA58K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/44.0.2403.119 Mobile Safari/537.36  phh_android_version/3.39.0 phh_android_build/228842 phh_android_channel/gw_pc',
		}
		
		ip = str(random.randint(100, 200))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))
		headers['CLIENT-IP'] 	=	ip
		headers['X-FORWARDED-FOR']=	ip
		return headers

	def errback_httpbin(self, failure):
		request = failure.request
		response = failure.value.response
		if response.status == 403:
			return
		#headers = self.make_headers()
		#meta = {'proxy':self.proxy}
		meta = request.meta
		yield scrapy.Request(request.url, meta=meta, callback=self.parse, headers=request.headers,dont_filter=True,errback=self.errback_httpbin)

