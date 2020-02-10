# -*- coding: utf-8 -*-
import scrapy
import json, time, sys, random, re, pyssdb, urllib.request
from spider.items import CategoryItem

'''获取店铺信息及销量记录'''
class PddMallCategorySpider(scrapy.Spider):
	name 		= 'pdd_mall_category'
	url 		= 'https://mms.pinduoduo.com/vodka/v2/mms/categories'
	cookie 		= ''
	fail_nums   = 50

	# custom_settings = {
	# 	'LOG_FILE':'',
	# 	'LOG_LEVEL':'INFO',
	# 	'LOG_ENABLED':True
	# 	}

	def __init__(self):
		self.ssdb 	= pyssdb.Client('172.16.0.5', 8888)
		self.ssdb.hclear('pdd_keywords_hash')
		self.ssdb.hclear('pdd_keywords_extend_hash')
		self.ssdb.hclear('pdd_mall_category_hash')

		self.get_pdd_login_info()
		if not self.cookie:
			return False

	def start_requests(self):
		parentId 	=	0
		cat_list 	=	[]

		#meta 	= {'parentId':parentId, 'cat_list':cat_list}
		headers = self.make_headers()
		meta 	= {'level':0}
		
		yield scrapy.FormRequest(self.url+'?&parentId='+str(parentId),callback=self.parse, headers=headers)

	def parse(self, response):
		pass
		cat_list 	 = []
		
		categoryInfo = response.body.decode('utf-8') ##bytes转换为str
		categoryInfo = json.loads(categoryInfo) ##str转为字典
		
		if 'errorCode' in categoryInfo.keys() and categoryInfo['errorCode'] == 1000000:
			for cat in categoryInfo['result']:
				cat_id 	= cat['id']
				cat_name= cat['cat_name']
				parent_id=cat['parent_id']
				cat_level   = cat['level']
				info 	= {'cat_id':cat_id, 'cat_name':cat_name, 'level':cat_level, 'parent_id':parent_id}
				info['cat_id_1'] = cat['cat_id_1']
				info['cat_id_2'] = cat['cat_id_2']
				info['cat_id_3'] = cat['cat_id_3']
				info['cat_id_4'] = cat['cat_id_4']

				cat_list.append(info)
				if cat_level != 3:
					headers = self.make_headers()
					yield scrapy.FormRequest(self.url+'?&parentId='+str(cat_id),callback=self.parse, headers=headers)
			CatItem = CategoryItem()
			CatItem['cat_list'] = cat_list
			yield CatItem
			
		elif 'error_code' in categoryInfo.keys() and categoryInfo['error_code'] == 43001:
			self.get_pdd_login_info()

	'''生成headers头信息'''
	def make_headers(self):
		chrome_version   = str(random.randint(59,63))+'.0.'+str(random.randint(1000,3200))+'.94'
		headers = {
			"Host":"mms.pinduoduo.com",
			"Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
			"Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
			"Accept-Encoding":"gzip, deflate",
			"Referer":"http://mms.pinduoduo.com/Pdd.html",
			"Connection":"keep-alive",
			'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/'+chrome_version+' Safari/537.36',
			'Cookie':self.cookie,
		}
		
		ip = str(random.randint(100, 200))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))
		headers['CLIENT-IP'] 	=	ip
		headers['X-FORWARDED-FOR']=	ip
		return headers

	'''获取拼多多后台登录信息'''
	def get_pdd_login_info(self):
		if self.fail_nums <= 0:
			sys.exit(0)

		else:
			url = 'https://mms.pinduoduo.com/latitude/auth/login'

			headers = {
				'User-Agent':'Mozilla/5.0 (iPhone; CPU iPhone OS 11_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15F79 pddmt_ios_version/1.16.3 pddmt_ios_build/1806251428 ===  iOS/11.4 Model/iPhone9,1 BundleID/com.xunmeng.merchant AppVersion/1.16.3 AppBuild/1806251428',
				'Content-type':'application/json'
			}
			query_data = {"version":1,"username":"13638363484","password":"Jhc123456"}
			query_data = json.dumps(query_data).encode('utf-8')

			request = urllib.request.Request(url=url, data=query_data, headers=headers)
			login_data=urllib.request.urlopen(request)
			login_data = json.loads( login_data.read().decode('utf-8') )

			if 'PASS_ID' in login_data['result'].keys():
				self.cookie = 'PASS_ID='+login_data['result']['PASS_ID']+';'
			else:
				self.fail_nums -= 1
				self.get_pdd_login_info()
			# i 	= 1
			# while i <= 5 and not self.cookie:
			# 	file=urllib.request.urlopen('http://www.duoxiaoxia.com/api/pddmms/loginInfo')
			# 	data=file.read()
			# 	if data:
			# 		data = json.loads( data.decode('utf-8') )
			# 		if 'passId' in data.keys():
			# 			self.cookie += 'PASS_ID='+data['passId']+';'

			# 		if 'apiUid' in data.keys():
			# 			self.cookie += 'api_uid='+data['apiUid']+';'
			# 	else:
			# 		self.fail_nums -= 1
			# 	i+=1