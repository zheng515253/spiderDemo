# -*- coding: utf-8 -*-
# 获取首页的产品分类信息
import scrapy
import json, time, sys, random, pyssdb, re
from spider.items import CategoryItem

class PddGoodsCategorySpider(scrapy.Spider):
	name = 'pdd_goods_category'

	# custom_settings = {
	# 		'LOG_FILE':'',
	# 		'LOG_LEVEL':'INFO',
	# 		'LOG_ENABLED':True,
	# 	}

	def start_requests(self):
		headers = self.make_headers()
		url = 'http://mobile.yangkeduo.com'
		yield scrapy.Request(url, callback=self.parse, headers=headers)

	'''获取二级分类'''
	def parse(self, response):
		pass
		#a = response.xpath('/html').re('window\.rawData= (.*)\;\s*\<\/script\>')
		#a = response.xpath('/html').re('__NEXT_DATA__(.*)\s*module=\{\}')
		content = response.body.decode('utf-8')
		matches = re.search('\{"props":\{(.*)\"chunks\"\:\[\]\}', content)
		
		category= json.loads(matches.group(0))
		
		category = category['props']['data']['data']['portalCommon']['operations']
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
				yield scrapy.Request(url, meta = {'cat_name':cat_name,'cat_id':cat_id, 'cat_list':cat_list}, callback=self.get_third_category, headers=headers)

	
	'''获取三级分类'''
	def get_third_category(self, response):
		data = json.loads(response.body.decode('utf-8'))
		if data['opt_infos']:
			item = CategoryItem()
			cat_name	=	response.meta['cat_name']
			cat_id 		=	response.meta['cat_id']
			cat_list 	= 	response.meta['cat_list']
			#first_name = response.meta['first_name']
			#second_name = response.meta['second_name']
			
			for info in data['opt_infos']:
				name= info['opt_name']
				subject_id  = info['id']

				path 	= cat_name['first_name']+'>'+cat_name['second_name']+'>'+name
				path_id	= str(cat_id['first_id'])+'>'+str(cat_id['second_id'])+'>'+str(subject_id)

				cat = {'subject_id':subject_id, 'name':name, 'type':2, 'path':path, 'path_id':path_id}
				cat_list.append(cat)
				#print(cat)
			item['cat_list'] = cat_list
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
