# -*- coding: utf-8 -*-
# 拼多多固定活动栏分类
import scrapy
import json, time, sys, random, urllib
from spider.items import CategoryItem
import mq.mq as mq
import ssdb.ssdbapi

class PddGoodsFixActivitySpider(scrapy.Spider):
	name = 'pdd_goods_fix_activity'
	#allowed_domains = ['pdd']

	def start_requests(self):
		subject_list = [
							#{'id':14, 'tab_id':11, 'activity_type':3}, #品牌清仓
							{'id':12, 'tab_id':2, 'activity_type':3}, #9.9特卖
							#{'id':17, 'tab_id':0, 'activity_type':2}, #食品超市
							#{'id':22, 'tab_id':0, 'activity_type':2}, #时尚穿搭
							{'id':11, 'tab_id':0, 'activity_type':2}, #品质水果
						]
		for subject in subject_list:
			activity_type = subject['activity_type']
			subject_id    = subject['id']
			tab_id        = subject['tab_id']
			meta      = subject

			search_id = tab_id if tab_id else subject_id ##有tab_id则用tab_id拼接url 无则为subject_id

			if activity_type == 3:
				url = 'http://apiv4.yangkeduo.com/api/gentian/'+str(search_id)+'/resource_tabs?pdduid=0'
				meta['parent_tab_id'] = search_id
				callback_fun = self.parse_activity_3
			elif activity_type == 2:
				url = 'http://apiv4.yangkeduo.com/subject_collection/'+str(search_id)+'?pdduid=0'
				callback_fun = self.parse_activity_2
				print(url)

			headers = self.make_headers()
			#yield scrapy.Request(url, meta=meta, callback=callback_fun, headers=headers)

	def parse_activity_3(self, response):
		pass
		# result = json.loads(response.body.decode('utf-8'))
		# parent_name = result['name']
		# parent_tab_id = response.meta['tab_id']
		# cat_list  = []

		# for data in result['list']:
		# 	tab_id = data['tab_id']
		# 	name   = data['subject']
		# 	path   = [parent_name, name]
		# 	info = {'subject_id':tab_id, 'parent_tab_id':parent_tab_id, 'name':name, 'path':path, 'type':1, 'activity_type':3}
		# 	cat_list.append(info)

		# item = CategoryItem()
		# item['cat_list'] 	=	cat_list
		# yield item

	def parse_activity_2(self, response):
		pass
		print(response.body)

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
