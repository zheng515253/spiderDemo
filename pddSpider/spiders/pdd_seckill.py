# -*- coding: utf-8 -*-
# 拼多多秒杀活动抓取
import scrapy
import json, time, sys, random, pyssdb, datetime
from spider.items import PddSeckillItem
from urllib import parse as urlparse

class PddSeckillSpider(scrapy.Spider):
	name = 'pdd_seckill'
	#allowed_domains = ['pdd']
	
	def __init__(self):
		self.goods_seckill_info = [	
									{'subject_id':-1,'path':'限时秒杀','name':'限时秒杀','type':1,'path_id':'-1'},
									{'subject_id':-2,'path':'限时秒杀','name':'限时秒杀','type':1,'path_id':'-1>-2'},
								  ]

	def start_requests(self):
		type_list 	=	[
							{'type':'today', 'name':'限时秒杀','subject_id':-2},
							#{'type':'tomorrow', 'name':'明日预告'},
							#{'type':'all_after', 'name':'更多预告'},
						]
		for data in type_list:
			name 		= data['name']
			data_type	= data['type']

			page 		= 1
			url 		= self.build_url(data_type, page)
			goods_list 	= []
			data['goods_list'] 	= goods_list ##秒杀产品详情
			data['goods_rank_list']=[] ##秒杀活动产品列表

			data['page'] 		= page
			yield scrapy.Request(url, meta={'data':data}, callback=self.parse, headers=self.make_headers())
			break

	def parse(self, response):
		meta_data		= response.meta['data']
		data_type 		= meta_data['type']
		subject_id 		= meta_data['subject_id']

		data = json.loads(response.body.decode('utf-8'))
		
		if data['items']:
			for item in data['items']:
				item_type 	= item['type']
				if item_type == 1:
					goods_data  = self.get_goods_data(item['data']) ##获取秒杀详情
					meta_data['goods_list'].append(goods_data)

					goods_rank_info = {'goods_id':item['data']['goods_id'],'rank':0,'subject_id':subject_id,'type':1} ##获取秒杀排名信息
					meta_data['goods_rank_list'].append(goods_rank_info)
				
				elif item_type == 2:
					pass

					brand_id 	= item['data']['id']
					start_time  = item['data']['start_time']
					end_time	= item['data']['end_time']
					
					meta 		= {}
					meta['goods_list']	=	[]
					meta['goods_rank_list']	=	[]
					meta['page']		= 1
					meta['brand_id']	= brand_id
					meta['subject_id'] 	= meta_data['subject_id']
					meta['start_time']	= start_time
					meta['end_time']	= int(end_time)
					url 	=	self.get_brand_url(brand_id, 1)
					
					yield scrapy.Request(url, meta={'data':meta}, callback=self.brand_parse, headers=self.make_headers())
				else:
					continue


			##继续采集下一页数据
			meta_data['page'] += 1

			url 	= self.build_url(data_type, meta_data['page'])
			yield scrapy.Request(url, meta={'data':meta_data}, callback=self.parse, headers=self.make_headers())

		else:
			pass
			item = PddSeckillItem()
			item['goods_seckill_info']	= self.goods_seckill_info
			item['goods_list']			= meta_data['goods_list']
			item['goods_rank_list']		= meta_data['goods_rank_list']
			yield item

		

	##获取秒杀下子brand信息
	def brand_parse(self, response):
		meta_data 	= response.meta['data']
		data = json.loads(response.body.decode('utf-8'))
		
		if data['result']:
			for goods_info in data['result']:
				goods_data 	= self.get_goods_data(goods_info)
				if meta_data['end_time']:
					goods_data['end_time'] = meta_data['end_time']

				meta_data['goods_list'].append(goods_data)

				goods_rank_info = {'goods_id':goods_info['goods_id'],'rank':0,'subject_id':meta_data['subject_id'],'type':1} ##获取秒杀排名信息
				meta_data['goods_rank_list'].append(goods_rank_info)

			meta_data['page'] += 1
			url 	= self.get_brand_url(meta_data['brand_id'], meta_data['page'])

			yield  scrapy.Request(url, meta={'data':meta_data}, callback=self.brand_parse, headers=self.make_headers())
		else:
			pass
			item = PddSeckillItem()
			item['goods_seckill_info']	= []
			item['goods_list']			= meta_data['goods_list']
			item['goods_rank_list']		= meta_data['goods_rank_list']
			yield item
			#if jump_url:
				#subject_id = self.get_subject_by_url(url) ##分离出url里的subject_id


	def build_url(self, data_type, page):
		return 'http://apiv3.yangkeduo.com/api/spike/v3/list/'+str(data_type)+'?type=1&page='+str(page)+'&size=500&pdduid=0&is_back=1'

	def get_brand_url(self, brand_id, page):
		return 'http://apiv3.yangkeduo.com/api/spike/v4/brand/goods?spike_brand_id='+str(brand_id)+'&page='+str(page)+'&size=500&pdduid=0'

	def get_goods_data(self, item):
		goods_data 	= {}
		goods_data['goods_id'] 		= item['goods_id']
		goods_data['all_quantity']	= int(item['all_quantity'])
		goods_data['sold_quantity']	= int(item['sold_quantity'])
		goods_data['start_time'] 	= item['start_time']

		if int(item['is_onsale']) == 1 and  goods_data['sold_quantity'] < goods_data['all_quantity']:
			goods_data['end_time']	= self.makeTomorrowTimestamp()
		else:
			goods_data['end_time']	= int(time.time())
			
		goods_data['price'] 		= float(item['price']/100)
		return goods_data

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

	def makeTomorrowTimestamp(self):
		date 	=	( datetime.date.today() + datetime.timedelta(days=1) ).strftime("%Y-%m-%d 00:00:00")
		date 	= 	time.strptime(date, "%Y-%m-%d %H:%M:%S")
		timestamp=   int(time.mktime(date))
		return timestamp