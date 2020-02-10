# -*- coding: utf-8 -*-
import scrapy
import json, time, sys, random, re, pyssdb

from scrapy.utils.project import get_project_settings

from spider.items import GoodsSalesItem

goods_list = []
'''获取店铺内产品信息'''
class PddMallGoodsSpider(scrapy.Spider):
	name = 'pdd_mall_goods'
	mall_id_hash 	= 'pdd_mall_id_hash'
	hash_num 		= 0
	ssdb_client     = ''
	process_nums 	= 1
	limit			= 100

	def __init__(self, hash_num = 0, process_nums = 1):
		self.ssdb_client = pyssdb.Client(get_project_settings().get('SSDB_HOST'), 8888)
		self.hash_num = int(hash_num) ##当前脚本号
		self.process_nums   = int(process_nums) ##脚本总数
		self.pageSize = 500 ##每次抓取的产品数 最大只返回500

	def start_requests(self):
		mall_nums 		= 	self.limit * int(self.process_nums) ##一次查询的数量

		is_end 			=	False
		start_mall_id 	=	'' ##起始查询的店铺key
		while not is_end:
			mall_ids 	=	self.ssdb_client.hkeys(self.mall_id_hash, start_mall_id, '', mall_nums)
			
			if  not mall_ids: ##没有数据返回
				is_end 	=	True
				continue

			for mall_id in mall_ids:
				mall_id = int( mall_id.decode('utf-8') )
				start_mall_id = mall_id

				if mall_id % self.process_nums != self.hash_num:
					continue
					
				goods_list=[]
				page = 1

				headers = self.make_headers()
				url = 'http://apiv4.yangkeduo.com/api/turing/mall/query_cat_goods?category_id=0&type=0&sort_type=_sales&mall_id='+str(mall_id)+'&page_no='+str(page)+'&page_size=500'
				meta = {'page':page, 'mall_id':mall_id, 'goods_list':goods_list}
				yield scrapy.Request(url, meta=meta, callback=self.parse, headers=headers)
			
	def parse(self, response):
		pass
		goods_list=response.meta['goods_list'] ##产品集合
		mall_id  = response.meta['mall_id'] ##店铺ID
		page 	 = response.meta['page'] ##每返回一次页面数据 记录页数

		mall_goods = response.body.decode('utf-8') ##bytes转换为str
		mall_goods = json.loads(mall_goods)

		goods_len  = len(mall_goods['goods_list'])

		if goods_len > 0:
			goods_list = goods_list + mall_goods['goods_list'] ##合并产品列表

		if goods_len > self.pageSize - 100:
			page += 1
			##继续采集下一页面
			url = 'http://apiv4.yangkeduo.com/api/turing/mall/query_cat_goods?category_id=0&type=0&sort_type=_sales&mall_id='+str(mall_id)+'&page_no='+str(page)+'&page_size=500'
			meta = {'page':page, 'mall_id':mall_id, 'goods_list':goods_list}
			headers = self.make_headers()
			yield scrapy.Request(url, meta=meta, callback=self.parse, headers=headers)
		else:
			if goods_list:
				item = GoodsSalesItem()
				item['goods_list'] = goods_list
				item['mall_id']    = mall_id
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