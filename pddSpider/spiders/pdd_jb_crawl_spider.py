# -*- coding: utf-8 -*-
import datetime

import scrapy
import json, time, sys, random, re, pyssdb, logging, urllib.request, urllib.parse, pddSign,hashlib,hmac,os
import base64
from Crypto.Cipher import PKCS1_v1_5 as Cipher_pkcs1_v1_5
from Crypto.Cipher import AES
from Crypto.PublicKey import RSA
from spider.items import CategoryItem, DuoDuoJinBaoItem
from scrapy.utils.project import get_project_settings
from urllib import error


class DuoDuoJinBaoSpider(scrapy.Spider):
	""" 多多进宝数据抓取"""
	name = 'pdd_jb_crawl_spider'
	key = 'eNgWuwpQ84MZVTQbmdtHWTEwYXFibzDNXPSMP+B5VsA='
	cookie = 'api_uid='
	fail_nums = 5
	username = '18682498600'
	password = 'Zwmyhj*660730'
	category_info = {}
	url_list = {"cat_url": "https://jinbao.pinduoduo.com/network/api/common/optIdList",  # 分类
				"url_1": "https://jinbao.pinduoduo.com/network/api/common/goodsList",  # 单品推广
				"url_2": "https://jinbao.pinduoduo.com/network/api/common/queryTopGoodsList",  # 热销榜单
				"url_3": "https://jinbao.pinduoduo.com/network/api/common/brand/goodsList"}  # 品牌好货

	custom_settings = {
		'DOWNLOADER_MIDDLEWARES': {'spider.middlewares.ProxyMiddleware': 101},
		'DOWNLOAD_TIMEOUT': 5,
		'DOWNLOAD_DELAY': 0.1,
		'LOG_ENABLED': True,
	}

	def __init__(self):
		self.ssdb_client = pyssdb.Client(get_project_settings().get('SSDB_HOST'), 8888)
		self.key = base64.b64decode(self.key)
		self.pdd_class = pddSign.pddSign()

	def start_requests(self):
		yield scrapy.Request(self.url_list["cat_url"], method="GET",
			meta={}, headers=self.make_headers(), dont_filter=True, callback=self.parse_category)
		items = DuoDuoJinBaoItem()
		cate_list = list()
		name_list = ['实时热销榜', '实时收益榜', '今日销量榜']
		if name_list:
			for sort_type in range(1, 4):
				cate_dict = dict()
				cate_dict["cat_2"] = sort_type
				cate_dict["cat_2_name"] = name_list[sort_type - 1]
				cate_dict['cat_1'] = 2
				cate_dict['cat_1_name'] = '热销榜'
				cate_list.append(cate_dict)
				yield scrapy.Request(self.url_list['url_2'], method="POST", dont_filter=True, meta={'sort_type': sort_type,  'cat_1_name': '热销榜', 'cat_1': 2, 'cate_list': cate_list,
					"cat_2": sort_type, 'cat_2_name': name_list[sort_type - 1]}, body=json.dumps({'sortType': sort_type, 'type': 1}), headers=self.make_headers(), callback=self.parse)

	def parse_category(self, response):
		result = json.loads(response.body.decode('utf-8'))
		# logging.debug(result)
		items = DuoDuoJinBaoItem()
		cate_list = list()
		if "result" in result.keys() and len(result['result']['optIdList']) > 0:
			for i in result['result']['optIdList']:
				cate_dict = dict()
				opt_id = i['optId']
				opt_name = i['optName']
				cate_dict['cat_1'], cate_dict['cat_1_name'], cate_dict['cat_2'], cate_dict['cat_2_name'] = 1, '单品推广', opt_id, opt_name
				cate_list.append(cate_dict)
				href = 'https://jinbao.pinduoduo.com/promotion/hot-promotion'
				for page_number in range(1, 3):
					yield scrapy.Request(url=self.url_list['url_1'],  method="POST", headers=self.make_headers(), dont_filter=True, body=json.dumps({'categoryId': opt_id,
									'crawlerInfo':  self.pdd_class.messagePackV2('0al', href),
									'isMallCps': 0, 'pageNumber': page_number, 'pageSize': 300, 'sortType': 6, 'withCoupon': 0}), meta={'cat_2_name': opt_name, "cat_2": opt_id, 'cat_1_name': '单品推广', 'cat_1': 1},
									callback=self.parse)
					yield scrapy.Request(url=self.url_list['url_3'],  method="POST", headers=self.make_headers(), dont_filter=True, callback=self.parse, body=json.dumps({'isMallCps': 0,
										'keyword': "", 'optId': opt_id, 'pageNumber': page_number, 'pageSize': 300, 'sortType': 6, 'withCoupon': 0}),
										meta={'cat_2_name': opt_name, "cat_2": opt_id,  'cat_1_name': '品牌好货', 'cat_1': 3})
			items["cate_list"] = cate_list
			yield items

	def parse(self, response):
		cat_1_name = response.meta['cat_1_name']
		cat_2_name = response.meta['cat_2_name']
		cat_1 = response.meta['cat_1']
		cat_2 = response.meta['cat_2']
		null = None
		items = DuoDuoJinBaoItem()
		if 'cate_list' in response.meta.keys():
			items['cate_list'] = response.meta['cate_list']
		goods_list = list()
		result = json.loads(response.body.decode('utf-8'))
		result_list = []
		if 'errorCode' in result and result['errorCode'] == 1000000:
			if "goodsList" in result['result'].keys():
				result_list = result["result"]['goodsList']
			elif 'list' in result['result'].keys():
				result_list = result["result"]['list']
			# logging.debug(json.dumps({'result_' + str(cat_1_name): result_list}))
			# self.save_jb_log(json.dumps({"result_count_" + str(cat_1_name) + '_' + str(cat_2): len(result_list)}))
			if len(result_list) > 0:
				for i in result_list:
					# logging.debug(json.dumps({'avgDesc': i['avgDesc'], 'avgLgst': i['avgDesc'], 'avgServ': i['avgDesc']}))
					goods_dict = dict()
					goods_dict['cat_1_name'] = cat_1_name
					goods_dict['avg_desc'] = self.division_number(i['avgDesc'], 100)
					goods_dict['avg_lgst'] = self.division_number(i['avgLgst'], 100)
					goods_dict['avg_serv'] = self.division_number(i['avgServ'], 100)
					goods_dict['cat_1'] = cat_1
					goods_dict['cat_2'] = cat_2
					goods_dict['cat_2_name'] = cat_2_name
					# goods_dict['category_id'] = i['categoryId']
					# goods_dict['category_name'] = i['categoryName']
					goods_dict['price'] = self.division_number(i['minGroupPrice'], cat_1)  # 价格
					goods_dict['coupon_discount'] = self.division_number(i['couponDiscount'], cat_1)  # 优惠券面值
					goods_dict['coupon_remain_quantity'] = i['couponRemainQuantity']  # 优惠券剩余数量
					goods_dict['coupon_start_time'] = i['couponStartTime']  # 开始时间
					goods_dict['coupon_end_time'] = i['couponEndTime']  # 结束时间
					goods_dict['coupon_total_quantity'] = i['couponTotalQuantity']  # 优惠券总量
					goods_dict['goods_id'] = i['goodsId']
					goods_dict['goods_name'] = i['goodsName']
					goods_dict['merchant_type'] = i['merchantType']  # 正品标志  1,6 非正品
					goods_dict['sales_tip'] = self.get_goods_sale(i['salesTip'])   # 销量
					goods_dict['plan_type_all'] = i['planTypeAll']  # 是否招商推广   0 不是
					goods_dict['promotion_rate'] = self.division_number(i['promotionRate'], cat_1)  # 比率
					goods_list.append(goods_dict)
					self.save_jb_log(json.dumps({'goods_dict_' + str(cat_1) + '_' + str(cat_2): goods_dict}))
				# logging.debug(json.dumps({"count": len(goods_list), 'goods_list_' + str(cat_1_name): goods_list}))
				# self.save_jb_log(json.dumps({"goods_count_" + str(cat_1_name) + '_' + str(cat_2): len(goods_list)}))
				items["goods_list"] = goods_list
				yield items
			else:
				self.save_jb_log(json.dumps({"fail_goods_" + str(cat_1_name) + '_' + str(cat_2): result}))
		else:
			self.save_jb_log(json.dumps({"fail_result_" + str(cat_1_name) + '_' + str(cat_2): result}))

	def make_headers(self):
		""" 生成headers信息"""
		headers = {
			'Content-Type': 'application/json; charset=UTF-8',
			'Host': 'jinbao.pinduoduo.com',
			'Origin': 'http://jinbao.pinduoduo.com',
			'Referer': 'http://jinbao.pinduoduo.com/',
			'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36',
			'Accept': 'application/json, text/javascript, */*; q=0.01',
			# 'cookie': 'api_uid=rBRp9VzBvTerwHEGUmenAg==; _nano_fp=Xpdynqgqn0PblpTqlT_ROSUdTH0oxVZQkVCUaQgG',
		}
		return headers

	def get_goods_sale(self, price):
		if type(price) == str:
			if price.__contains__('万'):
				price = re.search(r'\d.*\d|\d*', price).group()
				price = int(float(price) * 10000)
		return price

	def division_number(self, number, cat_1):
		if number:
			if cat_1 == 2:
				number = number / 100
			else:
				number = number/1000
		return number

	def save_jb_log(self, content):
		time_now = time.strftime('%Y-%m-%d')
		file_path = '/data/spider/logs/jb_log'
		if not os.path.exists(file_path):
			os.makedirs(file_path)
		file_name = file_path + '/' + time_now + ".log"
		with open(file_name, 'a+') as f:
			f.write(content + '\r\n')




