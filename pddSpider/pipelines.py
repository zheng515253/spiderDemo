# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os, sys, json, time, pyssdb, random,datetime
from sys import stdin, stdout
#import mq.mq as mq
#import ssdb.ssdbapi

class SpiderPipeline(object):
	ssdb_host = ''
	ssdb_client = ''
	def __init__(self, ssdb_host):
		self.ssdb_host = ssdb_host
		self.today = datetime.date.today()
		self.tomorrow = self.today + datetime.timedelta(days=1)

	@classmethod
	def from_crawler(cls, crawler):
		return cls(
			ssdb_host=crawler.settings.get('SSDB_HOST'),
		)

	def open_spider(self, spider):
		self.ssdb_client = pyssdb.Client(self.ssdb_host, 8888)

	def close_spider(self, spider):
		pass

	'''爬虫数据处理入口文件'''
	def process_item(self, item, spider):
		pass
		#print(spider.name)
		if spider.name == 'pdd_goods' or spider.name == 'pdd_goods_new': ##拼多多产品信息处理
			self.process_pdd_goods(item)

		elif spider.name == 'pdd_mall' or spider.name == 'pdd_mall_new' or spider.name == 'pdd_mall_v3' or spider.name == 'pdd_mall_v4' or spider.name == 'pdd_mall_v5' or spider.name == 'pdd_mall_v6' or spider.name == 'pdd_mall_v7' or spider.name == 'pdd_mall_v8' or spider.name == 'pdd_mall_v8_t' or spider.name == 'pdd_mall_v9' or spider.name == 'pdd_mall_v10' or spider.name == 'pdd_mall_v11' or spider.name == 'pdd_mall_v12' or spider.name == 'pdd_mall_v13' or spider.name == 'pdd_mall_v14' or spider.name == 'pdd_mall_v15' or spider.name == 'pdd_mall_v16' or spider.name == 'pdd_mall_v17' or spider.name == 'pdd_mall_v18' or spider.name == 'pdd_sync_mall' or spider.name == 'pdd_mall_v19': ##拼多多店铺信息及店铺销量增量处理
			self.process_pdd_mall(item)

		elif spider.name == 'pdd_goods_sales' or spider.name == 'pdd_goods_sales_new' or spider.name=='pdd_goods_sales_test' or spider.name == 'pdd_goods_sales_fail' or spider.name == 'pdd_goods_sales_v3' or spider.name == 'pdd_goods_sales_v4' or spider.name == 'pdd_goods_sales_v5' or spider.name == 'pdd_goods_sales_v6' or spider.name == 'pdd_goods_sales_v7' or spider.name == 'pdd_goods_sales_v8' or spider.name == 'pdd_goods_sales_v9' or spider.name == 'pdd_goods_sales_v10' or spider.name == 'pdd_goods_sales_v11' : ##拼多多产品销量增量处理
			self.process_pdd_goods_sale(item)

		elif spider.name == 'pdd_goods_category': ##获取首页的产品分类信息
			self.process_pdd_goods_category(item)

		elif spider.name == 'pdd_category_goods': ##获取首页分类下的产品信息
			self.process_pdd_category_goods(item)

		elif spider.name == 'pdd_scroll_activity': ##获取首页滚动广告栏类目
			self.process_pdd_scroll_activity(item)

		elif spider.name == 'pdd_scroll_activity_goods': ##首页滚动广告栏产品列表
			self.process_pdd_category_goods(item)

		elif spider.name == 'pdd_fix_activity': ##首页固定活动栏分类
			self.process_pdd_scroll_activity(item)

		elif spider.name == 'pdd_seckill': ##秒杀活动
			self.process_pdd_seckill(item)

		elif spider.name == 'pdd_sync_goods': ##同步产品信息
			self.process_pdd_goods(item)

		elif spider.name == 'pdd_mall_check':  # 仅同步店铺信息
			self.process_pdd_mall_check(item)

		elif spider.name == 'pdd_mall_category': ##店铺后台分类
			self.process_pdd_mall_category(item)

		elif spider.name == 'pdd_mall_keywords' or spider.name == 'pdd_mall_keywords_v2': ##关键词信息
			self.process_pdd_mall_keywords(item)

		elif spider.name == 'pdd_keywords_extend': ##关键词扩展信息
			self.process_pdd_keywords_extend(item)

		elif spider.name == 'pdd_scroll_activity_new': ##活动及类目抓取新脚本
			self.process_pdd_scroll_activity_new(item)

		elif spider.name == 'pdd_activity_goods' or spider.name == 'pdd_618_goods': ##活动下产品列表
			self.process_pdd_activity_goods(item)

		elif spider.name == 'pdd_goods_sales_check' or spider.name == 'pdd_goods_sales_check_v2' or spider.name == 'pdd_goods_sales_check_v3' or spider.name == 'pdd_goods_sales_check_v4' : ##产品销量增量复查
			self.process_pdd_goods_sales_check(item)

		elif spider.name == 'pdd_category_goods_sales': ##前台类目产品销量
			self.process_pdd_category_goods_sales(item)

		elif spider.name == 'pdd_category_goods_sales_push': ##推送前台类目有销量的产品
			self.process_pdd_category_goods_sales_push(item)

		elif spider.name == 'pdd_goods_price': ##产品价格变化
			self.process_pdd_goods_price(item)

		elif spider.name == 'pdd_goods_reviews': ##产品留言
			self.process_pdd_goods_reviews(item)

		elif spider.name == 'pdd_goods_sku' or spider.name == 'pdd_goods_sku_v2' or spider.name == 'pdd_goods_sku_v3' or spider.name == 'pdd_goods_sku_v4' or spider.name == 'pdd_goods_sku_v5' or spider.name == 'pdd_goods_sku_v6' or spider.name == 'pdd_goods_sku_v7'  : ##产品SKU
			self.process_pdd_goods_sku(item)
		elif spider.name == 'pdd_keyword_goods_rank':  # 热搜词排名
			self.process_pdd_keyword_goods_rank(item)

		elif spider.name == 'pdd_category_goods_v3' or spider.name == 'pdd_category_goods_v4' or spider.name == 'pdd_category_goods_v5' or spider.name == 'pdd_category_goods_v6' or spider.name == 'pdd_category_goods_v7' or spider.name == 'pdd_category_goods_v8' or spider.name == 'pdd_category_goods_v9' or spider.name == 'pdd_category_goods_v10' or spider.name == 'pdd_category_goods_v11' or spider.name == 'pdd_category_goods_v12' : ##分类销量排名并行
			self.process_pdd_category_goods_sales(item)
			goods_lists = []
			for goods_data in item['goods_lists']:
				goods_lists.append({'goods_id': goods_data['goods_id'], 'rank': goods_data['rank'], 'subject_id': goods_data['subject_id'], 'type': 2, 'price': float(goods_data['group']['price'] / 100)})
			item['goods_lists'] = goods_lists
			self.process_pdd_category_goods(item)

		elif spider.name == 'pdd_keyword_goods_rank_v2' or spider.name == 'pdd_keyword_goods' or spider.name == 'pdd_keyword_goods_retry' or spider.name == 'pdd_keyword_goods_rank_v3' or spider.name == 'pdd_keyword_goods_rank_v3_retry' or spider.name == 'pdd_keyword_goods_v2' or spider.name == 'pdd_keyword_goods_retry_v2' or spider.name == 'pdd_keyword_goods_rank_v4' or spider.name == 'pdd_keyword_goods_rank_v4_retry' or spider.name == 'pdd_keyword_goods_v5_rank_retry' or spider.name == 'pdd_keyword_goods_v5' or spider.name == 'pdd_keyword_goods_v5_rank' or spider.name == 'pdd_keyword_goods_v5_retry'  or spider.name == 'pdd_keyword_goods_v6_rank_retry' or spider.name == 'pdd_keyword_goods_v6' or spider.name == 'pdd_keyword_goods_v6_rank' or spider.name == 'pdd_keyword_goods_v6_retry' : ##分类销量排名并行
			if item['page'] <= 5:
				self.process_pdd_keyword_goods_sales(item)
				# self.process_pdd_keyword_goods_total_sales(item)
			goods_lists = []
			for goods_data in item['goods_list']:
				goods_lists.append({
					'keyword': goods_data['keyword'],
					'sort': goods_data['sort'],
					'goods_id': goods_data['goods_id'],
					'p_time': goods_data['p_time'],
					'mall_id': goods_data['mall_id'],
					'is_ad': goods_data['is_ad'],
					'suggest_keyword': goods_data['suggest_keyword']
				})
			item['goods_list'] = goods_lists
			self.process_pdd_keyword_goods_rank(item)

		elif spider.name == 'pdd_keyword_goods_v7_rank_retry' or spider.name == 'pdd_keyword_goods_v7' or spider.name == 'pdd_keyword_goods_v7_rank' or spider.name == 'pdd_keyword_goods_v7_retry' or spider.name == 'pdd_keyword_goods_v8_rank_retry' or spider.name == 'pdd_keyword_goods_v8' or spider.name == 'pdd_keyword_goods_v8_rank' or spider.name == 'pdd_keyword_goods_v8_retry' : ##分类销量排名并行
			if item['page'] <= 5:
				self.process_pdd_keyword_goods_sales(item)
				# self.process_pdd_keyword_goods_total_sales(item)
			goods_lists = []
			for goods_data in item['goods_list']:
				goods_lists.append({
					'keyword': goods_data['keyword'],
					'sort': goods_data['sort'],
					'goods_id': goods_data['goods_id'],
					'p_time': goods_data['p_time'],
					'mall_id': goods_data['mall_id'],
					'is_ad': goods_data['is_ad'],
					'suggest_keyword': goods_data['suggest_keyword']
				})
			item['goods_list'] = goods_lists
			self.process_pdd_keyword_goods_rank_v2(item)

		elif spider.name == 'pdd_promotion_keywords' or spider.name == 'pdd_promotion_keywords_v2' or spider.name == 'pdd_promotion_keywords_v3' or spider.name == 'pdd_promotion_keywords_v4' or spider.name == 'pdd_promotion_keywords_v5' or spider.name == 'pdd_promotion_keywords_v6' or spider.name == 'pdd_promotion_keywords_v7' or spider.name == 'pdd_promotion_keywords_v8' : # 推广热搜词
			self.process_pdd_promotion_keywords(item)

		elif spider.name == 'pdd_hot_goods_and_spu':
			self.process_pdd_hot_goods_and_spu(item)

		elif spider.name == 'pdd_goods_sales_check_v5':
			self.process_pdd_goods_sales_check_v5(item)

		elif spider.name == 'pdd_auth_mall_goods_sales'  or spider.name == 'pdd_auth_mall_goods_sales_v2' :
			self.process_pdd_auth_mall_goods_sale(item)

		elif spider.name == 'pdd_crawl_goods_sales_v3' or spider.name == 'pdd_crawl_goods_sales_v4' or spider.name == 'pdd_crawl_goods_sales_v6' or spider.name == 'pdd_crawl_goods_sales_v7' :
			self.process_pdd_crawl_goods_sales_v3(item)

		elif spider.name == 'pdd_goods_sales_check_v6':
			self.process_pdd_goods_sales_check_v6(item)

		elif spider.name == 'pdd_sync_goods_v2' or spider.name == 'pdd_sync_goods_v3':
			self.process_pdd_sync_goods_v2(item)

		elif spider.name == 'pdd_auth_mall':
			self.process_pdd_auth_mall(item)

		elif spider.name == 'pdd_scroll_activity_v1' or spider.name == 'pdd_scroll_activity_v2' or spider.name == 'pdd_scroll_activity_v3':  # 拼多多活动
			self.process_scroll_activity(item)
		elif spider.name == 'pdd_activity_goods_v1'or spider.name == 'pdd_activity_goods_v2' or spider.name == 'pdd_activity_goods_v3':  # 拼多多活动商品信息
			self.process_goods_info(item)

		elif spider.name == 'pdd_goods_quantity_v1' or spider.name == 'pdd_goods_quantity_v2':
			self.process_goods_quantity(item)

		elif spider.name == 'pdd_jb_crawl_spider':
			self.process_pdd_jb(item)

	'''拼多多产品基本信息处理函数'''
	def process_pdd_goods(self, item):
		'''ssdb存储 MQ推送版本'''
		list_name  = 'pdd_goods_list'
		mall_list  = 'pdd_mall_id_list'
		queue_list = 'pdd_sync_mall_list'

		mq_data    = self.item_to_json(item)
		self.ssdb_client.qpush_back(list_name, mq_data) ##保存到产品信息队列

		if item['mall_id'] > 0:
			self.ssdb_client.qpush_back(queue_list, item['mall_id'])
			self.ssdb_client.qpush_back(mall_list, item['mall_id']) ##保存到店铺ID队列
			goods_id 	= item['goods_id']
			self.save_max_goods_id(goods_id)

	def process_pdd_sync_goods_v2(self, item):
		'''ssdb存储 MQ推送版本'''
		list_name = 'pdd_goods_list'
		mall_list = 'pdd_mall_id_list'
		queue_list = 'pdd_sync_mall_list'

		mq_data = self.item_to_json(item)
		self.ssdb_client.qpush_back(list_name, mq_data)  ##保存到产品信息队列

		if item['mall_id'] > 0:
			self.ssdb_client.qpush_back(queue_list, item['mall_id'])
			self.ssdb_client.qpush_back(mall_list, item['mall_id'])  ##保存到店铺ID队列
			goods_id = item['goods_id']
			self.save_max_goods_id(goods_id)

	'''拼多多店铺信息及店铺销量增量处理'''
	def process_pdd_mall(self, item):
		#mall_mq_class = mq.mq('PDD_MALL_INFO_EXCHANGE', 'PDD_CRAWLER_MALL_INFO_QUEUE', '') ##店铺信息队列
		#mall_sale_mq_class=mq.mq('PDD_MALL_SALE_EXCHANGE', 'PDD_CRAWLER_MALL_SALE_QUEUE', '') ##店铺销量队列
		
		mall_list = 'pdd_mall_list'
		mall_sale_list = 'pdd_mall_sale_list'

		'''保存店铺信息'''
		mall_json_data = self.item_to_json(item)
		self.ssdb_client.qpush_back(mall_list, mall_json_data)

		'''保存店铺增量信息'''
		mall_sale_incr = self.get_mall_sales_incr(item['mall_id'], item['mall_sales']) ##获取店铺销量增量信息
		if mall_sale_incr:
				#mall_sale_mq_class.publish_data(mall_sale_incr)
				self.ssdb_client.qpush_back(mall_sale_list, mall_sale_incr)
				today_hash = 'pdd_crawl_mall_id_hash:'+str(self.today)
				today_list = 'pdd_crawl_mall_id_list:'+str(self.today)
				tomorrow_hash = 'pdd_crawl_mall_id_hash:'+str(self.tomorrow)
				tomorrow_list = 'pdd_crawl_mall_id_list:'+str(self.tomorrow)
				if not self.ssdb_client.hget(today_hash, item['mall_id']):
					self.ssdb_client.hset(today_hash, item['mall_id'], item['mall_id'])
					# 新数据放入队列
					self.ssdb_client.qpush_back(today_list, item['mall_id'])
				if not self.ssdb_client.hget(tomorrow_hash, item['mall_id']):
					self.ssdb_client.hset(tomorrow_hash, item['mall_id'], item['mall_id'])
					# 新数据放入队列
					self.ssdb_client.qpush_back(tomorrow_list, item['mall_id'])
				self.ssdb_client.hset('pdd_crawl_mall_id_hash', item['mall_id'], item['mall_id'])

		self.save_max_mall_id(item['mall_id']) ##保存最大店铺ID

	'''保存店铺销量增量信息'''
	def get_mall_sales_incr(self, mall_id, sales):
		hash_name = 'pdd_mall_sales'
		hash_key  = mall_id
		sales 	  = int(sales)
		if sales <= 0:
			return False

		last_record= self.ssdb_client.hget(hash_name, hash_key) ##获取上次的销量总数
		if last_record:
			last_sales = int(last_record.decode('utf-8'))
			diff_sales = sales - last_sales ##获取销量增量
		else:
			diff_sales = sales

		if not last_record or diff_sales > 0: ##不存在销量记录或者销量增量大于0 则更新记录
			msg = self.ssdb_client.hset(hash_name, hash_key, sales)
		
		if diff_sales <= 0: ##销量未变化则不更新
			return False

		'''推送店铺销量数据到队列'''
		sale_data = {'mall_id':mall_id, 'sales':diff_sales, 'total_sales':sales}
		sale_data = self.item_to_json(sale_data)
		#self.push_data_to_mq('', '', '', sale_data)
		return sale_data

	def process_pdd_auth_mall(self, item):
		mall_list = 'pdd_mall_list'
		mall_sale_list = 'pdd_mall_sale_list'
		auth_mall_id_list = 'pdd_auth_mall_id_list:'+str(self.today)

		'''保存店铺信息'''
		mall_json_data = self.item_to_json(item)
		self.ssdb_client.qpush_back(mall_list, mall_json_data)

		'''保存店铺增量信息'''
		mall_sale_incr = self.get_mall_sales_incr(item['mall_id'], item['mall_sales'])  ##获取店铺销量增量信息
		if mall_sale_incr:
			self.ssdb_client.qpush_back(auth_mall_id_list, item['mall_id'])
			today_hash = 'pdd_crawl_mall_id_hash:' + str(self.today)
			if not self.ssdb_client.hget(today_hash, item['mall_id']):
				self.ssdb_client.qpush_back(mall_sale_list, mall_sale_incr)
				self.ssdb_client.hset(today_hash, item['mall_id'], item['mall_id'])
		self.save_max_mall_id(item['mall_id'])  ##保存最大店铺ID

	'''处理产品销量增量信息'''
	def process_pdd_goods_sale(self, item):
		mall_id    = item['mall_id']
		# 销量对比列表
		queue_name  = 'pdd_goods_sales_list'
		today_hash = 'pdd_goods_sales_hash:'+str(self.today)
		list_name = 'pdd_goods_price_check_list'
		goods_list = item['goods_list']

		for goods in goods_list:
			goods_id 	= goods['goods_id']
			goods_sales = int(goods['cnt']) ##产品总销量
			goods_price = int(goods['group']['price']) ##产品团购价
			goods_price = float(goods_price/100)
			if goods_sales <= 0: ##没有销量
				continue

			if not self.ssdb_client.hget(today_hash, goods_id):
				goods_sales_detail = json.dumps({'goods_id': goods_id, 'goods_sales':goods_sales, 'goods_price': goods_price, 'mall_id':mall_id})
				self.ssdb_client.hset(today_hash, goods_id, goods_sales_detail)
				self.ssdb_client.qpush_back(list_name, json.dumps(goods))
				self.ssdb_client.qpush_back(queue_name, goods_sales_detail)
			# self.check_goods_sales(goods_id, goods_sales, goods_price, mall_id);
			#self.check_goods_price(goods_id, goods_price);

			# goods_info 	=	self.get_goods_info(goods_id) ##获取产品信息
			# last_sales 	=	int(goods_info['sales'])
			# last_price  =	float(goods_info['price'])

			# sales_diff  = 	goods_sales - last_sales
			# price_diff  = 	goods_price != last_price

			# if not sales_diff and  not price_diff: ##价格或销量没有变化 则跳过
			# 	continue

			# ##防止第一次初始化数据差异过大
			# if sales_diff > 10000 and sales_diff == goods_sales:
			# 	sales_diff = random.randint(1, 100)
			
			# goods_info['sales'] 	=	goods_sales
			# goods_info['price']		=	goods_price
			# self.ssdb_client.hset(hash_name, goods_id, json.dumps(goods_info))

			# if sales_diff < 0:
			# 	continue

			# if sales_diff > 0: ##销量有变化
			# 	amount    = float(goods_price *  sales_diff ) ##获取产品销量
			# 	total_amount=float(goods_price * goods_sales ) ##获取产品总销售额
			# 	push_data = {'goods_id':goods_id, 'mall_id':mall_id, 'sales':sales_diff,'amount':amount, 'total_sales':goods_sales,'total_amount':total_amount}
			# 	push_data = self.item_to_json(push_data) ##销量转换成json
			# 	self.ssdb_client.qpush_back(goods_sale_list, push_data)

			# if price_diff: ##价格有变化
			# 	push_data = {'goods_id':goods_id, 'price':goods_price}
			# 	push_data = self.item_to_json(push_data)
			# 	self.ssdb_client.qpush_back('pdd_goods_price_list', push_data)

	'''检测产品销量变化'''
	def check_goods_sales(self, goods_id, goods_sales, goods_price, mall_id, is_force=False, date=0):
		goods_sale_hash = 'pdd_goods_sale_hash:'+str(self.today)
		goods_sale_list = 'pdd_goods_sale_list'
		goods_sale_chech_hash = 'pdd_goods_sales_check_hash:'+str(self.today)
		date = int(date)
		if not date: ##销量抓取日期
			date  = time.strftime('%Y-%m-%d')
			date  = int( time.mktime( time.strptime(date, '%Y-%m-%d') ) )

		goods_info 	=	self.get_goods_info(goods_id) ##获取产品信息
		if 'date' in goods_info.keys():
			if date <= goods_info['date']:
				return True

		last_sales 	=	int(goods_info['sales'])
		sales_diff  = 	goods_sales - last_sales
		if sales_diff == 0: ##销量没有变化 则跳过
			return True

		##防止第一次初始化数据差异过大
		if sales_diff > 10000 and sales_diff == goods_sales:
			sales_diff = random.randint(1, 100)

		total_day_amount = float(goods_price * sales_diff)  # 日销售额
		if (total_day_amount > 100000 or sales_diff > 1000 or sales_diff < -100) and is_force == False:  # 日销售额大于500000或者销量大于1000或者销量小于100
			if not self.ssdb_client.hget(goods_sale_chech_hash, goods_id):
				self.ssdb_client.hset(goods_sale_chech_hash, goods_id, goods_id)
				self.ssdb_client.qpush('pdd_goods_sales_check_list', str(goods_id))
			return True

		# ##保存产品销量信息
		goods_info['sales'] = goods_sales
		goods_info['date']  = date

		self.save_goods_info(goods_id, goods_info)

		if sales_diff > 0: ##销量正数变化
			amount    = float(goods_price *  sales_diff ) ##获取产品销量
			total_amount=float(goods_price * goods_sales ) ##获取产品总销售额
			push_data = {'goods_id':goods_id, 'mall_id':mall_id, 'sales':sales_diff,'amount':amount, 'total_sales':goods_sales,'total_amount':total_amount}
			push_data = self.item_to_json(push_data) ##销量转换成json
			self.ssdb_client.qpush_back(goods_sale_list, push_data)
			self.ssdb_client.hset(goods_sale_hash, goods_id, push_data)

	'''检测产品销量变化'''
	def check_goods_sales_new(self, goods_id, goods_sales, goods_price, mall_id, is_force=False, date=0):
		force_check_goods_sales_list = 'force_check_goods_sales_list'
		self.ssdb_client.qpush_back(force_check_goods_sales_list, json.dumps({
			'goods_id': goods_id,
			'goods_sales': goods_sales,
			'goods_price': goods_price,
			'mall_id': mall_id,
			'date': date
		}))

	def goods_sales_new_v2(self, goods_id, goods_sales, goods_price, mall_id, is_force=False, date=0):
		hot_check_goods_sales_list = 'hot_check_goods_sales_list'
		self.ssdb_client.qpush_back(hot_check_goods_sales_list, json.dumps({
			'goods_id': goods_id,
			'goods_sales': goods_sales,
			'goods_price': goods_price,
			'mall_id': mall_id,
			'date': date
		}))


	'''检测产品价格是否有变化，有变化则推送到ssdb'''
	def check_goods_price(self, goods_id, goods_price):
		price_info  =   self.ssdb_client.hget('pdd_goods_price', goods_id)
		if price_info:
			price_info = json.loads( price_info.decode('utf-8') )
			last_price = float(price_info['price'])
			if last_price != goods_price:
				price_info['price'] = goods_price
			else:
				return True
		else:
			price_info = {'price':goods_price}

		'''将价格变化放到待推送队里'''
		push_data = {'goods_id':goods_id, 'price':goods_price}
		push_data = self.item_to_json(push_data)
		self.ssdb_client.qpush_back('pdd_goods_price_list', push_data)

		self.ssdb_client.hset('pdd_goods_price', goods_id, json.dumps(price_info) )

		# goods_info 	=	self.get_goods_info(goods_id) ##获取产品信息
		# last_price  =	float(goods_info['price'])

		# price_diff  = 	goods_price != last_price
		# if not price_diff: ##价格没有变化 则跳过
		# 	return True

		# push_data = {'goods_id':goods_id, 'price':goods_price}
		# push_data = self.item_to_json(push_data)
		
		# self.ssdb_client.qpush_back('pdd_goods_price_list', push_data)
		# goods_info['price'] = goods_price
		# self.save_goods_info(goods_id, goods_info)

	
	def process_pdd_goods_category(self, item):
		hash_name  = 'pdd_goods_activity_2'
		queue_name = 'pdd_goods_activity_list'

		for data in item['cat_list']:
			hash_key = data['subject_id']

			mq_data = self.item_to_json(data)
			#self.push_data_to_mq()
			self.ssdb_client.hset(hash_name, hash_key, mq_data)
			self.ssdb_client.qpush_back(queue_name, mq_data)

	def process_pdd_category_goods(self, item):

		goods_rank_list  = 'pdd_goods_rank_list'

		for data in item['goods_lists']:
			hash_key = data['subject_id']
			mq_data = self.item_to_json(data)

			self.ssdb_client.qpush_back(goods_rank_list, mq_data)
			#pdd_category_goods_mq_class.publish_data(mq_data)
			#self.push_data_to_mq('PDD_CRAWLER_EXCHANGE', 'PDD_CRAWLER_GOODS_RANK_QUEUE', 'PDD_CRAWLER_GOODS_RANK_ROUTING_KEY', mq_data)

			#msg = ssdb_class.hash_set(hash_name, hash_key, mq_data)
	
	'''保存拼多多首页滚动广告栏类目'''
	def process_pdd_scroll_activity(self, item):
		hash_name  	= 'pdd_goods_activity_1'
		queue_list 	= 'pdd_goods_activity_list'

		for data in item['cat_list']:
			hash_key = data['subject_id']
			
			data['path'] = '>'.join( data['path'] )
			data['path_id'] = '>'.join( str(s) for s in data['path_id'] )

			mq_data = self.item_to_json(data)
			
			self.ssdb_client.qpush_back(queue_list, mq_data)
			self.ssdb_client.hset(hash_name, hash_key, mq_data)

	'''保存拼多多秒杀信息'''
	def process_pdd_seckill(self, item):
		if item['goods_seckill_info']:
			for data in item['goods_seckill_info']:
				mq_data = self.item_to_json(data)
				self.ssdb_client.qpush_back('pdd_goods_activity_list', mq_data)

		if item['goods_list']:
			pass
			for data in item['goods_list']:
				mq_data = self.item_to_json(data)
				self.ssdb_client.qpush_back('pdd_goods_seckill_list', mq_data)

		if item['goods_rank_list']:
			pass
			for data in item['goods_rank_list']:
				mq_data = self.item_to_json(data)
				self.ssdb_client.qpush_back('pdd_goods_rank_list', mq_data)

	'''将item对象转换为json格式'''
	def item_to_json(self, item):
		pass
		date = int(time.time())
		data = {"date":date}

		data = dict(data, **item)
		# for key in item:
		# 	data[key] = item[key]

		data = json.dumps(data)
		return data

	'''保存最大的产品ID'''
	def save_max_goods_id(self, goods_id):
		key 		= 'pdd_max_goods_id'
		goods_id 	= int(goods_id)

		max_goods_id= self.ssdb_client.get(key)
		if max_goods_id == None:
			self.ssdb_client.set(key, goods_id)
		else:
			max_goods_id = int(max_goods_id.decode('utf-8'))
			if max_goods_id < goods_id:
				self.ssdb_client.set(key, goods_id)

	###将数据推送到队列中
	# def push_data_to_mq(self, exchage, queue, queue_route_key, data):
	# 	mq_class = mq.mq(exchage, queue, queue_route_key)
	# 	mq_class.publish_data(data)
	# 	#mq_class.close()
	
	def get_goods_info(self, goods_id):
		hash_name 	=	'pdd_goods_sales'
		hash_key 	=	goods_id
		i = 0
		goods_data 	= 	self.ssdb_client.hget(hash_name, hash_key)
		while i<=3 and not goods_data:
			goods_data = self.ssdb_client.hget(hash_name, hash_key)
			i+=1

		if goods_data:
			goods_data	=	goods_data.decode('utf-8')
			if len(goods_data) > 10: ##新的字典类型的数据
				goods_data = json.loads(goods_data)
			else:
				goods_price = self.ssdb_client.hget('pdd_goods_price', hash_key)
				if goods_price:
					goods_price = int(goods_price.decode('utf-8'))
					goods_price = float(goods_price/100)
				else:
					goods_price = 0
				
				goods_data = {'sales':goods_data, 'price':goods_price}
		else:
			goods_data = {'sales':0, 'price':0}

		return goods_data

	def save_goods_info(self, goods_id, goods_info):
		hash_name 	= 'pdd_goods_sales'
		hash_key 	= goods_id

		self.ssdb_client.hset(hash_name, hash_key, json.dumps(goods_info))

	def save_max_mall_id(self, mall_id):
		name 		= 'pdd_max_mall_id'
		mall_id 	= int(mall_id)
		max_mall_id = self.ssdb_client.get(name)
		if max_mall_id:
			max_mall_id = int(max_mall_id.decode('utf-8'))
		else:
			max_mall_id = 0

		if mall_id > max_mall_id:
			self.ssdb_client.set(name, mall_id)

	def process_pdd_mall_category(self, item):
		mall_category_hash = 'pdd_mall_category_hash'
		cat_list = item['cat_list']

		for cat in cat_list:
			cat_id = cat['cat_id']
			info = self.ssdb_client.hset(mall_category_hash, cat_id, json.dumps(cat))
			#print(info)
	
	def process_pdd_mall_keywords(self, item):
		keyword_list = 'pdd_keywords_list'
		# keyword_list = 'pdd_keywords_list_middle'

		keyword_data = item['cat_list']
		push_data = self.item_to_json(keyword_data)
		self.ssdb_client.qpush_back(keyword_list, push_data)

		# keyword_data_list 	= 'pdd_mall_keywords_list'
		# keywords_hash 		= 'pdd_keywords_hash'

		# keyword 	=	item['cat_list']['keyword']

		# keyword_data 	= self.item_to_json(item['cat_list'])

		# self.ssdb_client.qpush_back(keyword_data_list, keyword_data)
		# self.ssdb_client.hset(keywords_hash, keyword, 1)

	def process_pdd_promotion_keywords(self, item):
		keyword_list = 'pdd_promotion_keywords_list'

		keyword_data = item['cat_list']
		push_data = self.item_to_json(keyword_data)
		self.ssdb_client.qpush_back(keyword_list, push_data)

	def process_pdd_keywords_extend(self, item):
		hash_name 	=	'pdd_keywords_extend_hash'
		keyword_data=	item['cat_list']

		#hash_key 	=	json.dumps(keyword_data['keyword'])
		self.ssdb_client.hset(hash_name, keyword_data['keyword'], json.dumps(keyword_data))

	def process_pdd_scroll_activity_new(self, item):
		queue_name = 'pdd_goods_activity_list'
		cat_list = item['cat_list']
		for data in cat_list:
			subjectType = data['type']
			subject_id  = data['subject_id']

			if subjectType == 1: ##活动
				hash_name = 'pdd_goods_activity_1'
				data['path'] = ' > '.join(data['path'])
				data['path_id'] = '>'.join( str(s) for s in data['path_id'] )

			elif subjectType == 2: ##拼多多前台首页分类 不从此处获取了
				continue
				hash_name = 'pdd_goods_activity_2'

			push_data = self.item_to_json(data)
			#print(push_data)
			self.ssdb_client.hset(hash_name, subject_id, push_data)
			self.ssdb_client.qpush_back(queue_name, push_data)

	def process_pdd_activity_goods(self, item):
		goods_rank_list  = 'pdd_goods_rank_list'

		for data in item['goods_lists']:
			mq_data = self.item_to_json(data)
			self.ssdb_client.qpush_back(goods_rank_list, mq_data)

	def process_pdd_goods_sales_check(self, item):
		goods_list = item['goods_lists']
		goods_price= float( int(goods_list['goods_price']) / 100)
		self.check_goods_sales_new(goods_list['goods_id'], int(goods_list['goods_sales']), goods_price, goods_list['mall_id'], True)

	def process_pdd_goods_sales_check_v5(self, item):
		list_name = 'pdd_goods_list'

		#保存推送spu  hot_goods
		item = item['goods_lists']
		hot_goods_list = 'pdd_hot_goods_list'
		spu_data = self.make_spu_data(item)
		hot_goods_data = self.make_hot_goods_data(item)
		#推送到产品基础
		self.ssdb_client.qpush_back(list_name, spu_data)
		self.ssdb_client.qpush_back(hot_goods_list, hot_goods_data)

		#执行销量检测
		goods_list = {
							'goods_id': item['goods_id'],
							'goods_sales': item['sales'],
							'mall_id': item['mall_id'],
							'goods_price': item['min_on_sale_group_price']
						}
		goods_price = float(int(goods_list['goods_price']) / 100)
		self.check_goods_sales_new(goods_list['goods_id'], int(goods_list['goods_sales']), goods_price, goods_list['mall_id'], True)


	def process_pdd_goods_sales_check_v6(self, item):
		list_name = 'pdd_goods_list'

		# 保存推送spu  hot_goods
		item = item['goods_lists']
		price = item["price"]
		sku = item['sku']
		hot_goods_list = 'pdd_hot_goods_list'
		spu_data = self.make_spu_data_v2(item, price)
		hot_goods_data = self.make_hot_goods_data_v2(item, price, sku)
		# 推送到产品基础
		self.ssdb_client.qpush_back(list_name, spu_data)
		self.ssdb_client.qpush_back(hot_goods_list, hot_goods_data)

		# 执行销量检测
		goods_list = {
			'goods_id': item['goods_id'],
			'goods_sales': item['sold_quantity'],
			'mall_id': item['mall_id'],
			'goods_price': price['min_on_sale_group_price']
		}
		goods_price = float(int(goods_list['goods_price']) / 100)

		self.check_goods_sales_new(goods_list['goods_id'], int(goods_list['goods_sales']), goods_price,
						   	goods_list['mall_id'], True)


	def process_pdd_crawl_goods_sales_v3(self, item):
		list_name = 'pdd_goods_list'

		# 保存推送spu  hot_goods
		item = item['goods_lists']
		price = item['price']
		sku = item['sku']
		hot_goods_list = 'pdd_hot_goods_list'
		spu_data = self.make_spu_data_v2(item, price)
		hot_goods_data = self.make_hot_goods_data_v2(item, price, sku)
		# 推送到产品基础
		self.ssdb_client.qpush_back(list_name, spu_data)
		self.ssdb_client.qpush_back(hot_goods_list, hot_goods_data)

		# 执行销量检测
		goods_list = {
			'goods_id': item['goods_id'],
			'goods_sales': item['sold_quantity'],
			'mall_id': item['mall_id'],
			'goods_price': price['min_on_sale_group_price']
		}
		goods_price = float(int(goods_list['goods_price']) / 100)
		self.goods_sales_new_v2(goods_list['goods_id'], int(goods_list['goods_sales']), goods_price,
								   goods_list['mall_id'], True)

	def process_pdd_category_goods_sales(self, item):
		date 	   = time.strftime('%Y-%m-%d')
		timestamp  = int( time.mktime( time.strptime(date, '%Y-%m-%d') ) )

		goods_list = item['goods_lists']
		today_hash  = 'pdd_category_goods_sales_hash:'+str(self.today)
		list_name  = 'pdd_category_goods_sales_list'
		for goods in goods_list:
			goods_sales = goods['cnt']
			goods_price = int(goods['group']['price'])/100
			mall_id 	= 0
			goods_id 	= goods['goods_id']
			goods_data  = json.dumps({'goods_id':goods_id, 'goods_sales':goods_sales, 'goods_price':goods_price, 'mall_id':mall_id, 'date':timestamp})
			if not self.ssdb_client.hget(today_hash, goods_id):
				self.ssdb_client.hset(today_hash, goods_id, goods_data)
				self.ssdb_client.qpush_back(list_name, goods_data )

	def process_pdd_keyword_goods_sales(self, item):
		date 	   = time.strftime('%Y-%m-%d')
		timestamp  = int( time.mktime( time.strptime(date, '%Y-%m-%d') ) )

		goods_list = item['goods_list']
		list_name  = 'pdd_keyword_goods_sales_list'
		today_hash = 'pdd_keyword_goods_sales_hash:'+str(self.today)
		for goods in goods_list:
			goods_sales = goods['sales']
			goods_price = int(goods['price'])/100
			mall_id 	= 0
			goods_id 	= goods['goods_id']
			goods_data  = json.dumps({'goods_id':goods_id, 'goods_sales':goods_sales, 'goods_price':goods_price, 'mall_id':mall_id, 'date':timestamp})
			if not self.ssdb_client.hget(today_hash, goods_id):
				self.ssdb_client.hset(today_hash, goods_id, goods_data)
				self.ssdb_client.qpush_back(list_name, goods_data )

	'''关键词排名100商品销量列表'''
	def process_pdd_keyword_goods_total_sales(self, item):
		goods_list = item['goods_list']
		keyword_goods_hash = 'pdd_keyword_goods_hash:'+str(self.today)
		for goods in goods_list:
			keyword 	= goods['keyword']
			goods_id 	= goods['goods_id']
			if not self.ssdb_client.hget(keyword_goods_hash,keyword):
				goods_id_list = []
				goods_id_list.append(goods_id)
				self.ssdb_client.hset(keyword_goods_hash,keyword,json.dumps(goods_id_list))
			else:
				goods_id_list = self.ssdb_client.hget(keyword_goods_hash,keyword)
				goods_id_list = goods_id_list.decode('utf-8')
				goods_id_list.append(goods_id)
				self.ssdb_client.hset(keyword_goods_hash,keyword,json.dumps(goods_id_list))



	def process_pdd_category_goods_sales_push(self, item):
		goods_data = item['goods_lists']
		if goods_data['goods_sales'] <= 0:
			return True
		self.check_goods_sales(goods_data['goods_id'], goods_data['goods_sales'], goods_data['goods_price'], goods_data['mall_id'], False, goods_data['date'])

	def process_pdd_goods_price(self, item):
		mall_id    = item['mall_id']
		goods_list = item['goods_list']

		for goods in goods_list:
			goods_id 	= goods['goods_id']
			goods_sales = int(goods['cnt']) ##产品总销量
			goods_price = int(goods['group']['price']) ##产品团购价
			goods_price = float(goods_price/100)

			if goods_sales <= 0: ##没有销量
				continue

			self.check_goods_price(goods_id, goods_price)

	def process_pdd_goods_reviews(self, item):
		goods_id = item['mall_id']
		review_list = item['goods_list']
		max_review_id_hash = 'pdd_goods_max_review_id' ##产品review_id hash集合
		max_review_id = self.ssdb_client.hget(max_review_id_hash, goods_id) ##获取产品最大review_id
		max_review_id = int(max_review_id.decode('utf-8')) if max_review_id else 0
		
		record_max_review_id = max_review_id  ##ssdb中记录的最大review_id

		for review in review_list:
			review_id 		 =  int(review['review_id'])
			if max_review_id >= review_id: ##小于记录的最大review_id 则跳过
				continue
			else:
				if record_max_review_id < review_id:
					record_max_review_id = review_id
					self.ssdb_client.hset(max_review_id_hash, goods_id, review_id) ##更新最大review_id
				
			review_data = {
				'goods_id':goods_id,
				'uid':review['uid'],
				'comment':review['comment'],
				'stars':review['stars'],
				'time':review['time'],
				'avatar':review['avatar'],
				'name':review['name'],
				'pictures':json.dumps(review['pictures']),
				'review_id':review['review_id'],
			}
			review_keys = review.keys()
			review_data['labels'] = review['labels'] if 'labels' in review_keys else ''
			review_data['order_nums'] = review['order_nums'] if 'order_nums' in review_keys else 1
			review_data['append'] = json.dumps( review['append'] ) if 'append' in review_keys else ''
			review_data['desc_score'] = json.dumps( review['desc_score'] ) if 'desc_score' in review_keys else ''
			review_data['logistics_score'] = json.dumps( review['logistics_score'] ) if 'logistics_score' in review_keys else ''
			review_data['service_score'] = json.dumps( review['service_score'] ) if 'service_score' in review_keys else ''
			review_data['specs'] = json.dumps( review['specs'] ) if 'specs' in review_keys else ''

			review_data = self.item_to_json(review_data)
			self.ssdb_client.qpush_back('pdd_goods_reviews_list', review_data)

	def process_pdd_goods_sku(self, item):
		goods_data = item['goods_list']
		time = goods_data['time']
		goods_id = item['mall_id']
		sku_list = []
		push_data= {'goodsId':goods_id, 'time':time}
		key_list = ['sku_id', 'thumb_url', 'quantity', 'is_onsale', 'spec', 'normal_price', 'group_price', 'specs', 'weight']
		
		for sku_data in goods_data['sku']:
			for key in list(sku_data.keys()):
				if key not in key_list:
					del sku_data[key]

			# sku_data['normal_price'] /= 100
			# sku_data['group_price']  /= 100
			sku_data['specs'] = json.dumps(sku_data['specs'])
			sku_list.append(sku_data)
			# sku_data = self.item_to_json(sku_data)
			# self.ssdb_client.qpush_back('pdd_goods_sku_list', sku_data)
		push_data['skuList'] = sku_list
		push_data = json.dumps(push_data)
		
		self.ssdb_client.qpush_back('pdd_goods_sku_list', push_data)

	def   save_goods_log(self, log_data):
		# log_data = self.item_to_json(log_data)  ##销量转换成json
		# date = time.strftime('%Y-%m-%d')
		# file_name = "/data/spider/log/" + str(date) + ".log";
		# with open(file_name, "a+") as f:
		# 	f.write(log_data)
		date = time.strftime('%Y-%m-%d')
		file_path = '/data/spider/log/sales_log'
		if not os.path.exists(file_path):
			os.makedirs(file_path)

		file_name = file_path+'/'+date+'.log'
		with open(file_name, "a+") as f:
			f.write(log_data+"\r\n")

	def process_pdd_mall_check(self, item):
		mall_list = 'pdd_mall_list'
		'''保存店铺信息'''
		mall_json_data = self.item_to_json(item)
		self.ssdb_client.qpush_back(mall_list, mall_json_data)


	def process_pdd_auth_mall_goods_sale(self,item):
		mall_id    = item['mall_id']
		# 销量对比列表
		queue_name  = 'pdd_auth_goods_sales_list'
		today_hash = 'pdd_goods_sales_hash:'+str(self.today)
		list_name = 'pdd_goods_price_check_list'
		goods_list = item['goods_list']

		for goods in goods_list:
			goods_id 	= goods['goods_id']
			goods_sales = int(goods['cnt']) ##产品总销量
			goods_price = int(goods['group']['price']) ##产品团购价
			goods_price = float(goods_price/100)
			if goods_sales <= 0: ##没有销量
				continue

			if not self.ssdb_client.hget(today_hash, goods_id):
				self.ssdb_client.hset(today_hash, goods_id, goods_id)
				self.ssdb_client.qpush_back(list_name, json.dumps(goods))
				self.ssdb_client.qpush_back(queue_name, json.dumps({'goods_id': goods_id, 'goods_sales':goods_sales, 'goods_price': goods_price, 'mall_id':mall_id}))

	def process_pdd_keyword_goods_rank(self, item):  # 把产品列表推到ssdb队列里去
		goods_list = item['goods_list']
		list_name = 'pdd_keyword_goods_rank_list'
		max_num = len(goods_list)
		i = 0
		num = 100
		loop = True
		while loop:
			if i + num < max_num:
				goods_info = goods_list[i:i + num]
				self.ssdb_client.qpush_back(list_name, json.dumps(goods_info))
				i = i + num
			else:
				goods_info = goods_list[i:max_num]
				self.ssdb_client.qpush_back(list_name, json.dumps(goods_info))
				loop = False
				continue

	def process_pdd_keyword_goods_rank_v2(self, item):  # 把产品列表推到ssdb队列里去
		goods_list = item['goods_list']
		list_name = 'pdd_keyword_goods_rank_list'
		monitor_list_name = 'pdd_keyword_goods_monitor_result'
		max_num = len(goods_list)
		i = 0
		num = 100
		loop = True
		while loop:
			if i + num < max_num:
				goods_info = goods_list[i:i + num]
				self.ssdb_client.qpush_back(list_name, json.dumps(goods_info))
				self.ssdb_client.qpush_back(monitor_list_name, json.dumps({
					'goods_list': goods_info,
					'keyword': item['keyword']
				}))
				i = i + num
			else:
				goods_info = goods_list[i:max_num]
				self.ssdb_client.qpush_back(list_name, json.dumps(goods_info))
				self.ssdb_client.qpush_back(monitor_list_name, json.dumps({
					'goods_list': goods_info,
					'keyword': item['keyword']
				}))
				loop = False
				continue

	def process_pdd_hot_goods_and_spu(self, item):
		#产品基础
		list_name = 'pdd_goods_list'

		item = item['goods_list']
		hot_goods_list = 'pdd_hot_goods_list'
		spu_data = self.make_spu_data(item)
		hot_goods_data = self.make_hot_goods_data(item)

		#推送到基础信息
		self.ssdb_client.qpush_back(list_name, spu_data)
		self.ssdb_client.qpush_back(hot_goods_list, hot_goods_data)

	def make_spu_data(self, goods):
		min_group_price = float(goods['min_on_sale_group_price'] / 100)

		re = {
			'goods_id': goods['goods_id'],
			'mall_id': goods['mall_id'],
			'goods_type': goods['goods_type'],
			'category1': str(goods['cat_id_1']),
			'category2': str(goods['cat_id_2']),
			'category3': str(goods['cat_id_3']),
			'goods_name': goods['goods_name'],
			'market_price': float(goods['market_price'] / 100),  # 单位：元，下同
			'max_group_price': float(goods['max_on_sale_group_price'] / 100),
			'min_group_price': min_group_price,
			'max_normal_price': float(goods['max_on_sale_normal_price'] / 100),
			'min_normal_price': float(goods['min_on_sale_normal_price'] / 100),
			'thumb_url': goods['thumb_url'],
			'publish_date': goods['created_at'],
			'total_sales': int(goods['sales']),
			'is_on_sale': goods['is_onsale'],
			'price': min_group_price,
			'total_amount': float(int(goods['sales']) * float(min_group_price)),  # 总销售额
			'date': int(time.time())
		}
		return json.dumps(re)

	def make_spu_data_v2(self, goods, price):
		min_group_price = float(price['min_on_sale_group_price'] / 100)

		re = {
			'goods_id': goods['goods_id'],
			'mall_id': goods['mall_id'],
			'goods_type': goods['goods_type'],
			'category1': str(goods['cat_id_1']),
			'category2': str(goods['cat_id_2']),
			'category3': str(goods['cat_id_3']),
			'goods_name': goods['goods_name'],
			'market_price': float(goods['market_price'] / 100),  # 单位：元，下同
			'max_group_price': float(price['max_on_sale_group_price'] / 100),
			'min_group_price': min_group_price,
			'max_normal_price': float(price['max_on_sale_normal_price'] / 100),
			'min_normal_price': float(price['min_on_sale_normal_price'] / 100),
			'thumb_url': goods['thumb_url'],
			# 'publish_date': goods['created_at'],
			'total_sales': int(goods['sold_quantity']),
			'is_on_sale': goods['is_onsale'],
			'price': min_group_price,
			'total_amount': float(int(goods['sold_quantity']) * float(min_group_price)),  # 总销售额
			'date': int(time.time())
		}
		return json.dumps(re)


	def make_hot_goods_data(self, goods):
		min_group_price = float(goods['min_on_sale_group_price'] / 100)
		re = {
			'goods_name': goods['goods_name'],
			'cat_id_1': str(goods['cat_id_1']),
			'cat_id_2': str(goods['cat_id_2']),
			'cat_id_3': str(goods['cat_id_3']),
			'goods_id': goods['goods_id'],
			'total_sales': int(goods['sales']),
			'mall_id': goods['mall_id'],
			'min_group_price': min_group_price,
			'last_update_time': int(time.time()),
			'publish_date': goods['created_at'],
			'is_on_sale': goods['is_onsale'],
			'detail': {
				'goods_desc': goods['goods_desc'],
				'goods_sn': goods['goods_sn'],
				'goods_type': goods['goods_type'],
				'group': goods['group'],
				'hd_thumb_url': goods['hd_thumb_url'],
				'image_url': goods['image_url'],
				'is_onsale': goods['is_onsale'],
				'is_pre_sale': goods['is_pre_sale'],
				'is_refundable': goods['is_refundable'],
				'market_price': float(goods['market_price']/100),
				'max_group_price': float(goods['max_group_price']/100),
				'max_normal_price': float(goods['max_normal_price']/100),
				'max_on_sale_group_price': float(goods['max_on_sale_group_price']/100),
				'max_on_sale_normal_price': float(goods['max_on_sale_normal_price']/100),
				'min_normal_price': float(goods['min_normal_price']/100),
				'min_on_sale_group_price': float(goods['min_on_sale_group_price']/100),
				'min_on_sale_normal_price': float(goods['min_on_sale_normal_price']/100),
				'off_sale_type': goods['off_sale_type'],
				'old_max_group_price': float(goods['old_max_group_price']/100),
				'old_max_on_sale_group_price': float(goods['old_max_on_sale_group_price']/100),
				'old_min_group_price': float(goods['old_min_group_price']/100),
				'old_min_on_sale_group_price': float(goods['old_min_on_sale_group_price']/100),
				'shipment_limit_second': goods['shipment_limit_second'],
				'side_sales_tip': goods['side_sales_tip'],
				'skip_goods': goods['skip_goods'],
				'thumb_url': goods['thumb_url'],
				'second_hand': goods['second_hand'],
				'quantity': goods['quantity'],
				'allowed_region': goods['allowed_region'],
				'quick_refund': goods['quick_refund'],
				'price_style': goods['price_style'],
				'has_promotion': goods['has_promotion'],
				'cost_province_codes': goods['cost_province_codes'],
				'cost_template_id': goods['cost_template_id'],
				'global_sold_quantity': goods['global_sold_quantity'],
				'oversea_type': goods['oversea_type'],
				'pre_sale_time': goods['pre_sale_time']
			},
			'skus': goods['sku'],
			'gallery': goods['gallery'],
		}
		return json.dumps(re)

	def make_hot_goods_data_v2(self, goods, price, sku):
		min_group_price = float(price['min_on_sale_group_price'] / 100)
		re = {
			'goods_name': goods['goods_name'],
			'cat_id_1': str(goods['cat_id_1']),
			'cat_id_2': str(goods['cat_id_2']),
			'cat_id_3': str(goods['cat_id_3']),
			'goods_id': goods['goods_id'],
			'total_sales': int(goods['sold_quantity']),
			'mall_id': goods['mall_id'],
			'min_group_price': min_group_price,
			'last_update_time': int(time.time()),
			# 'publish_date': goods['created_at'],
			'is_on_sale': goods['is_onsale'],
			'detail': {
				'goods_desc': goods['goods_desc'],
				'goods_type': goods['goods_type'],
				'group': goods['group'],
				'hd_thumb_url': goods['hd_thumb_url'],
				'image_url': goods['image_url'],
				'is_pre_sale': goods['is_pre_sale'],
				'market_price': float(goods['market_price']/100),
				'max_group_price': float(price['max_group_price']/100),
				'max_normal_price': float(price['max_normal_price']/100),
				'max_on_sale_group_price': float(price['max_on_sale_group_price']/100),
				'max_on_sale_normal_price': float(price['max_on_sale_normal_price']/100),
				'min_normal_price': float(price['min_normal_price']/100),
				'min_on_sale_group_price': float(price['min_on_sale_group_price']/100),
				'min_on_sale_normal_price': float(price['min_on_sale_normal_price']/100),
				'off_sale_type': goods['off_sale_type'],
				'old_max_group_price': float(price['old_max_group_price']/100),
				'old_max_on_sale_group_price': float(price['old_max_on_sale_group_price']/100),
				'old_min_group_price': float(price['old_min_group_price']/100),
				'old_min_on_sale_group_price': float(price['old_min_on_sale_group_price']/100),
				'shipment_limit_second': goods['shipment_limit_second'],
				'side_sales_tip': goods['side_sales_tip'],
				'skip_goods': goods['skip_goods'],
				'thumb_url': goods['thumb_url'],
				'second_hand': goods['second_hand'],
				'quantity': goods['quantity'],
				'allowed_region': goods['allowed_region'],
				'price_style': price['price_style'],
				'has_promotion': goods['has_promotion'],
				'cost_province_codes': goods['cost_province_codes'],
				'cost_template_id': goods['cost_template_id'],
				'global_sold_quantity': goods['global_sold_quantity'],
				'oversea_type': goods['oversea_type'],
				'pre_sale_time': goods['pre_sale_time']
			},
			'skus': sku,
			'gallery': goods['gallery'],
		}
		return json.dumps(re)

	def process_goods_quantity(self, item):
		spu_queue = 'goods_quantity_list'
		sku_queue = 'goods_sku_quantity_list'
		gallery_queue = 'goods_gallery_quantity_list'
		detail = item['goods_lists']['detail']
		skus = item['goods_lists']['skus']
		galleries = item['goods_lists']['galleries']
		self.ssdb_client.qpush_back(spu_queue, json.dumps(detail))
		self.ssdb_client.qpush_back(sku_queue, json.dumps(skus))
		self.ssdb_client.qpush_back(gallery_queue, json.dumps(galleries))

	'''将item对象转换为json格式'''
	def data_item_to_json(self, item):
		date = int(time.time())
		item.pop("cat_1")
		item.pop("cat_1_name")
		data = {"date": date, 'cat_1': 3, 'cat_1_name': '品牌好货'}
		data = dict(data, **item)
		data = json.dumps(data)
		return data

	def process_scroll_activity(self, item):
		hash_name = 'pdd_scroll_activity_v2'
		queue_list = 'pdd_type_activity_queue'
		cat_list = item['cat_list']
		self.save_goods_log(json.dumps({"cat_list": cat_list}))
		for data in cat_list:
			self.save_goods_log(json.dumps({"data_item": data}))
			data['path'] = '>'.join(data['path'])
			data['path_id'] = '>'.join(str(s) for s in data['path_id'])
			hash_key = str(data['type_1']) + str(data['type_2']) + str(data['type_3'])
			mq_data = self.item_to_json(data)
			self.ssdb_client.qpush_back(queue_list, mq_data)
			self.save_goods_log(json.dumps({"data_save_ssdb": mq_data, "key": hash_key}))
			self.ssdb_client.hset(hash_name, hash_key, mq_data)

	def process_goods_info(self, item):
		goods_lunbo = 'pdd_activity_goods_lunbo'
		goods_kill = 'pdd_activity_goods_kill'
		goods_99 = 'pdd_activity_goods_99'
		goods_short = 'pdd_activity_goods_short'
		goods_brand = 'pdd_activity_goods_brand'
		goods_shopping = 'pdd_activity_goods_shopping'
		goods_home = 'pdd_activity_goods_home'
		for data in item['goods_lists']:
			api_type = data['api_type']
			mq_data = self.item_to_json(data)
			if api_type in [61, 62, 64, 63]:
				self.ssdb_client.qpush_back(goods_lunbo, mq_data)
			if api_type in [14]:
				self.ssdb_client.qpush_back(goods_kill, mq_data)
			if api_type in [41]:
				self.ssdb_client.qpush_back(goods_99, mq_data)
			if api_type in [21]:
				self.ssdb_client.qpush_back(goods_short, mq_data)
			if api_type in [31]:
				self.ssdb_client.qpush_back(goods_brand, mq_data)
			if api_type in [51]:
				self.ssdb_client.qpush_back(goods_shopping, mq_data)
			if api_type in [71, 72]:
				self.ssdb_client.qpush_back(goods_home, mq_data)

	def process_pdd_jb(self, item):
		goods_rank_list = 'pdd_jb_goods_list'
		goods_type_list = 'pdd_jb_type_list'
		if 'goods_list' in item.keys():
			for data in item['goods_list']:
				mq_data = self.item_to_json(data)
				self.ssdb_client.qpush_back(goods_rank_list, mq_data)
		if 'cate_list' in item.keys():
			for data in item['cate_list']:
				# self.save_goods_log(json.dumps({'goods_list': data}))
				if data['cat_1'] == 2:
					mq_data = self.item_to_json(data)
					# self.save_goods_log(json.dumps({'jb_data_cat': mq_data}))
					self.ssdb_client.qpush_back(goods_type_list, mq_data)
				if data['cat_1'] == 1:
					mq_data = self.item_to_json(data)
					# self.save_goods_log(json.dumps({'jb_data_cat': mq_data}))
					self.ssdb_client.qpush_back(goods_type_list, mq_data)
					if data['cat_2'] not in [-11, 590]:
						mq_data_cat = self.data_item_to_json(data)
						# self.save_goods_log(json.dumps({'jb_data_cat': mq_data_cat}))
						self.ssdb_client.qpush_back(goods_type_list, mq_data_cat)
