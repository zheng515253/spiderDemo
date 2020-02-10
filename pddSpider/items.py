# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy

'''产品信息'''
class SpiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    goods_id	= scrapy.Field()
    mall_id		= scrapy.Field()
    goods_type	= scrapy.Field()
    category1	= scrapy.Field()
    category2   = scrapy.Field()
    category3 	= scrapy.Field()
    goods_name	= scrapy.Field()
    market_price= scrapy.Field()
    price 		= scrapy.Field()
    is_on_sale   = scrapy.Field()
    max_group_price   = scrapy.Field()
    min_group_price   = scrapy.Field()
    max_normal_price  = scrapy.Field()
    min_normal_price  = scrapy.Field()
    thumb_url        = scrapy.Field()
    publish_date     = scrapy.Field()
    total_sales     =  scrapy.Field()
    total_amount    =  scrapy.Field()
    #goods_list = scrapy.Field()

'''店铺信息'''
class MallItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    mall_id		= scrapy.Field()
    mall_name	= scrapy.Field()
    goods_num	= scrapy.Field()
    score_avg	= scrapy.Field()
    mall_sales	= scrapy.Field()
    is_open		= scrapy.Field()
    status		= scrapy.Field()
    province	= scrapy.Field()
    city 		= scrapy.Field()
    area   		= scrapy.Field()
    street      = scrapy.Field()
    logo    	= scrapy.Field()

'''产品销量信息'''
class GoodsSalesItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    goods_list = scrapy.Field()
    mall_id    = scrapy.Field()

'''活动及分类信息'''
class CategoryItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    #first_name = scrapy.Field()
    #second_name= scrapy.Field()
    cat_list   = scrapy.Field()

'''分类下产品信息'''
class CategoryGoodsItem(scrapy.Item):
    pass
    # define the fields for your item here like:
    # name = scrapy.Field()
    goods_lists = scrapy.Field()

'''秒杀信息'''
class PddSeckillItem(scrapy.Item):
    pass
    # define the fields for your item here like:
    # name = scrapy.Field()
    goods_list = scrapy.Field()
    goods_rank_list = scrapy.Field()
    goods_seckill_info = scrapy.Field()
    
'''查询关键字后产品列表'''
class KeywordGoodsList(scrapy.Item):
    goods_list = scrapy.Field()
    page = scrapy.Field()
    keyword = scrapy.Field()


class DuoDuoJinBaoItem(scrapy.Item):
    """ 进宝商品信息"""
    goods_list = scrapy.Field()
    cate_list = scrapy.Field()