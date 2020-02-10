# -*- coding: utf-8 -*-
import scrapy
import json, time, sys, random, re, pyssdb
from spider.items import GoodsSalesItem

'''获取产品评价'''


class PddGoodsReviewsSpider(scrapy.Spider):
    name = 'pdd_goods_reviews'
    hash_name = 'pdd_review_goods_id_hash'

    limit = 1000
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {'spider.middlewares.BuildHeaderMiddleware': 100},
        # 'LOG_FILE':'',
        # 'LOG_LEVEL':'DEBUG',
        # 'LOG_ENABLED':True,
    }

    handle_httpstatus_list = [403]

    def __init__(self, hash_num=0, process_nums=1):
        self.hash_num = int(hash_num)  ##当前脚本号
        self.process_nums = int(process_nums)  ##脚本总数

        self.ssdb = pyssdb.Client('172.16.0.5', 8888)

    def start_requests(self):
        goods_nums = self.limit * int(self.process_nums)  ##一次查询的数量
        is_end = False
        start_goods_id = ''  ##起始查询的店铺key
        while not is_end:
            goods_ids = self.ssdb.hkeys(self.hash_name, start_goods_id, '', goods_nums)
            if not goods_ids:  ##没有数据返回
                is_end = True
                continue

            for goods_id in goods_ids:
                goods_id = int(goods_id.decode('utf-8'))
                # start_goods_id = goods_id
                if goods_id % self.process_nums != self.hash_num:
                    continue

                review_list = []
                page = 1

                ##url = 'http://apiv4.yangkeduo.com/api/turing/mall/query_cat_goods?category_id=0&type=0&sort_type=_sales&mall_id='+str(mall_id)+'&page_no='+str(page)+'&page_size=500'
                # url = 'http://apiv3.yangkeduo.com/api/turing/mall/query_cat_goods?category_id=0&type=0&mall_id='+str(mall_id)+'&page_no='+str(page)+'&page_size=500&sort_type=_sales&anticontent='+self.anti_content+'&pdduid='+str(self.log_user_id)
                url = self.build_url(goods_id, page)
                meta = {'page': page, 'goods_id': goods_id, 'review_list': review_list}
                yield scrapy.Request(url, meta=meta, callback=self.parse)

    def parse(self, response):
        pass
        review_list = response.meta['review_list']  ##产品集合
        goods_id = response.meta['goods_id']  ##店铺ID
        page = response.meta['page']  ##每返回一次页面数据 记录页数

        goods_reviews = response.body.decode('utf-8')  ##bytes转换为str
        goods_reviews = json.loads(goods_reviews)

        if 'data' not in goods_reviews.keys() or len(goods_reviews['data']) == 0:
            self.ssdb.hdel(self.hash_name, goods_id)
            if review_list:
                item = GoodsSalesItem()
                item['goods_list'] = review_list
                item['mall_id'] = goods_id
                yield item
        else:
            review_list = review_list + goods_reviews['data']  ##合并评论列表
            page += 1
            url = self.build_url(goods_id, page)
            meta = {'page': page, 'goods_id': goods_id, 'review_list': review_list}
            yield scrapy.Request(url, meta=meta, callback=self.parse)

    def build_url(self, goods_id, page=1, page_size=20):
        # url = 'http://apiv3.yangkeduo.com/api/turing/mall/query_cat_goods?category_id=0&type=0&mall_id='+str(mall_id)+'&page_no='+str(page)+'&page_size='+str(page_size)+'&sort_type=_sales&anticontent='+pdd_sign+'&pdduid='+str(self.get_uid())
        url = 'http://apiv3.yangkeduo.com/reviews/' + str(goods_id) + '/list?page=' + str(page) + '&size=' + str(
            page_size) + '&pdduid=0'
        return url