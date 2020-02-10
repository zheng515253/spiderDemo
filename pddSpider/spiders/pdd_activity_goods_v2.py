# -*- coding: utf-8 -*-
# 获取拼多多活动下的产品列表
import logging
import os

import scrapy
import json, time, sys, random, pyssdb, re
from spider.items import CategoryGoodsItem
from urllib import parse as urlparse


class PddActivityGoods(scrapy.Spider):
    name = 'pdd_activity_goods_v2'

    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {'spider.middlewares.ProxyMiddleware': 100},
        'LOG_FILE': '',
        'LOG_LEVEL': 'DEBUG',
        'LOG_ENABLED': True,
        'DOWNLOAD_TIMEOUT': 5,
        'RETRY_TIMES': 10,
    }

    def start_requests(self):
        '''先从ssdb获取首页下的分类信息'''
        ssdb_class = pyssdb.Client('172.16.0.5', 8888)
        hash_names = ['pdd_scroll_activity_v2']
        for hash_name in hash_names:
            subject_data = ssdb_class.hgetall(hash_name)
            if subject_data:
                for subject_info in subject_data:
                    subject_info = subject_info.decode('utf-8')
                    self.save_log(json.dumps({'subject_begin_info': subject_info}))
                    try:
                        subject_info = eval(subject_info)
                    except Exception:
                        subject_info = None
                    if type(subject_info) == dict and subject_info["api_type"] in [11, 14, 21, 41]:
                        self.save_log(json.dumps({'subject_get_info': subject_info}))
                        subject_id = subject_info['subject_id']
                        url = ""
                        meta = {"subject_info": subject_info}
                        logging.debug(
                            json.dumps({"meta": meta, "subject_id": subject_id, "api_type": subject_info["api_type"]}))
                        if subject_info["api_type"] == 11 and subject_info["name"] == "ongoing":  # 正在秒杀
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/spike/v6/list/ongoing?'
                        elif subject_info["api_type"] == 11 and subject_info["name"] == "future":  # 即将秒杀
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/spike/v4/list/future?'
                        elif subject_info["api_type"] == 11 and subject_info["name"] == "more":  # 秒杀活动
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/spike/v4/list/more?'
                        elif subject_info["api_type"] == 11 and subject_info["name"] == "brand_top":  # 品牌秒杀秒杀
                            url = "https://mobile.yangkeduo.com/proxy/api/api/spike/v4/list/brand_top?time_type=101&list_id=R34CqQZmbD&size=20&pdduid=0&is_back=1"  # 限制size
                        elif subject_info["api_type"] == 14:  # 品牌秒杀子活动 tab_id
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/spike/v4/brand/tab_goods?spike_brand_id=' + str(
                                subject_id)
                        elif subject_info["api_type"] == 21:  # 断码清仓
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/milk/clearance_sale/goods_list?tab_id=' + str(
                                subject_id)
                        elif subject_info["api_type"] == 41:  # 9块9特卖
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/gentian/2/resource_goods?tab_id=' + str(
                                subject_id)
                        for page in range(1, 5):
                            meta["page"] = page
                            yield scrapy.Request(url=url + "&page=" + str(page) + "&page_size=500",
                                                 headers=self.make_headers(), callback=self.parse, dont_filter=True, meta=meta)

    def parse(self, response):
        result = json.loads(response.body.decode('utf-8'))
        # logging.debug(json.dumps({"result": result, "meta": response.meta}))
        subject_info = response.meta['subject_info']
        page = int(response.meta['page'])
        rank = (page - 1) * 500 + 1
        api_type = subject_info["api_type"]
        goods_list = []
        if api_type in [11]:  # items  限时秒杀
            # self.save_log(json.dumps({'goods_api_kill': api_type, 'goods_api_items_kill': result["items"]}))
            if "items" in result.keys() and len(result["items"]) > 0:
                for i in result["items"]:
                    try:
                        goods_id = i["data"]["goods_id"]
                    except Exception:
                        goods_id = None
                    if goods_id:
                        goods_info = self.get_kill_goods_info(subject_info, rank, i["data"])
                        goods_list.append(goods_info)
                        # self.save_log(json.dumps({'goods_info_' + str(api_type): goods_info}))
            else:
                return None
        if api_type in [14]:  # 品牌秒杀子活动
            # self.save_log(json.dumps({'goods_api_slow': api_type, 'goods_api_items_slow': result["result"]}))
            if "result" in result.keys() and len(result["result"]) > 0:
                for i in result["result"]:
                    goods_info = self.get_goods_info_slow(subject_info, rank, i)
                    goods_list.append(goods_info)
                    # self.save_log(json.dumps({'goods_info_' + str(api_type): goods_info}))
            else:
                return None

        if api_type in [21]:  # list   断码清仓
            if "list" in result.keys() and len(result["list"]) > 0:
                for i in result["list"]:
                    goods_info = self.get_short_goods_info(subject_info, rank, i)
                    goods_list.append(goods_info)
                    # self.save_log(json.dumps({'goods_info_' + str(api_type): goods_info}))
            else:
                return None

        if api_type in [41, 71]:  # goods_list   # 9块9特卖
            # self.save_log(json.dumps({'goods_api_brand': api_type, 'goods_api_items_brand': result["goods_list"]}))
            if "goods_list" in result.keys() and len(result["goods_list"]) > 0:
                for i in result["goods_list"]:
                    goods_info = self.get_goods_info_99(subject_info, rank, i)
                    goods_list.append(goods_info)
                    # self.save_log(json.dumps({'goods_info_' + str(api_type): goods_info}))
            else:
                return None

        if api_type in [51, 61]:  # result/goods_list  爱逛街/首页轮播
            null = None
            false = None
            # self.save_log(json.dumps({'goods_api_shopping': api_type, 'goods_api_items_shopping': result["result"]["goods_list"]}))
            if "result" in result.keys() and len(result["result"]["goods_list"]) > 0:
                for i in result["result"]["goods_list"]:
                    goods_info = self.get_shopping_goods_info(subject_info, rank, i)
                    goods_list.append(goods_info)
                    # self.save_log(json.dumps({'goods_info_' + str(api_type): goods_info}))
            else:
                return None

        item = CategoryGoodsItem()
        item['goods_lists'] = goods_list
        yield item

    def get_goods_info_99(self, subject_info, rank, i):
        """ 获取goods_info"""
        return {"goods_id": i["goods_id"], "price": i["group"]["price"], "sale": i["cnt"], "rank": rank,
                'api_type': subject_info["api_type"], 'all_quantity': i['quantity'], 'tag':  self.get_tag_list(i),
                "subject_id": subject_info["subject_id"], 'type_1': subject_info["type_1"],
                'type_2': subject_info["type_2"], 'type_3': subject_info["type_3"], "goods_name": i['goods_name']}

    def get_short_goods_info(self, subject_info, rank, i):
        """ 获取断码清仓goods_info"""
        return {"goods_id": i["goods_id"], "price": i["group"]["price"], "sale": i["cnt"], "rank": rank, 'tag':  self.get_tag_list(i),
                'quantity': i['quantity'], 'discount': i['discount'], 'api_type': subject_info["api_type"],
                "subject_id": subject_info["subject_id"], 'type_1': subject_info["type_1"],
                'type_2': subject_info["type_2"], 'type_3': subject_info["type_3"], "goods_name": i['goods_name']}

    def get_shopping_goods_info(self, subject_info, rank, i):
        return {"goods_id": i["goods_id"], "price": i["price"], "sale": i["cnt"], "rank": rank,
                'api_type': subject_info["api_type"], 'tag': self.get_tag_list(i),
                "subject_id": subject_info["subject_id"], 'type_1': subject_info["type_1"],
                'type_2': subject_info["type_2"], 'type_3': subject_info["type_3"], "goods_name": i['goods_name']}

    def get_kill_goods_info(self, subject_info, rank, i):
        return {"goods_id": i["goods_id"], "price": i["price"], "sale": i["sales_tip"], "rank": rank,
                'api_type': subject_info["api_type"], "all_quantity": i["all_quantity"], 'tag': self.get_tag_list(i),
                "sold_quantity": i["sold_quantity"], 'start_time': i['start_time'], 'coupon': i['coupon'],
                "subject_id": subject_info["subject_id"], 'type_1': subject_info["type_1"],
                'type_2': subject_info["type_2"], 'type_3': subject_info["type_3"], "goods_name": i['goods_name']}

    def get_goods_info_slow(self, subject_info, rank, i):
        """ 获取goods_info"""
        return {"goods_id": i["goods_id"], "price": i["price"], "sale": self.get_goods_sale(i["sales_tip"]), "rank": rank,
                'api_type': subject_info["api_type"], "all_quantity": i["all_quantity"],
                "sold_quantity": i["sold_quantity"], 'tag': self.get_tag_list(i), 'start_time': i['start_time'],
                "subject_id": subject_info["subject_id"], 'type_1': subject_info["type_1"],
                'type_2': subject_info["type_2"], 'type_3': subject_info["type_3"], "goods_name": i['goods_name']}

    '''生成headers头信息'''

    def make_headers(self):
        # chrome_version = str(random.randint(59, 63)) + '.0.' + str(random.randint(1000, 3200)) + '.94'
        headers = {
            'User-Agent': 'android Mozilla/5.0 (Linux; Android 6.0; 4G Build/MRA58K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/55.0.2883.91 Mobile Safari/537.36  phh_android_version/4.51.0 phh_android_build/deb7f0aa693a9c2817697e7d4ce2f8132839d0eb phh_android_channel/qihu360',
            "Referer": "Android",
            "X-PDD-QUERIES": "width=720&height=1356&net=1&brand=4G&model=4G&osv=6.0&appv=4.51.0&pl=2",
            "ETag": "LRruqcTa",
            "Content-Type": "application/json;charset=UTF-8",
            "p-appname": "pinduoduo",
            "PDD-CONFIG": "00102"
        }

        ip = str(random.randint(100, 200)) + '.' + str(random.randint(1, 255)) + '.' + str(
            random.randint(1, 255)) + '.' + str(random.randint(1, 255))
        headers['CLIENT-IP'] = ip
        headers['X-FORWARDED-FOR'] = ip
        return headers

    def get_tag_list(self, data):
        tag_list = []
        if 'display_label' in data.keys():
            display_label = data['display_label']
            tag_list.append(display_label)
        if 'discount_desc' in data.keys():
            discount_desc = data['discount_desc']
            tag_list.append(discount_desc)
        if 'ext' in data.keys():
            tag = self.dict_get(data["ext"], 'text', None)
            if tag:
                tag_list.append(tag)
        if 'tag_list' in data.keys():
            tag = self.dict_get(data["tag_list"], 'text', None)
            if tag:
                tag_list.append(tag)
        if not tag_list:
            tag_list = ''

        return tag_list

    def get_goods_sale(self, sale):
        if type(sale) == str and len(sale) > 0:
            logging.debug(json.dumps({'sale': sale}))
            if sale.__contains__('万'):
                sale = re.search(r'\d.*\d|\d', sale).group()
                logging.debug(json.dumps({"sale_1": sale}))
                sale = int(float(sale) * 10000)
            else:
                sale = re.search(r'\d.*\d|\d', sale).group()
                logging.debug(json.dumps({"sale_2": sale}))
                sale = int(sale)
        return sale

    def get_goods_id_by_url(self, goods_url):
        """ 获取链接里的数据 """
        ##拆分出URL参数
        url_arr = urlparse.urlparse(goods_url)
        url_arr = urlparse.parse_qs(url_arr.query)
        if url_arr:
            keys = url_arr.keys()
            if 'goods_id' in keys:  ##单独活动
                return url_arr['goods_id'][0]
            else:
                return False
        else:
            return False

    def dict_get(self, dict, objkey, default):
        tmp = dict
        for k, v in tmp.items():
            if k == objkey:
                return v
            else:
                if (type(v).__name__ == 'dict'):
                    ret = self.dict_get(v, objkey, default)
                    if ret is not default:
                        return ret
        return default

    def errback_httpbin(self, failure):
        request = failure.request
        response = failure.value.response
        if response.status == 403:
            return
        # headers = self.make_headers()
        # meta = {'proxy':self.proxy}
        meta = request.meta
        yield scrapy.Request(request.url, meta=meta, callback=self.parse, headers=request.headers, dont_filter=True,
                             errback=self.errback_httpbin)

    def save_log(self, content):
        date = time.strftime('%Y-%m-%d')
        file_path = '/data/spider/logs/activity_goods_log'
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        content = content
        file_name = file_path + '/' + date + ".log"
        with open(file_name, 'a+') as f:
            f.write(content + "\r\n")
