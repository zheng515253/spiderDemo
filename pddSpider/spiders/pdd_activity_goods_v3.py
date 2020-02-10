# -*- coding: utf-8 -*-
# 获取拼多多活动下的产品列表
import logging
import os

import scrapy
import json, time, sys, random, pyssdb, re
from spider.items import CategoryGoodsItem
from urllib import parse as urlparse


class PddActivityGoods(scrapy.Spider):
    name = 'pdd_activity_goods_v3'

    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {'spider.middlewares.ProxyMiddleware': 100},
        'LOG_FILE': '',
        'LOG_LEVEL': 'DEBUG',
        'LOG_ENABLED': True,
        'DOWNLOAD_TIMEOUT': 5,
        'RETRY_TIMES': 10,
    }

    def start_requests(self):
        ssdb_class = pyssdb.Client('172.16.0.5', 8888)
        hash_names = ['pdd_scroll_activity_v2']
        for hash_name in hash_names:
            subject_data = ssdb_class.hgetall(hash_name)
            if subject_data:
                for subject_info in subject_data:
                    subject_info = subject_info.decode('utf-8')
                    # self.save_log(json.dumps({'subject_begin_info': subject_info}))
                    try:
                        subject_info = eval(subject_info)
                    except Exception:
                        subject_info = None
                    if type(subject_info) == dict and subject_info["api_type"] in [51, 71, 61, 62, 63, 64, 31, 72]:
                        self.save_log(json.dumps({'subject_get_info': subject_info}))
                        subject_id = subject_info['subject_id']
                        url = ""
                        meta = {"subject_info": subject_info}
                        logging.debug(json.dumps({"meta": meta, "subject_id": subject_id, "api_type": subject_info["api_type"]}))
                        if subject_info["api_type"] == 51:  # 爱逛街
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/george/resource_goods/query_list?resource_type=7&page_size=400&tab_id=' + str(
                                subject_id)

                        elif subject_info["api_type"] == 61:  # 首页轮播
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/carnival/goods_list/list?group=cat_promotion'

                        elif subject_info["api_type"] == 62:  # 首页轮播
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/george/resource_goods/query_list?subject_id=' + str(
                                subject_id)

                        elif subject_info["api_type"] == 63:  # 首页轮播
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/george/resource_goods/query_list?subject_id=' + str(
                                subject_id)

                        elif subject_info["api_type"] == 64:  # 首页轮播
                            subject_id_str = subject_info["subject_str"]
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/george/resource_goods/query_list_by_goods_ids?goods_ids=' + subject_id_str

                        elif subject_info["api_type"] == 31:  # 品牌馆
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/gentian/15/resource_goods?tab_id=' + str(
                                subject_id)

                        elif subject_info["api_type"] == 71:  # 首页商品信息  subject_id 为71
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/alexa/v1/goods?'

                        elif subject_info["api_type"] == 72:  # 首页子活动信息
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/gentian/brand_goods?size=400&page=2&resource_type=15&brand_id=' + str(
                                subject_id)

                        for page in range(1, 5):
                            meta["page"] = page
                            yield scrapy.Request(url=url + "&page=" + str(page) + "&page_size=500",
                                                 headers=self.make_headers(), callback=self.parse,
                                                 dont_filter=True, meta=meta)

    def parse(self, response):
        result = json.loads(response.body.decode('utf-8'))
        # logging.debug(json.dumps({"result": result, "meta": response.meta}))
        subject_info = response.meta['subject_info']
        page = int(response.meta['page'])
        rank = (page - 1) * 500 + 1
        api_type = subject_info["api_type"]
        goods_list = []

        if api_type in [72]:  # list   首页子活动
            if "list" in result.keys() and len(result["list"]) > 0:
                for i in result["list"]:
                    goods_info = self.get_goods_info(subject_info, rank, i)
                    goods_list.append(goods_info)
                   # self.save_log(json.dumps({'goods_info_' + str(api_type): goods_info}))
            else:
                return None
        if api_type in [71]:  # goods_list   # 首页商品
            # self.save_log(json.dumps({'goods_api_brand': api_type, 'goods_api_items_brand': result["goods_list"]}))
            if "goods_list" in result.keys() and len(result["goods_list"]) > 0:
                for i in result["goods_list"]:
                    goods_info = self.get_goods_info(subject_info, rank, i)
                    goods_list.append(goods_info)
                    # self.save_log(json.dumps({'goods_info_' + str(api_type): goods_info}))
            else:
                return None
        if api_type in [31]:  # goods_list   # 品牌馆
            # self.save_log(json.dumps({'goods_api_brand': api_type, 'goods_api_items_brand': result["goods_list"]}))
            if "goods_list" in result.keys() and len(result["goods_list"]) > 0:
                for i in result["goods_list"]:
                    goods_info = self.get_goods_info_brand(subject_info, rank, i)
                    goods_list.append(goods_info)
                    # self.save_log(json.dumps({'goods_info_' + str(api_type): goods_info}))
            else:
                return None

        if api_type in [62, 63, 64]:  # result/goods_list  首页轮播
            null = None
            false = None
            # self.save_log(json.dumps({'goods_api_shopping': api_type, 'goods_api_items_shopping': result["result"]["goods_list"]}))
            if "result" in result.keys() and len(result["result"]["goods_list"]) > 0:
                for i in result["result"]["goods_list"]:
                    goods_info = self.get_goods_info_lunbo_1(subject_info, rank, i)
                    goods_list.append(goods_info)
                    # self.save_log(json.dumps({'goods_info_' + str(api_type): goods_info}))
            else:
                return None
        if api_type in [51]:  # result/goods_list  爱逛街
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

        if api_type in [61]:  # result/goods_list 首页轮播
            # self.save_log(json.dumps({'goods_api_shopping': api_type, 'goods_api_items_shopping': result["goods_list"]}))
            if "goods_list" in result.keys() and len(result["goods_list"]) > 0:
                for i in result["goods_list"]:
                    goods_info = self.get_goods_info_lunbo_2(subject_info, rank, i)
                    goods_list.append(goods_info)
                    # self.save_log(json.dumps({'goods_info_' + str(api_type): goods_info}))
            else:
                return None

        item = CategoryGoodsItem()
        # logging.debug(json.dumps({'goods_list': goods_list, "goods_len": len(goods_list)}))
        # self.save_log(json.dumps({'goods_list_all': goods_list, "goods_len": len(goods_list)}))
        item['goods_lists'] = goods_list
        yield item

    def get_shopping_goods_info(self, subject_info, rank, i):
        return {"goods_id": i["goods_id"], "price": i["price"], "sale": i["cnt"], "rank": rank, 'api_type': subject_info["api_type"], 'all_quantity': i['quantity'], 'tag':  self.get_tag_list(i),
                'fans_num': i['fans_num'],"subject_id": subject_info["subject_id"], 'type_1': subject_info["type_1"], 'type_2': subject_info["type_2"], 'type_3': subject_info["type_3"], "goods_name": i['goods_name']}

    def get_goods_info(self, subject_info, rank, i):
        """ 获取goods_info"""
        return {"goods_id": i["goods_id"], "price": i["group"]["price"], "sale": i["cnt"], "rank": rank,  'api_type': subject_info["api_type"], 'tag': self.get_tag_list(i),
                "subject_id": subject_info["subject_id"], 'type_1': subject_info["type_1"], 'type_2': subject_info["type_2"], 'type_3': subject_info["type_3"], "goods_name": i['goods_name']}

    def get_goods_info_brand(self, subject_info, rank, i):
        """ 获取goods_info"""
        return {"goods_id": i["goods_id"], "price": i["group"]["price"], "sale": i["cnt"], "rank": rank,  'api_type': subject_info["api_type"], 'all_quantity': i['quantity'], 'tag': self.get_tag_list(i),
                "subject_id": subject_info["subject_id"], 'type_1': subject_info["type_1"], 'type_2': subject_info["type_2"], 'type_3': subject_info["type_3"], "goods_name": i['goods_name']}

    def get_goods_info_lunbo_1(self, subject_info, rank, i):
        return {"goods_id": i["goods_id"], "price": i["price"], "sale": i["cnt"], "rank": rank, 'api_type': subject_info["api_type"], 'all_quantity': i['quantity'], 'tag':  self.get_tag_list(i),
                "subject_id": subject_info["subject_id"], 'type_1': subject_info["type_1"], 'type_2': subject_info["type_2"], 'type_3': subject_info["type_3"], "goods_name": i['goods_name']}

    def get_kill_goods_info(self, subject_info, rank, i):
        return {"goods_id": i["goods_id"], "price": i["price"], "sale": i["sales_tip"], "goods_name": i['goods_name'], 'start_time': i['start_time'], 'quantity': i['quantity'], 'api_type': subject_info["api_type"],
                "subject_id": subject_info["subject_id"], 'type_1': subject_info["type_1"], 'type_2': subject_info["type_2"], 'type_3': subject_info["type_3"],"rank": rank }

    def get_goods_info_slow(self, subject_info, rank, i):
        """ 获取goods_info"""
        return {"goods_id": i["goods_id"], "price": i["price"], "sale": i["sales_tip"], "rank": rank, 'tag': self.get_tag_list(i),
                "subject_id": subject_info["subject_id"], 'type_1': subject_info["type_1"], 'api_type': subject_info["api_type"],
                'type_2': subject_info["type_2"], 'type_3': subject_info["type_3"], "goods_name": i['goods_name']}
    def get_goods_info_lunbo_2(self, subject_info, rank, i):
        """ 获取goods_info"""
        price = 0
        if 'group_price' in i["ext"].keys():
            price = i["ext"]["group_price"]
        return {"goods_id": i["goods_id"], "price": price, "sale": i["sales"], "rank": rank, 'api_type': subject_info["api_type"], 'tag': self.get_tag_list(i),
                "subject_id": subject_info["subject_id"], 'type_1': subject_info["type_1"], 'type_2': subject_info["type_2"], 'type_3': subject_info["type_3"], "goods_name": i['goods_name']}

    def make_headers(self):
        """ 生成headers信息"""
        chrome_version = str(random.randint(59, 63)) + '.0.' + str(random.randint(1000, 3200)) + '.94'
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
        if 'goods_tags' in data.keys() and data['goods_tags'] == list():
            for i in data['goods_tags']:
                tag_list.append(i['content'])
        if not tag_list:
            tag_list = ''
        return tag_list

    def get_goods_sale(self, sale):
        if type(sale) == str:
            if sale.__contains__('万'):
                sale = re.search(r'\d.*\d|\d*', sale).group()
                sale = int(float(sale) * 10000)
        return sale

    '''获取链接里面的goods_id'''
    def get_goods_id_by_url(self, goods_url):
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
