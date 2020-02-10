# -*- coding: utf-8 -*-
# 获取拼多多活动下的产品列表
import logging
import os

import scrapy
import json, time, sys, random, pyssdb, re
from spider.items import CategoryGoodsItem
from urllib import parse as urlparse


class PddActivityGoods(scrapy.Spider):
    name = 'pdd_activity_goods_v1'

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
        hash_names = ['pdd_scroll_activity_v1']
        for hash_name in hash_names:
            subject_data = ssdb_class.hgetall(hash_name)
            if subject_data:
                for subject_info in subject_data:
                    subject_info = subject_info.decode('utf-8')
                    try:
                        subject_info = eval(subject_info)
                    except Exception:
                        subject_info = None
                    if type(subject_info) == dict:
                        self.save_log(json.dumps({'subject_get_info': subject_info}))
                        subject_id = subject_info['subject_id']
                        url = ""
                        meta = {"subject_info": subject_info}
                        logging.debug(json.dumps({"meta": meta, "subject_id": subject_id, "api_type": subject_info["api_type"]}))
                        if subject_info["api_type"] == 11 and subject_info["name"] == "ongoing":  # 秒杀活动
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/spike/v6/list/ongoing?'
                        elif subject_info["api_type"] == 11 and subject_info["name"] == "future":  # 秒杀活动
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/spike/v4/list/future?'
                        elif subject_info["api_type"] == 11 and subject_info["name"] == "more":  # 秒杀活动
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/spike/v4/list/more?'
                        elif subject_info["api_type"] == 11 and subject_info["name"] == "brand_top":  # 品牌秒杀秒杀
                            url = "https://mobile.yangkeduo.com/proxy/api/api/spike/v4/list/brand_top?"
                        elif subject_info["api_type"] == 14:  # 品牌秒杀子活动 tab_id
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/spike/v4/brand/tab_goods?spike_brand_id=' + str(
                                subject_id)
                        elif subject_info["api_type"] == 16:  # 品牌秒杀子活动 by_goods
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/spike/v4/brand/tab_goods?spike_brand_id=5265&spike_brand_id=' + str(
                                subject_id)
                        elif subject_info["api_type"] == 21:  # 断码清仓
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/milk/clearance_sale/goods_list?tab_id=' + str(
                                subject_id)
                        elif subject_info["api_type"] == 31:  # 品牌馆
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/gentian/15/resource_goods?tab_id=' + str(
                                subject_id)
                        elif subject_info["api_type"] == 41:  # 9块9特卖
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/gentian/2/resource_goods?tab_id=' + str(
                                subject_id)
                        elif subject_info["api_type"] == 51:  # 爱逛街
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/george/resource_goods/query_list?resource_type=7&page_size=400&tab_id=' + str(
                                subject_id)
                        elif subject_info["api_type"] == 61:  # 首页活动商品
                            url = 'https://mobile.yangkeduo.com/proxy/api/api/alexa/v1/goods?'
                        for page in range(1, 500):
                            meta["page"] = page
                            yield scrapy.Request(url=url + "&page=" + str(page) + "&page_size=500", headers=self.make_headers(), callback=self.parse,
                                                 dont_filter=True, meta=meta)

    def parse(self, response):
        result = json.loads(response.body.decode('utf-8'))
        logging.debug(json.dumps({"result": result, "meta": response.meta}))
        subject_info = response.meta['subject_info']
        page = int(response.meta['page'])
        rank = (page - 1) * 500 + 1
        api_type = subject_info["api_type"]
        goods_list = []
        if api_type in [11, 14, 15]:  # items  限时秒杀
            if "item" in result.keys() and len(result["items"]) > 0:
                for i in result["items"]:
                    goods_info = self.get_kill_goods_info(subject_info, rank, i["data"])
                    goods_list.append(goods_info)
                    self.save_log(json.dumps({'goods_info_kill': goods_info}))
            else:
                return None
        if api_type in [16]:  # result
            if "result" in result.keys() and len(result["result"]) > 0:
                for i in result["result"]:
                    goods_info = self.get_goods_info(subject_info, rank, i)
                    goods_list.append(goods_info)
                    self.save_log(json.dumps({'goods_info_others': goods_info}))
            else:
                return None
        if api_type in [21]:  # list   断码清仓
            if "list" in result.keys() and len(result["list"]) > 0:
                for i in result["list"]:
                    goods_info = self.get_goods_info(subject_info, rank, i)
                    goods_list.append(goods_info)
                    self.save_log(json.dumps({'goods_info_short': goods_info}))
            else:
                return None
        if api_type in [31, 41, 61]:  # goods_list   # 品牌馆  9块9特卖
            if "goods_list" in result.keys() and len(result["goods_list"]) > 0:
                for i in result["goods_list"]:
                    goods_info = self.get_goods_info(subject_info, rank, i)
                    goods_list.append(goods_info)
                    self.save_log(json.dumps({'goods_info_brand': goods_info}))
            else:
                return None
        if api_type in [51]:  # result/goods_list  爱逛街
            if "result" in result.keys() and len(result["result"]["goods_list"]) > 0:
                for i in result["result"]["goods_list"]:
                    goods_info = self.get_shopping_goods_info(subject_info, rank, i)
                    goods_list.append(goods_info)
                    self.save_log(json.dumps({'goods_info_shopping': goods_info}))
            else:
                return None

        item = CategoryGoodsItem()
        logging.debug(json.dumps({'goods_list': goods_list, "goods_len": len(goods_list)}))
        self.save_log(json.dumps({'goods_list_all': goods_list, "goods_len": len(goods_list)}))
        item['goods_lists'] = goods_list
        yield item

    def get_goods_info(self, subject_info, rank, i):
        """ 获取goods_info"""
        return {"goods_id": i["goods_id"], "price": i["group"]["price"], "sale": i["cnt"], "rank": rank,
                "subject_id": subject_info["subject_id"],
                "type": subject_info["type"], "goods_name": i['goods_name']}

    def get_shopping_goods_info(self,  subject_info, rank, i):
        return {"goods_id": i["goods_id"], "price": i["price"], "sale": i["cnt"], "rank": rank,
                "subject_id": subject_info["subject_id"],
                "type": subject_info["type"], "goods_name": i['goods_name']}

    def get_kill_goods_info(self,  subject_info, rank, i):
        return {"goods_id": i["goods_id"], "price": i["price"], "sale": i["sales_tip"], "rank": rank,
                "subject_id": subject_info["subject_id"],
                "type": subject_info["type"], "goods_name": i['goods_name']}

    '''获取产品信息'''
    def parse_goods_info(self, response):
        goods_info = response.meta['goods_info']
        content = response.body.decode('utf-8')
        a = re.search('window\.rawData= (.*)\;\s*\<\/script\>', content)
        if a:
            content = json.loads(a.group(1))
            if 'goods' not in content.keys():
                return False
            goods = content['goods']
            goods_info['price'] = goods['minOnSaleGroupPrice']

        item = CategoryGoodsItem()
        item['goods_lists'] = [goods_info]
        # print(len(goods_lists), cat_id)
        yield item

    '''生成headers头信息'''

    def make_headers(self):
        chrome_version = str(random.randint(59, 63)) + '.0.' + str(random.randint(1000, 3200)) + '.94'
        headers = {
            "Host": "mobile.yangkeduo.com",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Referer": "http://yangkeduo.com/goods.html?goods_id=442573047&from_subject_id=935&is_spike=0&refer_page_name=subject&refer_page_id=subject_1515726808272_1M143fWqjQ&refer_page_sn=10026",
            "Connection": "keep-alive",
            'cookie': 'api_uid=rBQ5vlzBpOVVmAXVEzAbAg==; ua=Mozilla%2F5.0%20(Windows%20NT%2010.0%3B%20Win64%3B%20x64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F72.0.3626.109%20Safari%2F537.36; webp=1; _nano_fp=Xpdyn5gbn0T8l0Tbn9_wNR__G8~FgcKa0lATgz4y; msec=1800000; rec_list_mall_bottom=rec_list_mall_bottom_1MX53n; goods_detail=goods_detail_V2l30O; goods_detail_mall=goods_detail_mall_3vdTRG; JSESSIONID=ED2FEBAC94D04AA54FC09EBEFBF0F58C; promotion_subject=promotion_subject_x0psf8; rec_list_index=rec_list_index_pDVkjB',
            "Upgrade-Insecure-Requests": 1,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/' + chrome_version + ' Safari/537.36',
        }

        ip = str(random.randint(100, 200)) + '.' + str(random.randint(1, 255)) + '.' + str(
            random.randint(1, 255)) + '.' + str(random.randint(1, 255))
        headers['CLIENT-IP'] = ip
        headers['X-FORWARDED-FOR'] = ip
        return headers

    '''生成拼多多产品排名数据格式'''

    def build_goods_rank_info(self, goods_id, subject_id, activity_type, rank, price):
        return {'goods_id': goods_id, 'subject_id': subject_id, 'type': activity_type, 'rank': rank, 'price': price}



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