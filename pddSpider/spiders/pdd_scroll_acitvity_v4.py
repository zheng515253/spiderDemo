# -*- coding: utf-8 -*-
# 首页滚动栏活动信息
import logging
import os

import scrapy
import json, time, sys, random, urllib, pyssdb, re
from spider.items import CategoryItem
from urllib import parse as urlparse


class PddScrollActivityNewSpider(scrapy.Spider):
    """ 活动app接口版本 """
    name = 'pdd_scroll_activity_v4'
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {'spider.middlewares.ProxyMiddleware': 100},
        # 'LOG_FILE':'',
        # 'LOG_LEVEL':'DEBUG',
        # 'LOG_ENABLED':True,
        'DOWNLOAD_TIMEOUT': 5,
        'RETRY_TIMES': 10,
    }

    def start_requests(self):
        yield scrapy.Request(url='https://api.yangkeduo.com/api/alexa/homepage/hub?list_id=3482f3237f&platform=0&support_formats=1&client_time=' + str(int(time.time()*1000)), headers=self.make_headers())

    def parse(self, response):
        """ 获取首页活动信息"""
        body = response.body.decode("utf-8")
        result = json.loads(body)

        # 处理首页下拉商品中的活动
        logging.debug(json.dumps({'result': result}))
        list_activity = self.dict_get(result, 'crossSlideList', None)
        logging.debug(json.dumps({'list_activity': list_activity}))
        banner = self.dict_get(result, 'carouselData', None)
        logging.debug(json.dumps({'banner': banner}))
        if list_activity:
            subject_list = []
            subject_info = self.build_subject_info(71, "首页商品", "首页商品", [71], 71, 1)
            subject_list.append(subject_info)
            a = 0
            for i in list_activity:
                subject_list_id = re.findall(r"'brand_id': '\d+'", str(i))
                subject_id_list = list(set(subject_list_id))
                a += 1
                logging.debug(json.dumps({'brand_id': subject_list_id}))
                if subject_id_list:
                    if len(subject_id_list) == 1:
                        name = self.dict_get(i, "subject", None)
                        subject_id = self.dict_get(i, "subject_id", None)
                        subject_info = self.build_subject_info(subject_id, name, name, [subject_id], 72, 1, a)
                        subject_list.append(subject_info)
                        logging.debug(json.dumps({'subject_info_home_1': subject_info}))
                        self.save_log(json.dumps({"subject_info_home_1": subject_info}))
                    else:
                        b = 0
                        for i in i["subject_list"]:
                            b += 1
                            subject_id = i['p_rec']["brand_id"]
                            name = i["name"]
                            subject_info = self.build_subject_info(subject_id, name, name, [subject_id], 72, 1, a, b)
                            subject_list.append(subject_info)
                            logging.debug(json.dumps({'subject_info_home_2': subject_info}))
                            self.save_log(json.dumps({"subject_info_home_2": subject_info}))

            item = CategoryItem()
            logging.debug(json.dumps({'subject_list_home': subject_list}))
            self.save_log(json.dumps({'subject_list_home': subject_list}))
            item['cat_list'] = subject_list
            yield item

    def categray_parse(self, response):
        # 首页固定分类活动
        result_str = response.body.decode('utf-8')
        result_dict = json.loads(result_str)
        active_list = self.dict_get(result_dict, 'icon_set', None)
        if len(active_list) > 0:
            list_subject = []
            for i in active_list:
                if i["id"] in [1, 164, 130, 42, 41]:
                    list_subject.append(i)
            for i in list_subject:
                subject_id = i["id"]
                path = i["title"]
                link_url = i['link']
                headers = self.make_headers()
                if subject_id == 1:
                    url = link_url
                else:
                    url = "https://mobile.yangkeduo.com/" + link_url
                function = ''
                meta = {'path_id': [subject_id], 'path': [path]}
                if subject_id == 1:  # 限时秒杀活动 app
                    subject_list = []
                    subject_dic = {"ongoing": '100', "future": '101', "more": '102',
                                   "brand_top": '103'}  # 100代表正在抢购，101代表马上抢购，102代表明日预告，103品牌秒杀
                    a = 1
                    for name, subject_id in subject_dic.items():
                        path = [name]
                        path_id = [subject_id]
                        subject = self.build_subject_info(subject_id, name, path, path_id, 11, 2, a)
                        a += 1
                        subject_list.append(subject)
                    meta = {'subject_list': subject_list, 'path_id': [103], 'path': ["brand_top"]}
                    url = "https://mobile.yangkeduo.com/luxury_spike.html?refer_page_name=seckill"  # 品牌秒杀
                    function = self.kill_parse_subject
                elif subject_id == 164:  # 断码清仓 app
                    function = self.short_parse_subject
                elif subject_id == 130:  # 品牌馆 app
                    function = self.brand_parse_subject
                    url = 'https://mobile.yangkeduo.com/sjs_brand_house_brand_day.html?subjects_id=21&scene_group=1'
                elif subject_id == 42:  # 9块9特卖   app端需要登陆
                    function = self.special_parse_subject
                elif subject_id == 41:  # 爱逛街 app
                    url = "https://api.pinduoduo.com/api/gentian/7/resource_tabs?without_mix=1&platform=1&pdduid=0"
                    function = self.shopping_parse_subject
                yield scrapy.Request(url, meta=meta, callback=function, headers=headers)

    def brand_parse_subject(self, response):
        """ 品牌馆 """
        body = response.body.decode()
        logging.debug(json.dumps({'body': body}))
        path = response.meta["path"]
        path_id = response.meta["path_id"]
        result = re.search(r'{"props".*?"https://cdn.yangkeduo.com"}', body)
        if result:
            subject_list = []
            result = json.loads(result.group())
            tabList = self.dict_get(result, 'tabList', None)
            if tabList and len(tabList) > 0:
                a = 1
                for i in tabList:
                    subject_id = str(i["web_url"])
                    subject_id = re.search(r"\d+", subject_id).group()
                    name = i['tab_name']
                    subject_info = self.build_subject_info(subject_id, name, path + [name], path_id + [subject_id], 31, 3, a)
                    a += 1
                    subject_list.append(subject_info)
                    self.save_log(json.dumps({"subject_info_brand": subject_info}))
                item = CategoryItem()
                logging.debug(json.dumps({'subject_list_brand': subject_list}))
                self.save_log(json.dumps({'subject_list_brand': subject_list}))
                item['cat_list'] = subject_list
                yield item

    def special_parse_subject(self, response):
        """ 9块9特卖 """
        subject_list = []
        path = response.meta["path"]
        path_id = response.meta["path_id"]
        body = response.body.decode()
        logging.debug(json.dumps({'body': body}))
        result = json.loads(body)
        if 'list' in result.keys() and len(result['list']) > 0:
            a = 1
            for i in result['list']:
                subject_id = i["tab_id"]
                name = i["subject"]
                subject_info = self.build_subject_info(subject_id, name, path + [name], path_id + [subject_id], 41, 4, a)
                a += 1
                subject_list.append(subject_info)
                self.save_log(json.dumps({"subject_info_special": subject_info}))
            item = CategoryItem()
            logging.debug(json.dumps({'subject_list_special': subject_list}))
            self.save_log(json.dumps({'subject_list_special': subject_list}))
            item['cat_list'] = subject_list
            yield item

    def shopping_parse_subject(self, response):
        """ 爱逛街 """
        subject_list = []
        path = response.meta["path"]
        path_id = response.meta["path_id"]
        body = response.body.decode('utf-8')
        logging.debug(json.dumps({'body': body}))
        result = json.loads(body)
        list_subject = self.dict_get(result, 'list', None)
        if list_subject:
            a = 1
            for i in list_subject:
                subject_id = i["tab_id"]
                name = i["subject"]
                subject_info = self.build_subject_info(subject_id, name, path + [name], path_id + [subject_id], 51, 5, a)
                a += 1
                subject_list.append(subject_info)
                self.save_log(json.dumps({"subject_info_shopping": subject_info}))
            item = CategoryItem()
            logging.debug(json.dumps({'subject_list_shopping': subject_list}))
            self.save_log(json.dumps({'subject_list_shopping': subject_list}))
            item['cat_list'] = subject_list
            yield item

    def short_parse_subject(self, response):
        """ 断码清仓"""
        path = response.meta["path"]
        path_id = response.meta["path_id"]
        body = response.body.decode()
        logging.debug(json.dumps({'body': body}))
        result = re.search(r'{"props".*?"https://cdn.yangkeduo.com"}', body)
        if result:
            result = json.loads(result.group())
            result = self.dict_get(result, 'filterTabList', None)
            subject_list = []
            a = 1
            if result and len(result) > 0:
                for i in result:
                    subject_id = i["id"]
                    str_i = str(i)
                    d = re.search(r"'brand_name': '\w+'", str_i).group()
                    name = re.sub(r"'brand_name':", '', d)
                    subject_info = self.build_subject_info(subject_id, name, path + [name], path_id + [subject_id], 21,
                                                           6, a)
                    a += 1
                    subject_list.append(subject_info)
                    self.save_log(json.dumps({"subject_info_short": subject_info}))
                item = CategoryItem()
                logging.debug(json.dumps({'subject_list_short': subject_list}))
                self.save_log(json.dumps({'subject_list_short': subject_list}))
                item['cat_list'] = subject_list
                yield item

    def kill_parse_subject(self, response):
        """ 限时秒杀"""
        path = response.meta["path"]
        path_id = response.meta["path_id"]
        subject_list = response.meta["subject_list"]
        self.save_log(json.dumps({"kill_subject_list": subject_list}))
        body = response.body.decode()
        logging.debug(json.dumps({'body': body}))
        result = re.search(r'{"props".*?"https://cdn.yangkeduo.com"}', body).group()
        if result:
            result = json.loads(result)
            result = self.dict_get(result, 'brandList', None)
            if result:
                a = 1
                for i in result:
                    subject_id = i["data"]["id"]
                    name = i["data"]["name"]
                    subject_info = self.build_subject_info(subject_id, name, path + [name], path_id + [subject_id], 14,
                                                           7, a)
                    a += 1
                    subject_list.append(subject_info)
                    self.save_log(json.dumps({"subject_info_kill": subject_info}))
                item = CategoryItem()
                logging.debug(json.dumps({'subject_list_kill': subject_list}))
                self.save_log(json.dumps({'subject_list_kill': subject_list}))
                item['cat_list'] = subject_list
                yield item

    def parse_subject_banner(self, response):
        """ 首页轮播"""
        subject_list = []
        body = response.body.decode('utf-8')
        logging.debug(json.dumps({'body': body}))
        result = re.search(r'{"store".*?"ssr":true}', body)
        path = response.meta['path']
        path_id = response.meta['path_id']
        if result:
            result_dict = json.loads(result.group())
            result_str = result.group()
            try:
                name = result_dict["store"]["pageTitle"]
            except Exception:
                name = ''
            f = re.findall(r'"id":\d+,"DIYGoodsIDs"', result_str)
            if not f:
                f = re.findall(r'"id":"\d+","DIYGoodsIDs"', result_str)
            subject_id_list = re.findall(r"\d+", str(f))
            a = 1
            for subject_id in subject_id_list:
                subject_info = self.build_subject_info(subject_id, name, path + [name], path_id + [subject_id], 61, 8,
                                                       a)
                a += 1
                subject_list.append(subject_info)
                self.save_log(json.dumps({"subject_info_banner": subject_info}))
            item = CategoryItem()
            logging.debug(json.dumps({'subject_list_banner': subject_list}))
            self.save_log(json.dumps({'subject_list_banner': subject_list}))
            item['cat_list'] = subject_list
            yield item

    '''通过url获取subject_id'''

    def get_subject_id(self, link_url):
        url_arr = urlparse.urlparse(link_url)
        url_arr = urlparse.parse_qs(url_arr.query)
        if url_arr:
            keys = url_arr.keys()
            if "id" in keys:
                subject_id = int(url_arr['id'][0])

            elif "subject_id" in keys:
                subject_id = int(url_arr['subject_id'][0])
            else:
                return False
        else:
            return False
        return {'subject_id': subject_id}

    '''生成活动信息'''
    def build_subject_info(self, subject_id, title, path, path_id, api_type, type_1, type_2=0, type_3=0):
        info = {'subject_id': subject_id, 'name': title, 'path': path, 'type_1': type_1, "api_type": api_type,
                'type_2': type_2, 'type_3': type_3, 'path_id': path_id}
        return info

    '''生成headers头信息'''
    def make_headers(self):
        headers = {
            'User-Agent': 'android Mozilla/5.0 (Linux; Android 6.0; 4G Build/MRA58K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/55.0.2883.91 Mobile Safari/537.36  phh_android_version/4.51.0 phh_android_build/deb7f0aa693a9c2817697e7d4ce2f8132839d0eb phh_android_channel/qihu360',
            "Referer": "Android",
            "X-PDD-QUERIES": "width=720&height=1356&net=1&brand=4G&model=4G&osv=6.0&appv=4.51.0&pl=2",
            "ETag": "LRruqcTa",
            "Content-Type": "application/json;charset=UTF-8",
            "p-appname": "pinduoduo",
            "PDD-CONFIG": "00102"
        }

        return headers

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

    def save_log(self, content):
        date = time.strftime('%Y-%m-%d')
        file_path = '/data/spider/logs/activity_scroll_log'
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        content = content + ","
        file_name = file_path + '/' + date + ".log"
        with open(file_name, 'a+') as f:
            f.write(content + "\r\n")
