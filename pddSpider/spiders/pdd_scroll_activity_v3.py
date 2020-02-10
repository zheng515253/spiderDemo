# -*- coding: utf-8 -*-
# 首页滚动栏活动信息
import logging
import os

import scrapy
import json, time, sys, random, urllib, pyssdb, re
from spider.items import CategoryItem
from urllib import parse as urlparse


class PddScrollActivityNewSpider(scrapy.Spider):
    name = 'pdd_scroll_activity_v3'
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {'spider.middlewares.ProxyMiddleware': 100},
        # 'LOG_FILE':'',
        # 'LOG_LEVEL':'DEBUG',
        # 'LOG_ENABLED':True,
        'DOWNLOAD_TIMEOUT': 5,
        'RETRY_TIMES': 10,
    }

    kill_show_start_time = ''
    kill_show_end_time = ''

    brand_show_start_time = ''
    brand_show_end_time = ''

    shopping_show_start_time = ''
    shopping_show_end_time = ''

    url_list = [
        {"url_type": 1,
         'url': 'https://mobile.yangkeduo.com/proxy/api/api/carnival/image_list/v2?types[]=spike_floating_window'},
        {"url_type": 2,
         'url': 'https://mobile.yangkeduo.com/proxy/api/api/carnival/image_list/v2?types[]=floating_window'},
        {"url_type": 3,
         'url': 'https://api.pinduoduo.com/api/carnival/image_list/v2?types[]=floating_window&pdduid=0'},
        {"url_type": 4, 'url': 'https://mobile.yangkeduo.com/'},
    ]

    def start_requests(self):
        headers = self.make_headers()
        function = ''
        meta = {}
        for i in self.url_list:
            meta['type'] = i['url_type']
            url = i['url']
            if i['url_type'] == 1:
                function = self.parse_time
            if i['url_type'] == 2:
                function = self.parse_time
            if i['url_type'] == 3:
                function = self.parse_time
            if i['url_type'] == 4:
                function = self.parse
            yield scrapy.Request(url=url, headers=headers, meta=meta, callback=function)

    def parse_time(self, response):
        type = response.meta['type']
        result = response.body.decode('utf-8')
        result = json.loads(result)
        if 'carnival_images' in result.keys() and len(result['carnival_images']) > 0:
            for i in result['carnival_images']:
                if type == 1:
                    self.kill_show_end_time = i['show_end_time']
                    self.kill_show_start_time = i['show_start_time']

                elif type == 2:
                    self.brand_show_end_time = i['show_end_time']
                    self.brand_show_start_time = i['show_start_time']

                elif type == 3:
                    self.shopping_show_end_time = i['show_end_time']
                    self.shopping_show_start_time = i['show_start_time']

    def parse(self, response):
        """ 获取首页活动信息"""
        body = response.body.decode("utf-8")
        result = re.search(r'{"pageProps".*?null}}', body)
        if not result:
            result = re.search(r'{"props".*?206]}', body)
        if not result:
            result = re.search(r'{"props".*?355]}', body)
        if not result:
            result = re.search(r'{"props".*?344]}', body)
        result = json.loads(result.group())

        # 首页固定分类活动
        active_list = self.dict_get(result, 'quickEntrances', None)
        if len(active_list) > 0:
            list_subject = []
            for i in active_list:
                if i["id"] in [36, 134, 162, 115, 41, 126]:
                    list_subject.append(i)
            for i in list_subject:
                subject_id = i["id"]
                path = i["title"]
                link_url = i['link']
                headers = self.make_headers()
                url = "https://mobile.yangkeduo.com/" + link_url
                function = ''
                meta = {'path_id': [subject_id], 'path': [path]}
                if subject_id == 36:  # 限时秒杀活动
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
                    url = "https://mobile.yangkeduo.com/luxury_spike.html?refer_page_name=seckill&refer_page_id=10025_1556522752181_6xi8f7UAgH&refer_page_sn=10025"  # 品牌秒杀
                    function = self.kill_parse_subject
                elif subject_id == 134:  # 断码清仓
                    function = self.short_parse_subject
                elif subject_id == 162 or subject_id == 126:  # 品牌馆
                    function = self.brand_parse_subject
                elif subject_id == 115:  # 9块9特卖
                    function = self.special_parse_subject
                elif subject_id == 41:  # 爱逛街
                    url = "https://api.pinduoduo.com/api/gentian/7/resource_tabs?without_mix=1&platform=1&pdduid=0"
                    function = self.shopping_parse_subject
                logging.debug(json.dumps({'subject_id': subject_id}))
                yield scrapy.Request(url, meta=meta, callback=function, headers=headers)

    def brand_parse_subject(self, response):
        """ 品牌馆 """
        body = response.body.decode()
        path = response.meta["path"]
        path_id = response.meta["path_id"]
        result = re.search(r'{"props".*?"https://cdn.yangkeduo.com"}', body)
        if result:
            subject_list = []
            result = json.loads(result.group())
            tabList = self.dict_get(result, 'tabList', None)
            if tabList and len(tabList) > 0:
                a = 0
                for i in tabList:
                    a += 1
                    subject_id = str(i["web_url"])
                    subject_id = re.search(r"\d+", subject_id).group()
                    name = i["tab_name"]
                    subject_info = self.build_subject_info(subject_id, name, path + [name], path_id + [subject_id], 31,
                                                           3, a)
                    subject_list.append(subject_info)
                    self.save_log(json.dumps({"subject_info_brand": subject_info}))
                item = CategoryItem()
                item['cat_list'] = subject_list
                yield item

    def special_parse_subject(self, response):
        """ 9块9特卖 """
        subject_list = []
        path = response.meta["path"]
        path_id = response.meta["path_id"]
        body = response.body.decode()
        result = re.search(r'{"props".*?"https://cdn.yangkeduo.com"}', body)
        if result:
            result = json.loads(result.group())
            tab_list = self.dict_get(result, 'tabList', None)
            if tab_list:
                a = 0
                for i in tab_list:
                    a += 1
                    subject_id = i["tab_id"]
                    name = i["subject"]
                    subject_info = self.build_subject_info_brand_time(subject_id, name, path + [name],
                                                                      path_id + [subject_id], 41, 4, a)
                    subject_list.append(subject_info)
                    self.save_log(json.dumps({"subject_info_special": subject_info}))
                item = CategoryItem()
                item['cat_list'] = subject_list
                yield item

    def shopping_parse_subject(self, response):
        """ 爱逛街 """
        subject_list = []
        path = response.meta["path"]
        path_id = response.meta["path_id"]
        body = response.body.decode('utf-8')
        result = json.loads(body)
        list_subject = self.dict_get(result, 'list', None)
        if list_subject:
            a = 0
            for i in list_subject:
                a += 1
                subject_id = i["tab_id"]
                name = i["subject"]
                subject_info = self.build_subject_info_shopping(subject_id, name, path + [name], path_id + [subject_id],
                                                                51, 5, a)
                subject_list.append(subject_info)
                self.save_log(json.dumps({"subject_info_shopping": subject_info}))
            item = CategoryItem()
            item['cat_list'] = subject_list
            yield item

    def short_parse_subject(self, response):
        """ 断码清仓"""
        path = response.meta["path"]
        path_id = response.meta["path_id"]
        body = response.body.decode()
        result = re.search(r'{"props".*?"https://cdn.yangkeduo.com"}', body)
        if result:
            result = json.loads(result.group())
            result = self.dict_get(result, 'filterTabList', None)
            subject_list = []
            a = 0
            if result and len(result) > 0:
                for i in result:
                    a += 1
                    subject_id = i["id"]
                    name = i['tabName']
                    subject_info = self.build_subject_info_brand_time(subject_id, name, path + [name],
                                                                      path_id + [subject_id], 21,
                                                                      6, a)
                    subject_list.append(subject_info)
                    self.save_log(json.dumps({"subject_info_short": subject_info}))
                item = CategoryItem()
                item['cat_list'] = subject_list
                yield item

    def kill_parse_subject(self, response):
        """ 限时秒杀"""
        path = response.meta["path"]
        path_id = response.meta["path_id"]
        subject_list = response.meta["subject_list"]
        body = response.body.decode()
        result = re.search(r'{"props".*?"https://cdn.yangkeduo.com"}', body).group()
        if result:
            result = json.loads(result)
            result = self.dict_get(result, 'brandList', None)
            if result:
                a = 1
                for i in result:
                    subject_id = i["data"]["id"]
                    name = i["data"]["name"]
                    subject_info = self.build_subject_info_kill_time(subject_id, name, path + [name],
                                                                     path_id + [subject_id], 14,
                                                                     2, 3, a)
                    a += 1
                    subject_list.append(subject_info)
                    self.save_log(json.dumps({"subject_info_kill": subject_info}))
                item = CategoryItem()
                item['cat_list'] = subject_list
                yield item

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


    def build_subject_info(self, subject_id, title, path, path_id, api_type, type_1, type_2=0, type_3=0):
        """ 生成活动信息 """
        info = {'subject_id': subject_id, 'name': title, 'path': path, 'type_1': type_1, "api_type": api_type,
                'type_2': type_2, 'type_3': type_3, 'path_id': path_id}
        return info

    def build_subject_info_shopping(self, subject_id, title, path, path_id, api_type, type_1, type_2=0, type_3=0):
        """ 生成爱逛街活动信息 """
        info = {'subject_id': subject_id, 'name': title, 'path': path, 'type_1': type_1, "api_type": api_type,
                 'show_start_time': self.shopping_show_start_time, 'show_end_time': self.shopping_show_end_time,
                'type_2': type_2, 'type_3': type_3, 'path_id': path_id}
        return info

    def build_subject_info_kill_time(self, subject_id, title, path, path_id, api_type, type_1, type_2=0, type_3=0):
        """ 生成限时秒杀活动信息 """
        info = {'subject_id': subject_id, 'name': title, 'path': path, 'type_1': type_1, "api_type": api_type,
                 'show_start_time': self.kill_show_start_time, 'show_end_time': self.kill_show_end_time,
                'type_2': type_2, 'type_3': type_3, 'path_id': path_id}
        return info

    def build_subject_info_brand_time(self, subject_id, title, path, path_id, api_type, type_1, type_2=0, type_3=0):
        """ 生成品牌馆活动信息 """
        info = {'subject_id': subject_id, 'name': title, 'path': path, 'type_1': type_1, "api_type": api_type,
                'show_start_time': self.brand_show_start_time,'show_end_time': self.brand_show_end_time,
                'type_2': type_2, 'type_3': type_3, 'path_id': path_id}
        return info

    def make_headers(self):
        """ 生成header信息"""
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
