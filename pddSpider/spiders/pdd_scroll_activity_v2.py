# -*- coding: utf-8 -*-
# 首页滚动栏活动信息
import logging
import os

import scrapy
import json, time, sys, random, urllib, pyssdb, re
from spider.items import CategoryItem
from urllib import parse as urlparse


##import mq.mq as mq
##import ssdb.ssdbapi

class PddScrollActivityNewSpider(scrapy.Spider):
    name = 'pdd_scroll_activity_v2'
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

        # 处理轮播活动
        banner = self.dict_get(result, 'carouselData', None)
        if len(banner) > 0:
            path = ['首页滚动banner']
            for i in banner:
                title = i["title"]
                if not title:
                    title = '商品'
                end_url = i["forwardURL"]
                subject = self.get_subject_type_id(end_url)
                if not subject:
                    continue
                subject_id = subject["subject_id"]
                type = subject["type"]
                headers = self.make_headers()
                new_url = "https://mobile.yangkeduo.com/" + end_url
                meta = {'path': path + [title], 'path_id': [subject_id], 'url_type': type}
                yield scrapy.Request(new_url, meta=meta, callback=self.parse_subject_banner, headers=headers,
                                     dont_filter=True, errback=self.errback_httpbin)

        # 处理首页下拉商品中的活动
        list_activity = self.dict_get(result, 'crossSlideList', None)
        if list_activity:
            subject_list = []
            subject_info = self.build_subject_info(71, "首页商品", "首页商品", [71], 71, 1)
            subject_list.append(subject_info)
            a = 0
            for i in list_activity:
                subject_list_id = re.findall(r"'brand_id': '\d+'", str(i))
                subject_id_list = list(set(subject_list_id))
                a += 1
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
                            if not subject_id:
                                subject_id = 0
                            name = i["name"]
                            if not name:
                                name = '商品' + str(b)
                            subject_info = self.build_subject_info(subject_id, name, name, [subject_id], 72, 1, a,
                                                                   b)
                            subject_list.append(subject_info)
                            logging.debug(json.dumps({'subject_info_home_2': subject_info}))
                            self.save_log(json.dumps({"subject_info_home_2": subject_info}))
            item = CategoryItem()
            item['cat_list'] = subject_list
            yield item

    def parse_subject_banner(self, response):
        """ 首页轮播"""
        subject_list = []
        body = response.body.decode('utf-8')
        result = re.search(r'{"store".*?"ssr":true}', body)
        path = response.meta['path']
        path_id = response.meta['path_id']
        url_type = response.meta["url_type"]
        item = CategoryItem()
        if result:
            result_str = result.group()

            if url_type in [1, 2]:
                name = '轮播活动' + str(1) + str(url_type)
                subject_id = 0
                subject_info = self.build_subject_info(subject_id, name, path + [name], path_id + [subject_id], 61, 7,
                                                       url_type, 1)
                subject_list.append(subject_info)
                self.save_log(json.dumps({"subject_info_banner" + str(url_type): subject_info}))

            if url_type in [4, 6, 12]:
                f = re.findall(r'"id":\d+,"DIYGoodsIDs"', result_str)
                if not f:
                    f = re.findall(r'"id":"\d+","DIYGoodsIDs"', result_str)
                subject_id_list = re.findall(r"\d+", str(f))
                a = 1
                for subject_id in subject_id_list:
                    name = '轮播活动' + str(2) + str(a)
                    subject_info = self.build_subject_info(subject_id, name, path + [name], path_id + [subject_id], 62,
                                                           7, url_type, a)
                    a += 1
                    subject_list.append(subject_info)
                    self.save_log(json.dumps({"subject_info_banner_" + str(url_type): subject_info}))

            if url_type == 17:  # 登陆接口
                pass

            if url_type == 20:
                name_list = re.findall(r'"id":\d+,"DIYGoodsIDs"', result_str)
                if not name_list:
                    name_list = re.findall(r'"id":"\d+","DIYGoodsIDs"', result_str)
                id_list = re.findall(r'\d+', str(name_list))
                if id_list:
                    id_list = list(set(id_list))
                else:
                    id_list = []
                if len(id_list) > 1:
                    a = 0
                    for id in id_list:
                        a += 1
                        name = "轮播活动" + str(3) + str(a)
                        subject_info = self.build_subject_info(id, name, path + [name], path_id + [id], 63, 7, url_type,
                                                               a)
                        subject_list.append(subject_info)
                        self.save_log(json.dumps({"subject_info_banner_" + str(url_type): subject_info}))

                else:
                    f = re.findall(r'"DIYGoodsIDs":".*?"', result_str)
                    subject_id_list = re.findall(r"\d+", str(f))
                    subject_str = ''
                    if len(id_list) == 1:
                        subject_id = id_list[0]
                    else:
                        subject_id = subject_id_list[0]
                    for i in subject_id_list:
                        i = i + ','
                        subject_str += i
                    name = "轮播活动" + str(4) + str(1)
                    subject_info = self.build_subject_20_info(subject_id, name, path + [name], path_id + [subject_id],
                                                              subject_str, 64, 7, url_type)
                    subject_list.append(subject_info)
                    self.save_log(json.dumps({"subject_info_banner_id_" + str(url_type): subject_info}))

            item['cat_list'] = subject_list
            yield item

    def get_subject_type_id(self, link_url):
        logging.debug(json.dumps({'link_url': link_url}))
        url_arr = urlparse.urlparse(link_url)
        url_arr = urlparse.parse_qs(url_arr.query)
        if url_arr:
            keys = url_arr.keys()
            if "type" or keys:
                if "type" and "id" in keys:
                    subject_id = int(url_arr['id'][0])
                    type = int(url_arr["type"][0])
                elif "type" and "subject_id" in keys:
                    subject_id = int(url_arr['subject_id'][0])
                    type = int(url_arr["type"][0])
                elif "type" and "subject_id" in keys:
                    subject_id = int(url_arr['spike_brand_id'][0])
                    type = int(url_arr["type"][0])
                else:
                    return False
            elif "subject_id" in keys:
                subject_id = int(url_arr['subject_id'][0])
                type = ''
            else:
                return False
        else:
            return False
        return {'subject_id': subject_id, "type": type}

    '''生成活动信息'''

    def build_subject_info(self, subject_id, title, path, path_id, api_type, type_1, type_2=0, type_3=0):
        info = {'subject_id': subject_id, 'name': title, 'path': path, 'type_1': type_1, "api_type": api_type,
                'type_2': type_2, 'type_3': type_3, 'path_id': path_id}
        return info

    def build_subject_20_info(self, subject_id, title, path, path_id, subject_str, api_type, type_1, type_2=0,
                              type_3=0):
        info = {'subject_id': subject_id, 'name': title, 'path': path, 'type_1': type_1, "api_type": api_type,
                'subject_str': subject_str,
                'type_2': type_2, 'type_3': type_3, 'path_id': path_id}
        return info

    '''生成headers头信息'''

    def make_headers(self):
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
