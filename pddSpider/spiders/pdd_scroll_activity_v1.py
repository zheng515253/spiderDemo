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
    name = 'pdd_scroll_activity_v1'
    custom_settings = {
        # 'DOWNLOADER_MIDDLEWARES': {'spider.middlewares.ProxyMiddleware': 100},
        # 'LOG_FILE':'',
        # 'LOG_LEVEL':'DEBUG',
        # 'LOG_ENABLED':True,
        'DOWNLOAD_TIMEOUT': 5,
        'RETRY_TIMES': 10,
    }

    def start_requests(self):
        headers = self.make_headers()
        yield scrapy.Request(url="https://mobile.yangkeduo.com/", headers=headers)

    def parse(self, response):
        """ 获取首页活动信息"""
        body = response.body.decode("utf-8")
        result = re.search(r'{"props".*?344]}', body).group()
        result = json.loads(result)


        # 首页固定分类活动
        active_list = self.dict_get(result, 'quickEntrances', None)
        if len(active_list) > 0:
            list_subject = []
            for i in active_list:
                if i["id"] in [36, 134, 162, 115, 41]:
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
                    subject_dic = {"ongoing": '100', "future": '101', "more": '102', "brand_top": '103'}  # 100代表正在抢购，101代表马上抢购，102代表明日预告，103品牌秒杀
                    for name, subject_id in subject_dic.items():
                        subject = self.build_subject_info(subject_id, name, name, subject_id, 11)
                        subject_list.append(subject)
                    meta = {'subject_list': subject_list, 'path_id': [103], 'path': ["brand_top"]}
                    url = "https://mobile.yangkeduo.com/luxury_spike.html?refer_page_name=seckill&refer_page_id=10025_1556522752181_6xi8f7UAgH&refer_page_sn=10025"  # 品牌秒杀
                    function = self.kill_parse_subject
                elif subject_id == 134:  # 断码清仓
                    function = self.short_parse_subject
                elif subject_id == 162:  # 品牌馆
                    function = self.brand_parse_subject
                elif subject_id == 115:  # 9块9特卖
                    function = self.special_parse_subject
                elif subject_id == 41:  # 爱逛街
                    url = "https://api.pinduoduo.com/api/gentian/7/resource_tabs?without_mix=1&platform=1&pdduid=0"
                    function = self.shopping_parse_subject
                yield scrapy.Request(url, meta=meta, callback=function, headers=headers)


    def brand_parse_subject(self, response):
        """ 品牌馆 """
        body = response.body.decode()
        path = response.meta["path"]
        path_id = response.meta["path_id"]
        result = re.search(r'{"props".*?"https://cdn.yangkeduo.com"}', body).group()
        if result:
            subject_list = []
            result = json.loads(result)
            tabList = self.dict_get(result, 'tabList', None)
            if tabList and len(tabList) > 0:
                for i in tabList:
                    subject_id = str(i["web_url"])
                    subject_id = re.search(r"\d+", subject_id).group()
                    str_i = str(i)
                    name = re.search(r"'tab_name': '\w+'", str_i).group()
                    name = re.sub("'tab_name': ", "", name)
                    subject_info = self.build_subject_info(subject_id, name, path + [name], path_id + [subject_id], 31)
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
        result = re.search(r'{"props".*?"https://cdn.yangkeduo.com"}', body).group()
        if result:
            result = json.loads(result)
            tab_list = self.dict_get(result, 'tabList', None)
            if tab_list:
                for i in tab_list:
                    subject_id = i["tab_id"]
                    name = i["subject"]
                    subject_info = self.build_subject_info(subject_id, name, path + [name], path_id + [subject_id], 41)
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
        result = json.loads(body)
        logging.debug(result)
        list_subject = self.dict_get(result, 'list', None)
        if list_subject:
            for i in list_subject:
                subject_id = i["tab_id"]
                name = i["subject"]
                subject_info = self.build_subject_info(subject_id, name, path + [name], path_id + [subject_id], 51)
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
        result = re.search(r'{"props".*?"https://cdn.yangkeduo.com"}', body).group()
        logging.debug(result)
        if result:
            result = json.loads(result)
            result = self.dict_get(result, 'filterTabList', None)
            subject_list = []
            if result and len(result) > 0:
                for i in result:
                    subject_id = i["id"]
                    str_i = str(i)
                    d = re.search(r"'brand_name': '\w+'", str_i).group()
                    name = re.sub(r"'brand_name':", '', d)
                    subject_info = self.build_subject_info(subject_id, name, path + [name], path_id + [subject_id], 21)
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
        result = re.search(r'{"props".*?"https://cdn.yangkeduo.com"}', body).group()
        logging.debug(result)
        if result:
            result = json.loads(result)
            result = self.dict_get(result, 'brandList', None)
            if result:
                for i in result:
                    subject_id = i["data"]["id"]
                    name = i["data"]["name"]
                    subject_info = self.build_subject_info(subject_id, name, path + [name], path_id + [subject_id], 14)
                    subject_list.append(subject_info)
                    self.save_log(json.dumps({"subject_info_kill": subject_info}))
                item = CategoryItem()
                logging.debug(json.dumps({'subject_list_kill': subject_list}))
                self.save_log(json.dumps({'subject_list_kill': subject_list}))
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

    def build_subject_info(self, subject_id, title, path, path_id, api_type, subjectType=1, activity_type=2):
        info = {'subject_id': subject_id, 'name': title, 'path': path, 'type': subjectType, "api_type": api_type,
                'activity_type': activity_type, 'path_id': path_id}
        return info

    def build_subject_goods_info(self, subject_id, title, path, path_id, api_type, goods_id_str, subjectType=1, activity_type=2):
        info = {'subject_id': subject_id, 'name': title, 'path': path, 'type': subjectType, "api_type": api_type,
                'activity_type': activity_type, 'path_id': path_id, 'goods_id_str': goods_id_str}
        return info

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

