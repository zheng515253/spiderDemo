# -*- coding: utf-8 -*-
# 首页滚动栏活动信息
import logging
import os

import scrapy
import json, time, sys, random, urllib, pyssdb, re
from spider.items import CategoryItem
from urllib import parse as urlparse


class PddScrollActivityNewSpider(scrapy.Spider):
    """ 活动app接口版本"""
    name = 'pdd_scroll_activity_v'
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {'spider.middlewares.ProxyMiddleware': 100},
        # 'LOG_FILE':'',
        # 'LOG_LEVEL':'DEBUG',
        # 'LOG_ENABLED':True,
        'DOWNLOAD_TIMEOUT': 5,
        'RETRY_TIMES': 10,
    }

    def start_requests(self):  # app
        yield scrapy.Request(url='https://api.yangkeduo.com/api/alexa/homepage/hub?list_id=3482f3237f&platform=0&support_formats=1&client_time=' + str(int(time.time()*1000)), headers=self.make_headers())

    def parse(self, response):
        """ 获取首页活动信息"""
        result_str = response.body.decode('utf-8')
        result_dict = json.loads(result_str)

        # 处理轮播活动
        banner = self.dict_get(result_dict, 'carousel_banner', None)
        if banner and len(banner) > 0:
            path = ['首页滚动banner']
            for i in banner:
                title = i["title"]
                link_url = i["link_url"]
                subject = self.get_subject_type_id(link_url)
                if not subject:
                    continue
                subject_id = subject["subject_id"]
                type = subject["type"]
                headers = self.make_headers()
                new_url = "https://mobile.yangkeduo.com/" + link_url
                meta = {'path': path + [title], 'path_id': [subject_id], 'url_type': type}
                yield scrapy.Request(new_url, meta=meta, callback=self.parse_subject_banner, headers=headers,
                                     dont_filter=True, errback=self.errback_httpbin)

    def parse_subject_banner(self, response):
        """ 首页轮播"""
        subject_list = []
        body = response.body.decode('utf-8')
        logging.debug(json.dumps({'body': body}))
        result = re.search(r'{"store".*?"ssr":true}', body)
        path = response.meta['path']
        path_id = response.meta['path_id']
        url_type = response.meta["url_type"]
        item = CategoryItem()
        if result:
            result_dict = json.loads(result.group())
            result_str = result.group()
            try:
                name = result_dict["store"]["pageTitle"]
            except Exception:
                name = ''
            if url_type in [1, 2]:
                subject_id = path_id
                subject_info = self.build_subject_info(subject_id, name, path + [name], path_id + [subject_id], 61, url_type)
                subject_list.append(subject_info)
                self.save_log(json.dumps({"subject_info_banner" + str(url_type): subject_info}))

            if url_type in [4, 6, 12]:
                f = re.findall(r'"id":\d+,"DIYGoodsIDs"', result_str)
                if not f:
                    f = re.findall(r'"id":"\d+","DIYGoodsIDs"', result_str)
                subject_id_list = re.findall(r"\d+", str(f))
                a = 1
                for subject_id in subject_id_list:
                    subject_info = self.build_subject_info(subject_id, name, path + [name], path_id + [subject_id], 62, url_type, a)
                    a += 1
                    subject_list.append(subject_info)
                    self.save_log(json.dumps({"subject_info_banner_" + str(url_type): subject_info}))

            if url_type == 17:  # 接口不确定
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
                        name = "商品" + str(a)
                        subject_info = self.build_subject_info(id, name, path + [name], path_id + [id], 63, url_type, a)
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
                    subject_info = self.build_subject_20_info(subject_id, name, path + [name], path_id + [subject_id], subject_str, 64, url_type)
                    subject_list.append(subject_info)
                    self.save_log(json.dumps({"subject_info_banner_id_" + str(url_type): subject_info}))

            item['cat_list'] = subject_list
            yield item


    def get_subject_type_id(self, link_url):
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

    def build_subject_20_info(self, subject_id, title, path, path_id, subject_str, api_type, type_1, type_2=0, type_3=0):
        info = {'subject_id': subject_id, 'name': title, 'path': path, 'type_1': type_1, "api_type": api_type, 'subject_str':subject_str,
                'type_2': type_2, 'type_3': type_3, 'path_id': path_id}
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

