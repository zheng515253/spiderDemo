# -*- coding: utf-8 -*-
import redis
import scrapy
import json, time, sys, random, re, pyssdb, logging, urllib.request, urllib.parse, pddSign, hashlib, hmac, os
import base64
from Crypto.Cipher import PKCS1_v1_5 as Cipher_pkcs1_v1_5
from Crypto.Cipher import AES
from Crypto.PublicKey import RSA
from spider.items import CategoryItem
from scrapy.utils.project import get_project_settings
from urllib import error

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectionRefusedError, DNSLookupError
from twisted.web._newclient import ResponseFailed, ResponseNeverReceived
from scrapy.utils.response import response_status_message  # 获取错误代码信息

'''获取店铺信息及销量记录'''


class PddMallKeywordsV8Spider(scrapy.Spider):
    name = 'pdd_promotion_keywords_v8'
    laravel_cookie = ''
    url = 'https://mms.pinduoduo.com/venus/api/subway/analysis/queryKeywordRank'
    key = 'eNgWuwpQ84MZVTQbmdtHWTEwYXFibzDNXPSMP+B5VsA='
    fail_nums = 5
    cookie = ''
    username = '18682498600'
    password = 'Zwmyhj*660730'
    category_info = {}
    size = 10  # 页码
    max_page = 30  # 最大抓取页数
    mall_pass_id_key = "PDDMMS_PASSID_COLLECTION_HASH"
    days = [1, 3, 7, 15]
    rankTypes = [1, 2]
    now_time = int(time.time())
    mall_pass_list = dict()
    proxy_count = 0
    current_proxy = ''
    proxy_start_time = 0
    proxy_ip_list = []
    limit = 100

    custom_settings = {
        # 'LOG_FILE':'',
        # 'LOG_LEVEL':'DEBUG',
        # 'LOG_ENABLED':True,
        'DOWNLOAD_TIMEOUT': 5,  # 超时时间
        'RETRY_ENABLED': False,  # 是否重试,
        # 'RETRY_TIMES': 5,
        'DOWNLOAD_DELAY': 0.1,
    }

    def __init__(self, hash_num=0, process_nums=1):
        self.hash_num = int(hash_num)  ##当前脚本号
        self.process_nums = int(process_nums)  ##脚本总数
        self.ssdb = pyssdb.Client('172.16.0.5', 8888)
        self.ssdb_client = pyssdb.Client(get_project_settings().get('SSDB_HOST'), 8888)
        self.key = base64.b64decode(self.key)
        pool = redis.ConnectionPool(host=get_project_settings().get('TOKEN_REDIS_HOST'), port='6379', db=10,
                                    password=get_project_settings().get("PROXY_REDIS_AUTH"))
        self.redis_client = redis.Redis(connection_pool=pool)
        # 创建连接池
        pool_ip = redis.ConnectionPool(host=get_project_settings().get('PROXY_REDIS_HOST'), port=6379, db=10,
                                      password='20A3NBVJnWZtNzxumYOz', decode_responses=True)
        # 创建链接对象
        self.redis_client_ip = redis.Redis(connection_pool=pool_ip)
        self.get_pdd_login_info()
        self.pdd_class = pddSign.pddSign()

    def start_requests(self):
        self.get_mall_id_duodian()
        self.get_mall_id_dianba()
        cat_1_list = list(self.mall_pass_list.keys())
        # cat_1_list = ["9317"]
        if not cat_1_list:
            logging.debug(cat_1_list)
            return
        for cat_id_1 in cat_1_list:
            mall_result = self.get_pass_mall_id(cat_id_1)
            mall_id, pass_id = mall_result
            # mall_id = '515006362'
            # pass_id = '1-ipnc04fHI3mZJ7/9+gwLV3+w0Vs/SbqqMoOVSOEXCkvrG4ttZIszdZlV2FQua/PPD+xwPzF+ykd61u1Fax/dsg_515006362_12501437'
            if not mall_id:
                continue
            cookie = 'PASS_ID' + '=' + pass_id + ";"
            href = 'https://mms.pinduoduo.com/exp/tools/dataAnalysis'
            yield scrapy.Request("https://mms.pinduoduo.com/venus/api/subway/analysis/queryCategoryInfo",
                                 method="POST", body=json.dumps({
                    "mallId": mall_id,
                    'crawlerInfo': self.pdd_class.messagePackV2('0al', href)
                }), meta={"mallId": mall_id, "cookie": cookie, "passId": pass_id, "cat_id_1": cat_id_1, 'level': 1,
                          'proxy': self.get_proxy_ip(False)}, headers={
                    'Content-Type': 'application/json',
                    'Cookie': cookie,
                    'Referer': 'https://mms.pinduoduo.com/exp/tools/dataAnalysis',
                }, callback=self.parse_category_info, dont_filter=True, errback=self.errback_httpbin)

    def parse_category_info(self, response):
        mall_id = response.meta["mallId"]
        level = response.meta["level"]
        pass_id = response.meta['passId']
        cat_id_1 = response.meta["cat_id_1"]
        proxy = response.meta["proxy"]
        result = json.loads(response.body.decode('utf-8'))
        if result['success'] == False:
            if result["error_code"] == 43001 or result["error_code"] == 9:
                logging.debug(json.dumps(result))
                content = json.dumps(
                    {'level': 1, "status": 'fail_fetch', "cat_id": cat_id_1, 'mall_id': mall_id, 'pass_id': pass_id,
                     "fail_reason": result})
                self.save_mall_log(content)
                # 更换cookie重新发送请求
                mall_id, pass_id = self.get_pass_mall_id(cat_id_1)
                if not mall_id:
                    return None
                cookie = 'PASS_ID' + '=' + pass_id + ";"
                href = 'https://mms.pinduoduo.com/exp/tools/dataAnalysis'
                yield scrapy.Request("https://mms.pinduoduo.com/venus/api/subway/analysis/queryCategoryInfo",
                                     method="POST", body=json.dumps({
                        "mallId": mall_id,
                        'crawlerInfo': self.pdd_class.messagePackV2('0al', href)
                    }), meta={"mallId": mall_id, "cookie": cookie, "passId": pass_id, "cat_id_1": cat_id_1,
                              'level': level, 'proxy': self.get_proxy_ip(False)}, headers={
                        'Content-Type': 'application/json',
                        'Cookie': cookie,
                        'Referer': 'https://mms.pinduoduo.com/exp/tools/dataAnalysis',
                    }, callback=self.parse_category_info, dont_filter=True, errback=self.errback_httpbin)
            return None
        content = json.dumps(
            {'level': 1, "status": 'success_fetch', 'cat_id_1': cat_id_1, 'mall_id': mall_id, 'result': '有效pass_id',
             "pass_id": pass_id})
        self.save_mall_log(content)
        mall_list = result['result']
        # 一级分类
        cat = {
            "pass_id": pass_id,
            "mall_id": mall_id,
            'level': 1,
            'cat_id': cat_id_1,
            'cat_id_1': cat_id_1,
            'cat_id_2': '',
            'cat_id_3': '',
        }
        meta = {'cat': cat, 'page': 1, 'rank': 0, "pass_id": pass_id, "mall_id": mall_id, "proxy": proxy}
        cookie = 'PASS_ID' + '=' + pass_id + ";"
        headers = self.make_headers(cookie)
        for day in self.days:
            meta['day'] = day
            for rankType in self.rankTypes:
                meta['rank_type'] = rankType
                query_data = self.build_query_data(cat, meta['page'], self.size, day, rankType)
                yield scrapy.Request(self.url, method="POST", body=json.dumps(query_data), meta=meta, headers=headers,
                                     callback=self.parse, dont_filter=True, errback=self.errback_httpbin)

        # 二级分类
        for child_category in mall_list['catInfoList']:
            cat_id_2 = child_category['id']
            cat = {
                "passId": pass_id,
                "mall_id": mall_id,
                'level': 2,
                'cat_id': cat_id_2,
                'cat_id_1': cat_id_1,
                'cat_id_2': cat_id_2,
                'cat_id_3': '',
            }
            meta = {'cat': cat, 'page': 1, 'rank': 0, "cat_id_1": cat_id_1, 'mall_id': mall_id, "pass_id": pass_id,
                    "proxy": proxy}
            headers = self.make_headers(cookie)
            for day in self.days:
                meta['day'] = day
                for rankType in self.rankTypes:
                    meta['rank_type'] = rankType
                    query_data = self.build_query_data(cat, meta['page'], self.size, day, rankType)
                    yield scrapy.Request(self.url, method="POST", body=json.dumps(query_data), meta=meta,
                                         headers=headers, callback=self.parse, dont_filter=True, errback=self.errback_httpbin)

            # 三级分类
            href = 'https://mms.pinduoduo.com/exp/tools/dataAnalysis'
            yield scrapy.Request("https://mms.pinduoduo.com/venus/api/subway/analysis/queryCategoryInfo",
                                 method="POST", body=json.dumps({
                    "mallId": mall_id,
                    'crawlerInfo': self.pdd_class.messagePackV2('0al', href),
                    'catId2': child_category['id']
                }), meta={'cat_id_2': child_category['id'], 'cat_id_1': mall_list["mallCatId"], 'mall_id': mall_id,
                          'level': 2,
                          "pass_id": pass_id, "cookie": cookie, "proxy": proxy}, headers={
                    'Content-Type': 'application/json',
                    'Cookie': cookie,
                    'Referer': 'https://mms.pinduoduo.com/exp/tools/dataAnalysis',
                }, callback=self.parse_third_category, dont_filter=True, errback=self.errback_httpbin)

    def parse_third_category(self, response):
        mall_id = response.meta["mall_id"]
        level = response.meta["level"]
        cat_id_2 = response.meta['cat_id_2']
        cat_id_1 = response.meta["cat_id_1"]
        pass_id = response.meta["pass_id"]
        cookie = response.meta["cookie"]
        proxy = response.meta["proxy"]
        result = json.loads(response.body.decode('utf-8'))
        if result['success'] == False:
            logging.debug(json.dumps(result))
            content = json.dumps(
                {"level": level, "status": 'fail_fetch', "mall_id": mall_id, "pass_id": pass_id, "cat_1": cat_id_1,
                 "cat_2": cat_id_2, "fail_reason": result})
            self.save_mall_log(content)
            href = 'https://mms.pinduoduo.com/exp/tools/dataAnalysis'
            mall_result = self.get_pass_mall_id(cat_id_1)
            mall_id, pass_id = mall_result
            if not mall_id:
                return None
            cookie = 'PASS_ID' + '=' + pass_id + ";"
            yield scrapy.Request("https://mms.pinduoduo.com/venus/api/subway/analysis/queryCategoryInfo",
                                 method="POST", body=json.dumps({
                    "mallId": mall_id,
                    'crawlerInfo': self.pdd_class.messagePackV2('0al', href),
                    'catId2': cat_id_2
                }), meta={'cat_id_2': cat_id_2, 'cat_id_1': cat_id_1, 'mall_id': mall_id, 'level': level,
                          "pass_id": pass_id, "cookie": cookie, 'proxy': self.get_proxy_ip(False)}, headers={
                    'Content-Type': 'application/json',
                    'Cookie': cookie,
                    'Referer': 'https://mms.pinduoduo.com/exp/tools/dataAnalysis',
                }, callback=self.parse_third_category, dont_filter=True, errback=self.errback_httpbin)
            return
        content = json.dumps(
            {'level': level, "status": 'success_fetch', "mall_id": mall_id, "passId": pass_id, "cat_1": cat_id_1,
             "cat_2": cat_id_2, "result": result})
        self.save_mall_log(content)

        for child_category in result['result']['catInfoList']:
            cat_id_3 = child_category['id']
            cat = {
                'mall_id': mall_id,
                "passId": pass_id,
                'level': 3,
                'cat_id': cat_id_3,
                'cat_id_1': cat_id_1,
                'cat_id_2': cat_id_2,
                'cat_id_3': cat_id_3}
            meta = {'cat': cat, 'page': 1, 'rank': 0, 'mall_id': mall_id, "pass_id": pass_id, "proxy": proxy}
            headers = self.make_headers(cookie)
            for day in self.days:
                meta['day'] = day
                for rankType in self.rankTypes:
                    meta['rank_type'] = rankType
                    query_data = self.build_query_data(cat, meta['page'], self.size, day, rankType)
                    yield scrapy.Request(self.url, method="POST", body=json.dumps(query_data), meta=meta,
                                         headers=headers, callback=self.parse, dont_filter=True, errback=self.errback_httpbin)

    '''抓取关键词热度 丰富度 排名'''
    def parse(self, response):
        result = json.loads(response.body.decode('utf-8'))
        logging.debug(json.dumps(result))
        meta = response.meta
        cat = meta['cat']
        day = meta['day']
        rank_type = meta['rank_type']
        cat_id = cat['cat_id']
        cat_id_1 = cat['cat_id_1']
        cat_id_2 = cat['cat_id_2']
        cat_id_3 = cat['cat_id_3']

        if 'success' in result.keys() and result['success'] and 'result' in result.keys():
            content = json.dumps({'status': 'success', 'cat': cat, "result": result['result']})
            self.save_cat_log(content)
            for keyword_item in result['result']:
                keyword_data = {'category': cat_id, 'day': day, 'rank_type': rank_type}
                keyword_data['cat_id_1'] = cat['cat_id_1']
                if cat_id_2:
                    keyword_data['cat_id_2'] = cat_id_2
                    if cat_id_3:
                        keyword_data['cat_id_3'] = cat_id_3
                keyword_data['rank_num'] = keyword_item['rankNum']
                keyword_data['click_num'] = keyword_item['clickNum']
                keyword_data['compete_value'] = keyword_item['competeValue']
                keyword_data['ctr'] = keyword_item['ctr']
                keyword_data['cvr'] = keyword_item['cvr']
                keyword_data['impr_avg_bid'] = keyword_item['imprAvgBid']
                keyword_data['pv'] = keyword_item['pv']
                keyword_data['word'] = keyword_item['word']
                keywordItem = CategoryItem()
                keywordItem['cat_list'] = keyword_data
                yield keywordItem
        else:
            content = json.dumps({'status': 'fail', 'cat': cat, "result": result})
            self.save_cat_log(content)
            # 三级分类
            mall_result = self.get_pass_mall_id(cat_id_1)
            mall_id, pass_id = mall_result
            if not mall_id:
                return None
            cookie = 'PASS_ID' + '=' + pass_id + ";"
            cat["mall_id"] = mall_id
            meta["proxy"] = self.get_proxy_ip(False)
            headers = self.make_headers(cookie)
            query_data = self.build_query_data(cat, meta['page'], self.size, day, rank_type)
            yield scrapy.Request(self.url, method="POST", body=json.dumps(query_data), meta=meta, headers=headers,
                                 callback=self.parse, dont_filter=True, errback=self.errback_httpbin)
            return None

    def build_query_data(self, cat, page=1, page_size=10, spanDays=1, RankType=1):
        query_data = {'catId': ''}
        query_data['catId'] = cat['cat_id_1']
        if cat['level'] == 2:
            query_data['catId'] = cat['cat_id_2']
        if cat['level'] == 3:
            query_data['catId'] = cat['cat_id_2']
        query_data['dateRange'] = spanDays
        query_data['mallId'] = cat['mall_id']
        # query_data['pageSize'] = page_size
        query_data['keywordRankType'] = RankType
        query_data['beginDate'] = ''
        query_data['endDate'] = ''
        href = 'https://mms.pinduoduo.com/exp/tools/dataAnalysis'
        query_data['crawlerInfo'] = self.pdd_class.messagePackV2('0al', href)
        return query_data

    '''生成headers头信息'''

    def make_headers(self, cookie):
        chrome_version = str(random.randint(59, 63)) + '.0.' + str(random.randint(1000, 3200)) + '.94'
        headers = {
            "Host": "mms.pinduoduo.com",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Referer": "https://mms.pinduoduo.com/exp/tools/dataAnalysis",
            "Connection": "keep-alive",
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/' + chrome_version + ' Safari/537.36',
            'Cookie': cookie,
        }

        ip = str(random.randint(100, 200)) + '.' + str(random.randint(1, 255)) + '.' + str(
            random.randint(1, 255)) + '.' + str(random.randint(1, 255))
        headers['CLIENT-IP'] = ip
        headers['X-FORWARDED-FOR'] = ip
        return headers

    def get_all_category(self):
        category_hash = 'pdd_mall_category_hash'
        category_data = self.ssdb_client.hgetall(category_hash)
        if not category_data:
            return False
        return category_data

    '''获取拼多多后台登录信息'''

    def get_pdd_login_info(self):
        login_token = self.get_access_token()
        self.cookie = 'PASS_ID=' + login_token + ';'

    def create_sra_public_key(self, password):
        url = "https://mms.pinduoduo.com/earth/api/queryPasswordEncrypt"
        request = urllib.request.Request(url=url)
        login_data = urllib.request.urlopen(request)
        login_data = json.loads(login_data.read().decode('utf-8'))
        public_key = login_data['result']['publicKey']
        public_key2 = """-----BEGIN PUBLIC KEY-----
		%s
		-----END PUBLIC KEY-----""" % (public_key)
        ras_key = RSA.importKey(public_key2)
        cipher = Cipher_pkcs1_v1_5.new(ras_key)
        p = cipher.encrypt(password.encode(encoding='utf-8'))
        cipher_text = base64.b64encode(p)
        return str(cipher_text, encoding="utf-8")

    def get_access_token(self):
        self.login_duodian()
        i = 0
        while True:
            url = 'http://47.107.76.156/api/bindMall/pddMmsToken'
            now = int(time.time())
            salt = str(random.randint(100000, 999999))
            token = salt + str(now) + salt
            SIGN = {'time': now, 'salt': salt, 'token': hashlib.md5(token.encode(encoding='UTF-8')).hexdigest()}
            data = {'username': self.username, 'password': self.password}
            data = urllib.parse.urlencode(data).encode('utf-8')
            # data = json.dumps(data).encode('utf-8')
            headers = {'Accept': 'application/vnd.ddgj.v2+json', 'SIGN': self.aes_encrypt(SIGN),
                       'Cookie': self.laravel_cookie}
            logging.debug({
                'type': 'token',
                'SIGN': SIGN,
                'data': data,
                'headers': headers,
            })
            new_url = urllib.request.Request(url, data, headers)
            response = urllib.request.urlopen(new_url).read().decode('utf-8')
            logging.debug(response)
            login_token = json.loads(response)['passId']
            if login_token:
                return login_token
            i += 1
            if i > 5:
                return ''

    def aes_encrypt(self, data):
        key = self.key  # 加密时使用的key，只能是长度16,24和32的字符串
        iv = os.urandom(16)
        string = json.dumps(data).encode('utf-8')
        padding = 16 - len(string) % 16
        string += bytes(chr(padding) * padding, 'utf-8')
        value = base64.b64encode(self.mcrypt_encrypt(string, iv))
        iv = base64.b64encode(iv)
        mac = hmac.new(key, iv + value, hashlib.sha256).hexdigest()
        dic = {'iv': iv.decode(), 'value': value.decode(), 'mac': mac}
        return base64.b64encode(bytes(json.dumps(dic), 'utf-8')).decode('utf-8')

    def mcrypt_encrypt(self, value, iv):
        key = self.key
        AES.key_size = 128
        crypt_object = AES.new(key=key, mode=AES.MODE_CBC, IV=iv)
        return crypt_object.encrypt(value)

    def login_duodian(self):
        i = 0
        while True:
            url = 'http://47.107.76.156/api/user/loginV2'
            now = int(time.time())
            salt = str(random.randint(100000, 999999))
            token = salt + str(now) + salt
            SIGN = {'time': now, 'salt': salt, 'token': hashlib.md5(token.encode(encoding='UTF-8')).hexdigest()}
            data = {'username': 'dianba', 'password': 'dianba123456'}
            headers = {'Accept': 'application/vnd.ddgj.v2+json', 'SIGN': self.aes_encrypt(SIGN)}
            logging.debug({
                'type': 'login',
                'SIGN': SIGN,
                'data': data,
                'headers': headers,
            })
            data = urllib.parse.urlencode(data).encode('utf-8')
            new_url = urllib.request.Request(url, data, headers)
            response = urllib.request.urlopen(new_url)
            res_headers = response.getheaders()
            result_body = response.read().decode('utf-8')
            # print(res_headers, result_body)
            d = {}
            for k, v in res_headers:
                if "Set-Cookie" in k:
                    d[k] = v
            self.laravel_cookie = d['Set-Cookie'].split(';')[0]
            if self.laravel_cookie:
                return ''
            i += 1
            if i > 5:
                return ''

    def get_mall_id(self):
        """ 获取店铺id和pass_id"""
        url = 'http://47.107.76.156/api/open/pddmms/admalls/'
        headers = {"username": 'dianba.main', "password": "sVqk34j2U82nXnhYPDx8nwdUHW693Gdr",
                   'Accept': "application/vnd.ddgj.v2+json"}
        request_url = urllib.request.Request(url=url, headers=headers)
        response = urllib.request.urlopen(request_url)
        mall_id_dict = eval(response.read().decode("utf-8"))
        mall_id_list = list()
        for key, value in mall_id_dict.items():
            mall_dic = dict()
            mall_dic['mallId'] = value['mallId']
            mall_dic['passId'] = value['passId']
            mall_id_list.append(mall_dic)
        return mall_id_list

    def get_mall_id_duodian(self):
        """ 获取多店店铺id和pass_id"""
        url = 'http://47.107.76.156/api/open/pddmms/admallsV2/'
        headers = {"username": 'dianba.main', "password": "sVqk34j2U82nXnhYPDx8nwdUHW693Gdr",
                   'Accept': "application/vnd.ddgj.v2+json"}
        request_url = urllib.request.Request(url=url, headers=headers)
        response = urllib.request.urlopen(request_url)
        mall_id_dict = eval(response.read().decode("utf-8"))
        for key, value in mall_id_dict.items():
            list_mall = list()
            for i in value:
                dict_mall = dict()
                dict_mall["passId"] = i["passId"]
                dict_mall["mallId"] = i["mallId"]
                list_mall.append(dict_mall)
                self.mall_pass_list[key] = list_mall
            self.redis_client.hset(self.mall_pass_id_key, key, json.dumps(list_mall))

    def get_mall_id_dianba(self):
        """ 获取店铺id和pass_id"""
        url = 'http://www.dianba6.com/api/open/pddmms/admalls'
        request_url = urllib.request.Request(url=url)
        response = urllib.request.urlopen(request_url)
        mall_id_dict = eval(response.read().decode("utf-8"))
        for key, value in mall_id_dict.items():
            list_mall = list()
            for i in value:
                dict_mall = dict()
                dict_mall["passId"] = i["passId"]
                dict_mall["mallId"] = i["mallId"]
                list_mall.append(dict_mall)
                self.mall_pass_list[key] = list_mall
            self.redis_client.hset(self.mall_pass_id_key, key, json.dumps(list_mall))

    def get_pass_mall_id(self, cat_id_1):
        try:
            mall_pass_id_list = self.mall_pass_list[cat_id_1].pop()
            mall_id = mall_pass_id_list["mallId"]
            pass_id = mall_pass_id_list["passId"]
            content = json.dumps(
                {'level': 0, "status": 'success_get', "cat_id": cat_id_1, 'mall_id': mall_id, 'pass_id': pass_id})
            self.save_mall_log(content)
        except Exception:
            mall_id = None
            pass_id = None
            content = json.dumps(
                {'level': 0, "status": 'fail_get', "cat_id": cat_id_1, 'mall_id': mall_id, 'pass_id': pass_id,
                 "fail_reason": "当前分类没有pass_id"})
            self.save_mall_log(content)
        return mall_id, pass_id

    def errback_httpbin(self, failure):
        request = failure.request
        if failure.check(HttpError):
            response = failure.value.response
            errmsg = 'errback <%s> %s , response status:%s' % (
                request.url, failure.value, response_status_message(response.status))
            # self.err_after(request.meta, True)
            self.save_error_log(json.dumps({'meta': request.meta, "errmsg": errmsg}))
        elif failure.check(ResponseFailed):
            errmsg = 'errback <%s> ResponseFailed' % request.url
            # self.err_after(request.meta, True)
            self.save_error_log(json.dumps({'meta': request.meta, "errmsg": errmsg}))
        elif failure.check(ConnectionRefusedError):
            errmsg = 'errback <%s> ConnectionRefusedError' % request.url
            # self.err_after(request.meta, True)
            self.save_error_log(json.dumps({'meta': request.meta, "errmsg": errmsg}))
        elif failure.check(ResponseNeverReceived):
            errmsg = 'errback <%s> ResponseNeverReceived' % request.url
            # self.err_after(request.meta, False)
            self.save_error_log(json.dumps({'meta': request.meta, "errmsg": errmsg}))
        elif failure.check(TCPTimedOutError, TimeoutError):
            errmsg = 'errback <%s> TimeoutError' % request.url
            meta = request.meta
            body = request.body
            headers = request.headers
            meta["proxy"] = self.get_proxy_ip(True)
            logging.debug(json.dumps(meta))
            self.save_proxy_log(json.dumps(meta))
            if request.url == self.url:
                yield scrapy.Request(self.url, method="POST", body=body, meta=meta,
                                     headers=headers, callback=self.parse, dont_filter=True, errback=self.errback_httpbin)
            else:
                level = request.meta["level"]
                if level == 1:
                    yield scrapy.Request(request.url, method="POST", body=body, meta=meta,
                                         headers=headers, callback=self.parse_category_info,
                                         dont_filter=True, errback=self.errback_httpbin)
                elif level == 2:
                    yield scrapy.Request(request.url, method="POST", body=body, meta=meta,
                                         headers=headers, callback=self.parse_third_category,
                                         dont_filter=True, errback=self.errback_httpbin)

            self.save_error_log(json.dumps({'meta': request.meta, "errmsg": errmsg}))
        else:
            errmsg = 'errback <%s> OtherError' % request.url
            meta = request.meta
            body = request.body
            headers = request.headers
            meta["proxy"] = self.get_proxy_ip(True)
            logging.debug(request.meta)
            self.save_proxy_log(json.dumps(meta))
            if request.url == self.url:
                yield scrapy.Request(self.url, method="POST", body=body, meta=meta,
                                     headers=headers, callback=self.parse, dont_filter=True, errback=self.errback_httpbin)
            else:
                level = request.meta["level"]
                if level == 1:
                    yield scrapy.Request(self.url, method="POST", body=body, meta=meta,
                                         headers=headers, callback=self.parse_category_info,
                                         dont_filter=True, errback=self.errback_httpbin)
                elif level == 2:
                    yield scrapy.Request(self.url, method="POST", body=body, meta=meta,
                                         headers=headers, callback=self.parse_third_category,
                                         dont_filter=True, errback=self.errback_httpbin)
            self.save_error_log(json.dumps({'meta': request.meta, 'errmsg': errmsg, }))

    # def err_after(self, request, refresh):
    #     meta = request.meta
    #     body = request.body
    #     headers = request.headers
    #     meta["proxy"] = self.get_proxy_ip(True)
    #     logging.debug(request.meta)
    #     self.save_proxy_log(json.dumps(meta))
    #     if request.url == self.url:
    #         yield scrapy.Request(self.url, method="POST", body=body, meta=meta,
    #                              headers=headers, callback=self.parse, errback=self.errback_httpbin)
    #     else:
    #         level = request.meta["level"]
    #         if level == 1:
    #             yield scrapy.Request(self.url, method="POST", body=body, meta=meta,
    #                                  headers=headers, callback=self.parse_category_info, errback=self.errback_httpbin)
    #         elif level == 2:
    #             yield scrapy.Request(self.url, method="POST", body=body, meta=meta,
    #                              headers=headers, callback=self.parse_third_category, errback=self.errback_httpbin)

    def get_proxy_ip(self, refresh):
        if not refresh and self.proxy_count < 500 and self.current_proxy != '':
            ip = self.current_proxy
            self.proxy_count += 1
        else:
            self.proxy_count = 0
            now_time = int(time.time())
            if now_time - self.proxy_start_time >= 2:
                self.proxy_ip_list = self.get_ssdb_proxy_ip()
                self.proxy_start_time = now_time

            if len(self.proxy_ip_list) <= 0:
                self.proxy_ip_list = self.get_ssdb_proxy_ip()

            if len(self.proxy_ip_list) <= 0:
                return ''
            ip = random.choice(self.proxy_ip_list)
            self.current_proxy = ip
            logging.debug(json.dumps({'ip': ip, 'count': self.proxy_count}))
        return 'http://' + str(ip)

    def get_ssdb_proxy_ip(self):
        ips = self.redis_client_ip.hkeys('proxy_ip_hash_fy')
        logging.debug(ips)
        res = []
        for index in range(len(ips)):
            if index % self.process_nums != self.hash_num:
                continue
            res.append(ips[index])
        if res:
            return res
        else:
            return []

    def save_mall_log(self, content):
        date = time.strftime('%Y-%m-%d')
        file_path = '/data/spider/logs/promotion_mall_log'
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        content = content + ","
        file_name = file_path + '/' + date + ".log"
        with open(file_name, 'a+') as f:
            f.write(content + "\r\n")

    def save_cat_log(self, content):
        date = time.strftime('%Y-%m-%d')
        file_path = '/data/spider/logs/promotion_cat_log'
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        content = content + ","
        file_name = file_path + '/' + date + ".log"
        with open(file_name, 'a+') as f:
            f.write(content + "\r\n")

    def save_error_log(self, info):
        logging.debug(info)
        date = time.strftime('%Y-%m-%d')

        file_path = '/data/spider/logs/promotion_err_log/' + str(self.name)
        if not os.path.exists(file_path):
            os.makedirs(file_path)

        data = '[' + str(time.strftime('%Y-%m-%d %H:%M:%S')) + '] ' + info

        file_name = file_path + '/' + date + '.log'
        with open(file_name, "a+") as f:
            f.write(data + "\r\n")

    def save_proxy_log(self, content):
        date = time.strftime('%Y-%m-%d')
        file_path = '/data/spider/logs/promotion_proxy_log'
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        content = content + ","
        file_name = file_path + '/' + date + ".log"
        with open(file_name, 'a+') as f:
            f.write(content + "\r\n")
