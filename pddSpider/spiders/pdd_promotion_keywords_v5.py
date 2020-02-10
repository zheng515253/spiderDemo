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

'''获取店铺信息及销量记录'''


class PddMallKeywordsV5Spider(scrapy.Spider):
    name = 'pdd_promotion_keywords_v5'
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
    cate_1_key = "pdd_keywords_cate_1_key"
    cate_2_key = "pdd_keywords_cate_2_key"
    cate_3_key = "pdd_keywords_cate_3_key"
    days = [1, 3, 7, 15]
    rankTypes = [1, 2]
    now_time = int(time.time())
    custom_settings = {
        'DOWNLOAD_TIMEOUT': 10,
        # 'LOG_FILE':'',
        # 'LOG_LEVEL':'DEBUG',
        # 'LOG_ENABLED':True,
        'DOWNLOAD_DELAY': 0.1,
    }

    def __init__(self):
        self.ssdb_client = pyssdb.Client(get_project_settings().get('SSDB_HOST'), 8888)
        self.key = base64.b64decode(self.key)
        pool = redis.ConnectionPool(host=get_project_settings().get('TOKEN_REDIS_HOST'), port='6379', db=10,
                                    password=get_project_settings().get("PROXY_REDIS_AUTH"))
        self.redis_client = redis.Redis(connection_pool=pool)
        self.get_pdd_login_info()
        if not self.cookie:
            return False
        self.pdd_class = pddSign.pddSign()

    def start_requests(self):
        yield scrapy.Request("https://mms.pinduoduo.com/earth/api/user/userinfo",
                             method="POST", body=json.dumps({
                "sceneId": 15,
                'Referer': ''
            }), meta={}, headers={
                'Content-Type': 'application/json',
                'Cookie': self.cookie,
            }, callback=self.parse_mall_info)

    def parse_mall_info(self, response):
        result = json.loads(response.body.decode('utf-8'))
        # self.mall_info = result['result']['mall']
        # logging.debug(json.dumps(self.mall_info))
        self.get_mall_id_duodian()
        # self.get_mall_id_dianba()
        mallId_list = self.redis_client.hkeys(self.mall_pass_id_key)
        if not mallId_list:
            logging.debug(mallId_list)
            return
        for mall_id in mallId_list:
            mall_id = mall_id.decode("utf-8")
            passId = self.redis_client.hget(self.mall_pass_id_key, mall_id).decode("utf-8")
            cookie = 'PASS_ID' + '=' + passId + ";"
            href = 'https://mms.pinduoduo.com/exp/tools/dataAnalysis'
            yield scrapy.Request("https://mms.pinduoduo.com/venus/api/subway/analysis/queryCategoryInfo",
                                 method="POST", body=json.dumps({
                    "mallId": mall_id,
                    'crawlerInfo': self.pdd_class.messagePackV2('0al', href)
                }), meta={"mallId": mall_id, "cookie": cookie, "passId": passId}, headers={
                    'Content-Type': 'application/json',
                    'Cookie': cookie,
                    'Referer': 'https://mms.pinduoduo.com/exp/tools/dataAnalysis',
                }, callback=self.parse_category_info, errback=self.error_parse)

    def error_parse(self, failure):
        request = failure.request
        meta = request.meta
        mall_id = meta["mallId"]
        pass_id = meta['passId']
        cookie = meta["cookie"]
        logging.debug(request)
        log = json.dumps({'mall_id': str(mall_id), "pass_id": str(pass_id), 'request': "false"})
        self.save_request_error_log(log)
        href = 'https://mms.pinduoduo.com/exp/tools/dataAnalysis'
        yield scrapy.Request("https://mms.pinduoduo.com/venus/api/subway/analysis/queryCategoryInfo",
                             method="POST", body=json.dumps({"mallId": mall_id,
                                                             'crawlerInfo': self.pdd_class.messagePackV2('0al', href)}),
                             meta={"mallId": mall_id, "cookie": cookie, "passId": pass_id}, headers={
                'Content-Type': 'application/json',
                'Cookie': cookie,
                'Referer': 'https://mms.pinduoduo.com/exp/tools/dataAnalysis',
            }, callback=self.parse_category_info)

    def parse_category_info(self, response):
        mall_id = response.meta["mallId"]
        cookie = response.meta["cookie"]
        passId = response.meta['passId']
        result = json.loads(response.body.decode('utf-8'))
        if result['success'] == False:
            logging.debug(json.dumps(result))
            content = 'passId过期: mall_Id：' + str(mall_id) + "； " + 'PASS_ID：' + str(passId) + "； " + "失败原因：" + str(
                result)
            self.save_mall_pass_id_log(content)
            self.redis_client.hdel(self.mall_pass_id_key, mall_id)
            return None

        content = '有效pass_id: mall_Id：' + str(mall_id) + "； " + 'PASS_ID：' + str(passId) + "； "
        self.save_mall_pass_id_log(content)
        # self.save_mallId_log(mall_id, result)
        mall_list = result['result']
        # 一级分类
        cat_id_1 = mall_list['mallCatId']
        cat = {
            "passId": passId,
            "mallId": mall_id,
            'level': 1,
            'cat_id': cat_id_1,
            'cat_id_1': cat_id_1,
            'cat_id_2': '',
            'cat_id_3': '',
        }
        cate_1_list = self.redis_client.hkeys(self.cate_1_key)
        if str(cat_id_1).encode("utf-8") in cate_1_list:
            return
        self.redis_client.hset(self.cate_1_key, cat_id_1, self.now_time)
        meta = {'cat': cat, 'page': 1, 'rank': 0}
        headers = self.make_headers(cookie)
        for day in self.days:
            meta['day'] = day
            for rankType in self.rankTypes:
                meta['rank_type'] = rankType
                query_data = self.build_query_data(cat, meta['page'], self.size, day, rankType)
                if mall_id in self.max_times_mall_list:
                    return None
                yield scrapy.Request(self.url, method="POST", body=json.dumps(query_data), meta=meta, headers=headers,
                                     callback=self.parse)

        # 二级分类
        for child_category in mall_list['catInfoList']:
            cat_id_2 = child_category['id']
            cate_2_list = self.redis_client.hkeys(self.cate_2_key)
            if str(cat_id_2).encode("utf-8") in cate_2_list:
                return
            self.redis_client.hset(self.cate_2_key, cat_id_2, self.now_time)
            cat = {
                "passId": passId,
                "mallId": mall_id,
                'level': 2,
                'cat_id': cat_id_1,
                'cat_id_1': cat_id_1,
                'cat_id_2': cat_id_2,
                'cat_id_3': '',
            }
            meta = {'cat': cat, 'page': 1, 'rank': 0}
            headers = self.make_headers(cookie)
            for day in self.days:
                meta['day'] = day
                for rankType in self.rankTypes:
                    meta['rank_type'] = rankType
                    query_data = self.build_query_data(cat, meta['page'], self.size, day, rankType)
                    if mall_id in self.max_times_mall_list:
                        return None
                    yield scrapy.Request(self.url, method="POST", body=json.dumps(query_data), meta=meta,
                                         headers=headers, callback=self.parse)

            # 三级分类
            href = 'https://mms.pinduoduo.com/exp/tools/dataAnalysis'
            if mall_id in self.max_times_mall_list:
                return
            yield scrapy.Request("https://mms.pinduoduo.com/venus/api/subway/analysis/queryCategoryInfo",
                                 method="POST", body=json.dumps({
                    "mallId": mall_id,
                    'crawlerInfo': self.pdd_class.messagePackV2('0al', href),
                    'catId2': child_category['id']
                }), meta={'catId2': child_category['id'], 'catId1': mall_list["mallCatId"], 'mallId': mall_id,
                          "cookie": cookie, "passId": passId}, headers={
                    'Content-Type': 'application/json',
                    'Cookie': cookie,
                    'Referer': 'https://mms.pinduoduo.com/exp/tools/dataAnalysis',
                }, callback=self.parse_third_category)

    def parse_third_category(self, response):
        mall_id = response.meta["mallId"]
        catId2 = response.meta['catId2']
        catId1 = response.meta["catId1"]
        cookie = response.meta["cookie"]
        passId = response.meta["passId"]
        result = json.loads(response.body.decode('utf-8'))
        if result['success'] == False:
            logging.debug(json.dumps(result))
            content = json.dumps(
                {"第三级分类失败 mall_id": mall_id, "passId": passId, "cat_1": catId1, "cat_2": catId2, "result": result})
            self.save_third_log(content, success=False)
            return
        content = json.dumps(
            {"第三级分类成功 mall_id": mall_id, "passId": passId, "cat_1": catId1, "cat_2": catId2, "result": result})
        self.save_third_log(content, success=True)
        for child_category in result['result']['catInfoList']:
            cat_id_3 = child_category['id']
            cate_3_list = self.redis_client.hkeys(self.cate_3_key)
            if str(cat_id_3).encode("utf-8") in cate_3_list:
                return
            self.redis_client.hset(self.cate_3_key, cat_id_3, self.now_time)
            cat = {
                'mallId': mall_id,
                "passId": passId,
                'level': 2,
                'cat_id': catId1,
                'cat_id_1': catId1,
                'cat_id_2': catId2,
                'cat_id_3': cat_id_3,
            }
            meta = {'cat': cat, 'page': 1, 'rank': 0}
            headers = self.make_headers(cookie)
            for day in self.days:
                meta['day'] = day
                for rankType in self.rankTypes:
                    meta['rank_type'] = rankType
                    query_data = self.build_query_data(cat, meta['page'], self.size, day, rankType)
                    if mall_id in self.max_times_mall_list:
                        return None
                    yield scrapy.Request(self.url, method="POST", body=json.dumps(query_data), meta=meta,
                                         headers=headers, callback=self.parse)

    '''抓取关键词热度 丰富度 排名'''

    def parse(self, response):
        result = json.loads(response.body.decode('utf-8'))
        logging.debug(json.dumps(result))
        meta = response.meta
        cat = meta['cat']
        mallId = cat["mallId"]
        passId = cat["passId"]
        cat_id_1 = cat['cat_id_1']
        cat_id_2 = cat['cat_id_2']
        cat_id_3 = cat['cat_id_3']
        level = cat['level']
        day = meta['day']
        rank_type = meta['rank_type']
        if result['errorCode'] == 9:
            self.max_times_mall_list.append(mallId)
            content = json.dumps({"第三级分类成功 mall_id": mallId, "passId": passId, "result": result})
            self.max_times_out_log(content)
            return None
        if 'errorCode' in result.keys() and result['errorCode'] == 1000:
            if result['result'] is None:
                content = json.dumps({"mallId": mallId, "PASS_ID": passId, "status": "fail", "leval": level, "day": day,
                                      "rank_type": rank_type, "cat_id_1": cat_id_1, "cat_id_2": cat_id_2,
                                      "cat_id_3": cat_id_3, "result": result['result']}) + ","
                self.save_goods_log(str(mallId), content, success=False)
                return None
            content = json.dumps(
                {"PASS_ID": passId, "leval": level, "day": day, "rank_type": rank_type, "cat_id_1": cat_id_1,
                 "cat_id_2": cat_id_2, "cat_id_3": cat_id_3, "goods_count": len(result['result'])}) + ","
            self.save_goods_log(str(mallId), content, success=True)
            for keyword_item in result['result']:
                keyword_data = {'category': cat_id_1, 'day': day, 'rank_type': rank_type}
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
            content = json.dumps({"mallId": mallId, "PASS_ID": passId, "status": "fail", "leval": level, "day": day,
                                  "rank_type": rank_type, "cat_id_1": cat_id, "cat_id_2": cat_id_2,
                                  "cat_id_3": cat_id_3, "result": result}) + ","
            self.save_goods_log(str(mallId), content, success=False)

    def build_query_data(self, cat, page=1, page_size=10, spanDays=1, RankType=1):
        query_data = {'catId': ''}
        query_data['catId'] = cat['cat_id_1']
        if cat['level'] == 2:
            query_data['catId'] = cat['cat_id_2']
        if cat['level'] == 3:
            query_data['catId'] = cat['cat_id_2']
        query_data['dateRange'] = spanDays
        query_data['mallId'] = cat['mallId']
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
        # query_data = {}
        # headers = {
        #     'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15G77 pddmt_ios_version/2.2.1 pddmt_ios_build/1809091638 ===  iOS/11.4.1 Model/iPhone9,1 BundleID/com.xunmeng.merchant AppVersion/2.2.1 AppBuild/1809091638',
        #     'Content-type': 'application/json'
        # }
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

    def save_mall_pass_id_log(self, content):
        date = time.strftime("%Y-%m-%d")
        file_path = '/data/spider/log/mall_pass_id_log'
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        file_name = file_path + "/" + date + '_mall.log'
        with open(file_name, "a+") as f:
            f.write(content + "\r\n")

    def save_third_log(self, content, success):
        date = time.strftime("%Y-%m-%d")
        file_path = '/data/spider/log/save_third_log'
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        file_name = file_path + "/" + date + '_fail.log'
        if success:
            file_name = file_path + "/" + date + '_success.log'
        with open(file_name, "a+") as f:
            f.write(content + "\r\n")

    def save_goods_log(self, mall_id, content, success=True):
        date = time.strftime("%Y-%m-%d-%H")
        file_path = '/data/spider/log/save_goods_log/mallId_' + str(mall_id)
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        file_name = file_path + "/" + date + '_fail.log'
        if success:
            file_name = file_path + "/" + date + '_success.log'
        with open(file_name, "a+") as f:
            f.write(content + "\r\n")

    def get_mall_id_duodian(self):
        """ 获取多店店铺id和pass_id"""
        url = 'http://47.107.76.156/api/open/pddmms/admalls/'
        headers = {"username": 'dianba.main', "password": "sVqk34j2U82nXnhYPDx8nwdUHW693Gdr",
                   'Accept': "application/vnd.ddgj.v2+json"}
        request_url = urllib.request.Request(url=url, headers=headers)
        response = urllib.request.urlopen(request_url)
        mall_id_dict = eval(response.read().decode("utf-8"))
        for key, value in mall_id_dict.items():
            self.redis_client.hset(self.mall_pass_id_key, value['mallId'], value['passId'])

    def get_mall_id_dianba(self):
        """ 获取电霸店铺id和pass_id"""
        url = 'http://www.dianba6.com/api/open/pddmms/admalls'
        request_url = urllib.request.Request(url=url)
        response = urllib.request.urlopen(request_url)
        mall_id_dict = eval(response.read().decode("utf-8"))
        for key, value in mall_id_dict.items():
            self.redis_client.hset(self.mall_pass_id_key, value['mallId'], value['passId'])

    def save_log(self, content):
        date = time.strftime('%Y-%m-%d')
        file_path = '/data/spider/log/pass_id_check'
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        data = '[' + str(time.strftime('%Y-%m-%d %H:%M:%S')) + '] ' + content
        file_name = file_path + '/' + date + ".log"
        with open(file_name, 'a+') as f:
            f.write(data + "\r\n")

    def save_request_error_log(self, content):
        date = time.strftime('%Y-%m-%d')
        file_path = '/data/spider/log/passid_request_error_check'
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        # data = '[' + str(time.strftime('%Y-%m-%d %H:%M:%S')) + '] ' + content
        content = content + ","
        file_name = file_path + '/' + date + "check_.log"
        with open(file_name, 'a+') as f:
            f.write(content + "\r\n")

    def max_times_out_log(self, content):
        date = time.strftime('%Y-%m-%d')
        file_path = '/data/spider/log/max_times_out_log'
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        # data = '[' + str(time.strftime('%Y-%m-%d %H:%M:%S')) + '] ' + content
        content = content + ","
        file_name = file_path + '/' + date + "times_out.log"
        with open(file_name, 'a+') as f:
            f.write(content + "\r\n")