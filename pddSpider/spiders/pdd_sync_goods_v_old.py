# -*- coding: utf-8 -*-
import scrapy
import json, time, sys, random, pyssdb, re
from spider.items import SpiderItem

# from mq.pdd_goods_mq import pdd_goods_mq

'''从队列同步产品信息'''


class PddSyncGoodsSpider(scrapy.Spider):
    name = 'pdd_sync_goods_v_old'
    goods_list = []
    ssdb = ''
    queue_name = 'pdd_sync_goods_list'

    def __init__(self):
        self.ssdb = pyssdb.Client('172.16.0.5', 8888)

    def start_requests(self):
        search = True
        while search:
            headers = self.make_headers()

            goods_ids = self.ssdb.qpop_front(self.queue_name, 1)
            if goods_ids == True or goods_ids == None:
                time.sleep(30)
                continue

            if type(goods_ids) == bytes:  ##只有1条数据时返回的是bytes格式，转换成list
                goods_ids = [goods_ids]

            for goods_id in goods_ids:
                goods_id = goods_id.decode('utf-8')

                url = 'http://mobile.yangkeduo.com/goods.html?goods_id=' + str(goods_id)
                print(url)
                yield scrapy.Request(url, callback=self.parse, headers=headers, dont_filter=True)

    # for i in range(self.start, self.end):
    # 	if i >= self.end - 1: ##判断是否是最后一次循环
    # 		is_end = True

    # 	meta = {'is_end':is_end}

    # 	headers = self.make_headers()
    # 	url = 'http://mobile.yangkeduo.com/goods.html?goods_id='+str(i)

    # 	yield scrapy.Request(url, meta = meta, callback=self.parse, headers=headers,dont_filter=True)

    def parse(self, response):
        pass
        content = response.body.decode('utf-8')
        a = re.search('window\.rawData= (.*)\;\s*\<\/script\>', content)
        if a:
            content = json.loads(a.group(1))
            print(content)
            if 'goods' not in content.keys():
                return None

            goods = content['goods']

            goods_data = SpiderItem()
            goods_data['goods_id'] = goods['goodsID']
            goods_data['mall_id'] = goods['mallID']
            goods_data['goods_type'] = goods['goodsType']
            goods_data['category1'] = str(goods['catID1'])
            goods_data['category2'] = str(goods['catID2'])
            goods_data['category3'] = str(goods['catID3'])
            goods_data['goods_name'] = goods['goodsName']
            goods_data['market_price'] = goods['marketPrice']
            goods_data['max_group_price'] = goods['maxOnSaleGroupPrice']
            goods_data['min_group_price'] = goods['minOnSaleGroupPrice']
            goods_data['max_normal_price'] = goods['maxOnSaleNormalPrice']
            goods_data['min_normal_price'] = goods['minOnSaleNormalPrice']
            goods_data['thumb_url'] = goods['thumbUrl']
            goods_data['publish_date'] = self.get_goods_publish_date(goods['topGallery'], goods['detailGallery'],
                                                                     goods['skus'])
            goods_data['total_sales'] = int(goods['sales'])  ##总销量

            if goods['isOnSale'] and goods['isGoodsOnSale']:
                goods_data['is_on_sale'] = 1
            else:
                goods_data['is_on_sale'] = 0

            # ##获取核算价
            goods_data['price'] = self.get_goods_price(goods['skus'], goods['sales'])
            goods_data['total_amount'] = float(goods_data['total_sales'] * float(goods_data['price']))  ##总销售额

            yield goods_data

    """从图片链接中获取产品的发布日期"""

    def get_goods_publish_date(self, images, content_images, skus_images):
        datetime = []

        for i in content_images:
            url = i['url']
            if not url:
                continue
            date = url.split('/')[4]

            try:
                # 转换成时间数组
                timeArray = time.strptime(date, "%Y-%m-%d")
                # 转换成时间戳
                timestamp = int(time.mktime(timeArray))
                datetime.append(timestamp)
            except Exception as e:
                continue

        for i in skus_images:
            url = i['thumbUrl']
            if not url:
                continue
            date = url.split('/')[4]

            try:
                # 转换成时间数组
                timeArray = time.strptime(date, "%Y-%m-%d")
                # 转换成时间戳
                timestamp = int(time.mktime(timeArray))
                datetime.append(timestamp)
            except Exception as e:
                continue

        if datetime:
            return min(datetime)
        else:
            ##返回能获取到的最早的产品发布时间
            return 1489200000

    '''/**获取核算价 先按照销量最高的价格 若销量为0 则为价格最低的作为核算价**/'''

    def get_goods_price(self, goods_skus, goods_sold_num):
        # print(goods_skus[0])
        if goods_sold_num:
            goods_skus.sort(key=lambda x: -x['soldQuantity'])
        else:
            goods_skus.sort(key=lambda x: x['groupPrice'])

        if goods_skus:
            return goods_skus[0]['groupPrice']
        else:
            return 0

    '''生成headers头信息'''

    def make_headers(self):
        chrome_version = str(random.randint(59, 63)) + '.0.' + str(random.randint(1000, 3200)) + '.94'
        headers = {
            "Host": "yangkeduo.com",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Host": "yangkeduo.com",
            "Referer": "http://yangkeduo.com",
            "Connection": "keep-alive",
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/' + chrome_version + ' Safari/537.36',
        }

        ip = str(random.randint(100, 200)) + '.' + str(random.randint(1, 255)) + '.' + str(
            random.randint(1, 255)) + '.' + str(random.randint(1, 255))
        headers['CLIENT-IP'] = ip
        headers['X-FORWARDED-FOR'] = ip
        return headers