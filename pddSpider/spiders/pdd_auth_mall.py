# -*- coding: utf-8 -*-
import logging

import scrapy
import json, time, sys, random, re, setting, pyssdb, redis
from spider.items import MallItem
from urllib import parse, request
from scrapy.utils.project import get_project_settings

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import TimeoutError, TCPTimedOutError, ConnectionRefusedError, DNSLookupError
from twisted.web._newclient import ResponseFailed, ResponseNeverReceived
from scrapy.utils.response import response_status_message  # 获取错误代码信息

'''获取店铺信息及销量记录'''


class PddAuthMallSpider(scrapy.Spider):
    name = 'pdd_auth_mall'
    mall_id_list = 'pdd_auth_mall_list'  # 复制店铺队列（不使用hash！）
    hash_num = 0
    process_nums = 1
    limit = 500
    start_mall_id = ''  ##起始查询的店铺key
    proxy_start_time = 0
    proxy_ip_list = []
    current_proxy = ''
    proxy_count = 0
    # handle_httpstatus_list = [429]

    alias_name = 'mall_info'

    custom_settings = {
        # 'LOG_FILE':'',
        # 'LOG_LEVEL':'DEBUG',
        # 'LOG_ENABLED':True,
        'RETRY_ENABLED': False,  # 是否重试
        'DOWNLOAD_TIMEOUT': 2,  # 超时时间
        # 'RETRY_TIMES': 20,
        'CONCURRENT_REQUESTS': 10,
        'DOWNLOAD_DELAY': 0,
    }

    def __init__(self, hash_num=0, process_nums=1):
        self.ssdb_client = pyssdb.Client(get_project_settings().get('SSDB_HOST'), 8888)
        self.hash_num = int(hash_num)  ##当前脚本号
        self.process_nums = int(process_nums)  ##脚本总数
        # 创建连接池
        pool = redis.ConnectionPool(host=get_project_settings().get('PROXY_REDIS_HOST'), port=6379, db=10,
                                    password=get_project_settings().get('PROXY_REDIS_AUTH'), decode_responses=True)
        # 创建链接对象
        self.redis_client = redis.Redis(connection_pool=pool)

    def start_requests(self):
        mall_nums = self.limit * int(self.process_nums)  ##一次查询的数量
        is_end = False

        while not is_end:
            mall_ids = self.ssdb_client.qpop_front(self.mall_id_list, 60)
            if not mall_ids:  ##没有数据返回
                is_end = True
                continue

            for mall_id in mall_ids:
                mall_id = mall_id.decode('utf-8')
                if not mall_id:
                    is_end = True
                    continue

                headers = self.make_headers()
                meta = {'proxy': self.get_proxy_ip(False), 'mall_id': mall_id}
                yield scrapy.Request(
                    'http://api.yangkeduo.com/mall/' + str(mall_id) + '/info?query_mall_favorite_coupon=true',
                    meta=meta, callback=self.parse, headers=headers, dont_filter=True, errback=self.errback_httpbin)
            time.sleep(1)

    def parse(self, response):
        mall_info = response.body.decode('utf-8')  ##bytes转换为str
        mall_info = json.loads(mall_info)  ##str转为字典

        mall_data = MallItem()
        mall_data['mall_id'] = mall_info['mall_id']
        mall_data['mall_name'] = mall_info['mall_name']
        mall_data['goods_num'] = mall_info['goods_num']
        mall_data['score_avg'] = mall_info['score_avg']
        mall_data['mall_sales'] = mall_info['mall_sales']
        mall_data['is_open'] = mall_info['is_open']
        mall_data['status'] = mall_info['status']
        mall_data['logo'] = mall_info['logo']
        # refund_address 			= mall_info['refund_address']
        # address_info 			= self.get_address_info(refund_address)
        mall_data['province'] = ''  # address_info['province']
        mall_data['city'] = ''  # address_info['city']
        mall_data['area'] = ''  # address_info['area']
        mall_data['street'] = ''  # address_info['street']
        logging.info(mall_data)
        # yield mall_data

    '''获取地址信息'''

    def get_address_info(self, refund_address):
        pass
        patten = re.compile(r'自治区|壮族自治区|回族自治区|维吾尔自治区|特别行政区|自治州')  ##规范化省份信息
        refund_address = patten.sub('', refund_address)

        address = {'province': '', 'city': '', 'area': '', 'street': ''}

        if refund_address and len(refund_address) >= 7:  ##只有地址存在且地址大于固定长度才截取，避免无效地址
            pass
            province = self.get_address_province(refund_address)  ##获取地址中的省份
            # print(province)
            if province:
                address['province'] = province
                refund_address = refund_address[len(province):]  ##地址信息剔除省份
                if province in ['北京', '上海', '天津', '重庆']:
                    sheng = '市'
                else:
                    sheng = '省'
                refund_address = refund_address.replace(sheng, '')
                city = self.get_address_city(province, refund_address)  ##获取地址中城市信息
                if city:
                    refund_address = refund_address[len(city):]  ##地址信息剔除城市
                else:
                    city = province

                address['city'] = city
                if refund_address[0] == '市':
                    refund_address = refund_address[1:]

                matches = re.search('.*?(市|地区|区|县|镇)', refund_address)
                if matches:
                    area = matches.group(0)
                    street = refund_address[len(area):]
                else:
                    area = '';
                    street = refund_address
                address['area'] = area
                address['street'] = street
        return address

    '''获取地址中的省份信息'''

    def get_address_province(self, address):
        province = ''
        all_province = self.get_all_province()
        for i in all_province:
            position = address.find(i)
            if position != -1:
                province = address[position:len(i)]
                break
        return province

    '''获取地址中的省份信息'''

    def get_address_city(self, province, address):
        city = ''
        all_city = self.get_province_city(province)
        for i in all_city:
            position = address.find(i)
            if position != -1:
                city = address[position:len(i)]
                break
        return city

    def get_all_province(self):
        return ['北京', '上海', '天津', '重庆', '香港', '澳门', '台湾', '河北', '山西', '辽宁', '吉林', '黑龙江', '江苏', '浙江', '安徽', '福建', '江西',
                '山东', '河南', '湖北', '湖南', '广东', '广西', '海南', '四川', '贵州', '云南', '西藏', '陕西', '甘肃', '青海', '内蒙古', '宁夏', '新疆']

    '''获取省份对应的城市信息'''

    def get_province_city(self, province):
        city_relation = {
            '北京': ['北京'],
            '天津': ['天津'],
            '上海': ['上海'],
            '重庆': ['重庆'],
            '河北': ['石家庄', '唐山', '秦皇岛', '邯郸', '邢台', '保定', '张家口', '承德', '沧州', '廊坊', '衡水'],
            '山西': ['太原', '大同', '阳泉', '长治', '晋城', '朔州', '晋中', '运城', '忻州', '临汾', '吕梁'],
            '台湾': ['台北', '高雄', '基隆', '台中', '台南', '新竹', '嘉义', '台北', '宜兰', '桃园', '新竹', '苗栗', '台中', '彰化', '南投', '云林', '嘉义',
                   '台南', '高雄', '屏东', '澎湖', '台东', '花莲'],
            '辽宁': ['沈阳', '大连', '鞍山', '抚顺', '本溪', '丹东', '锦州', '营口', '阜新', '辽阳', '盘锦', '铁岭', '朝阳', '葫芦岛'],
            '吉林': ['长春', '四平', '辽源', '通化', '白山', '松原', '白城', '延边', '吉林'],
            '黑龙江': ['哈尔滨', '齐齐哈尔', '鹤岗', '双鸭山', '鸡西', '大庆', '伊春', '牡丹江', '佳木斯', '七台河', '黑河', '绥化', '大兴安岭'],
            '江苏': ['南京', '无锡', '徐州', '常州', '苏州', '南通', '连云港', '淮安', '盐城', '扬州', '镇江', '泰州', '宿迁'],
            '浙江': ['杭州', '宁波', '温州', '嘉兴', '湖州', '绍兴', '金华', '衢州', '舟山', '台州', '丽水'],
            '安徽': ['合肥', '芜湖', '蚌埠', '淮南', '马鞍山', '淮北', '铜陵', '安庆', '黄山', '滁州', '阜阳', '宿州', '巢湖', '六安', '亳州', '池州',
                   '宣城'],
            '福建': ['福州', '厦门', '莆田', '三明', '泉州', '漳州', '南平', '龙岩', '宁德'],
            '江西': ['南昌', '景德镇', '萍乡', '九江', '新余', '鹰潭', '赣州', '吉安', '宜春', '抚州', '上饶'],
            '山东': ['济南', '青岛', '淄博', '枣庄', '东营', '烟台', '潍坊', '济宁', '泰安', '威海', '日照', '莱芜', '临沂', '德州', '聊城', '滨州',
                   '荷泽'],
            '河南': ['郑州', '开封', '洛阳', '平顶山', '安阳', '鹤壁', '新乡', '焦作', '濮阳', '许昌', '漯河', '三门峡', '南阳', '商丘', '信阳', '周口',
                   '驻马店'],
            '湖北': ['武汉', '黄石', '十堰', '宜昌', '襄樊', '鄂州', '荆门', '孝感', '荆州', '黄冈', '咸宁', '随州', '恩施', '仙桃', '潜江', '天门',
                   '神农架'],
            '湖南': ['长沙', '株洲', '湘潭', '衡阳', '邵阳', '岳阳', '常德', '张家界', '益阳', '郴州', '永州', '怀化', '娄底', '湘西'],
            '广东': ['广州', '深圳', '珠海', '汕头', '韶关', '佛山', '江门', '湛江', '茂名', '肇庆', '惠州', '梅州', '汕尾', '河源', '阳江', '清远', '东莞',
                   '中山', '潮州', '揭阳', '云浮', '普宁'],
            '甘肃': ['兰州', '金昌', '白银', '天水', '嘉峪关', '武威', '张掖', '平凉', '酒泉', '庆阳', '定西', '陇南', '临夏', '甘南'],
            '四川': ['成都', '自贡', '攀枝花', '泸州', '德阳', '绵阳', '广元', '遂宁', '内江', '乐山', '南充', '眉山', '宜宾', '广安', '达州', '雅安',
                   '巴中', '资阳', '阿坝', '甘孜', '凉山'],
            '贵州': ['贵阳', '六盘水', '遵义', '安顺', '铜仁', '毕节', '黔西南', '黔东南', '黔南'],
            '海南': ['海口', '三亚', '五指山', '琼海', '儋州', '文昌', '万宁', '东方', '澄迈', '定安', '屯昌', '临高', '白沙', '昌江', '乐东', '陵水',
                   '保亭', '琼中'],
            '云南': ['昆明', '曲靖', '玉溪', '保山', '昭通', '丽江', '思茅', '临沧', '楚雄', '红河', '文山', '西双版纳', '大理', '德宏', '怒江', '迪庆'],
            '青海': ['西宁', '海东', '海北', '黄南', '海南', '果洛', '玉树', '海西蒙古族'],
            '陕西': ['西安', '铜川', '宝鸡', '咸阳', '渭南', '延安', '汉中', '榆林', '安康', '商洛'],
            '广西': ['南宁', '柳州', '桂林', '梧州', '北海', '防城港', '钦州', '贵港', '玉林', '百色', '贺州', '河池', '来宾', '崇左'],
            '西藏': ['拉萨', '昌都', '山南', '日喀则', '那曲', '阿里', '林芝'],
            '宁夏': ['银川', '石嘴山', '吴忠', '固原', '中卫'],
            '新疆': ['乌鲁木齐', '石河子', '克拉玛依', '伊犁', '巴音郭楞', '昌吉', '克孜勒', '博尔塔拉', '吐鲁番', '哈密', '喀什', '和田', '阿克苏'],
            '内蒙古': ['呼和浩特', '包头', '乌海', '赤峰', '通辽', '鄂尔多斯', '呼伦贝尔', '乌兰察布', '巴彦绰尔', '兴安', '阿拉善', '锡林格勒', '满洲里', '二连浩特'],
            '香港': ['香港'],
            '澳门': ['澳门'],
        }
        return city_relation[province]

    '''生成headers头信息'''

    def make_headers(self):
        # chrome_version   = str(random.randint(59,63))+'.0.'+str(random.randint(1000,3200))+'.94'
        headers = {
            # "Host":"yangkeduo.com",
            # "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            # "Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
            # "Accept-Encoding":"gzip, deflate",
            # "Host":"yangkeduo.com",
            # "Referer":"http://yangkeduo.com",
            # "Connection":"keep-alive",
            # 'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/'+chrome_version+' Safari/537.36',
            # "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            # "Accept-Language":"zh-CN,zh;q=0.9,en;q=0.8",
            # "Accept-Encoding":"gzip, deflate",
            # "Host":"yangkeduo.com",
            "Referer": "Android",
            # "Connection":"keep-alive",
            "Cookie": 'api_uid=' + str(self.ssdb_client.get('pdd_api_uid')),
            'User-Agent': setting.setting().get_default_user_agent(),
            # 'User-Agent':'Mozilla/5.0 (iPhone; CPU iPhone OS 11_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15F79 ===  iOS/11.4 Model/iPhone9,1 BundleID/com.xunmeng.pinduoduo AppVersion/4.15.0 AppBuild/1807251632 cURL/7.47.0',
            # 'AccessToken':'',
        }

        # ip = str(random.randint(100, 200))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))+'.'+str(random.randint(1, 255))
        # headers['CLIENT-IP'] 	=	ip
        # headers['X-FORWARDED-FOR']=	ip
        return headers

    def get_proxy_ip(self, refresh):
        if self.current_proxy != '' and not refresh and self.proxy_count < 30:
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
            # print('proxy_count', len(self.proxy_ip_list))

            ip = random.choice(self.proxy_ip_list)
            self.current_proxy = ip
        logging.debug(json.dumps({
            'ip': ip,
            'count': self.proxy_count
        }))

        # print(ip, self.proxy_count)

        return 'http://' + ip

    def get_ssdb_proxy_ip(self):
        ips = self.redis_client.hkeys('proxy_ip_hash_fy')
        res = []
        for index in range(len(ips)):
            if index % self.process_nums != self.hash_num:
                continue
            res.append(ips[index])
        # print(res)
        if res:
            return res
        else:
            return []

    def errback_httpbin(self, failure):
        request = failure.request
        if failure.check(HttpError):
            response = failure.value.response
            # print( 'errback <%s> %s , response status:%s' % (request.url, failure.value, response_status_message(response.status)) )
            self.err_after(request.meta)
        elif failure.check(ResponseFailed):
            # print('errback <%s> ResponseFailed' % request.url)
            self.err_after(request.meta, True)

        elif failure.check(ConnectionRefusedError):
            # print('errback <%s> ConnectionRefusedError' % request.url)
            self.err_after(request.meta, True)

        elif failure.check(ResponseNeverReceived):
            # print('errback <%s> ResponseNeverReceived' % request.url)
            self.err_after(request.meta)

        elif failure.check(TCPTimedOutError, TimeoutError):
            # print('errback <%s> TimeoutError' % request.url)
            self.err_after(request.meta, True)
        else:
            # print('errback <%s> OtherError' % request.url)
            self.err_after(request.meta)

    def err_after(self, meta, remove=False):
        proxy_ip = meta["proxy"]
        proxy_ip = proxy_ip.replace("http://", "").encode("utf-8")

        if remove and proxy_ip in self.proxy_ip_list:
            index = self.proxy_ip_list.index(proxy_ip)
            del self.proxy_ip_list[index]

        self.get_proxy_ip(True)

        mall_id = meta['mall_id']
        self.ssdb_client.qpush_back(self.mall_id_list, mall_id)  # 失败店铺ID重新放入队列

