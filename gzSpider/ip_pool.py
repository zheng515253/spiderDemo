import urllib.request
import urllib.parse
import time
from multiprocessing import Pool  # 多进程
import random
from lxml import etree  # 解析


def GetUserAgent():
    '''
    功能：随机获取HTTP_User_Agent
    '''
    user_agents=[
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
    "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
    "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
    "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
    "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
    "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
    "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 LBBROWSER",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; 360SE)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
    "Mozilla/5.0 (iPad; U; CPU OS 4_2_1 like Mac OS X; zh-cn) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148 Safari/6533.18.5",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b13pre) Gecko/20110307 Firefox/4.0b13pre",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:16.0) Gecko/20100101 Firefox/16.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
    "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10"
    ]
    user_agent = random.choice(user_agents)
    return user_agent


class IpPool:
    def __init__(self):
        self.ip_url = "http://www.xicidaili.com/nn/"

    def getProxies(self):
        '''
        功能：爬取西刺高匿IP构造原始代理IP池
        '''
        init_proxies = []
        # 爬取前十页
        for i in range(1, 11):
            url = self.ip_url + str(i)
            user_agent = GetUserAgent()
            headers = ("User-Agent", user_agent)
            opener = urllib.request.build_opener()
            opener.addheaders = [headers]
            data = ''
            try:
                data = opener.open(url, timeout=5).read()
            except Exception as er:
                print("爬取的时候发生错误，具体如下：")
                print(er)
            selector = etree.HTML(data)
            ip_addres = selector.xpath('//tr[@class="odd"]/td[2]/text()')  # IP地址
            port = selector.xpath('//tr[@class="odd"]/td[3]/text()')  # 端口
            sur_time = selector.xpath('//tr[@class="odd"]/td[9]/text()')  # 存活时间
            ver_time = selector.xpath('//tr[@class="odd"]/td[10]/text()')  # 验证时间
            for j in range(len(ip_addres)):
                ip = ip_addres[j]+":"+port[j]
                init_proxies.append(ip)
                # 输出爬取数据
                print(ip_addres[j]+"\t\t"+port[j]+"\t\t"+sur_time[j]+"\t"+ver_time[j])
        return init_proxies

    def testProxy(self, curr_ip):
        '''
        功能：验证IP有效性
        @curr_ip：当前被验证的IP
        '''
        tmp_proxies = []
        # socket.setdefaulttimeout(10)  #设置全局超时时间
        tarURL = "http://www.baidu.com/"
        user_agent = GetUserAgent()
        proxy_support = urllib.request.ProxyHandler({"http": curr_ip})
        opener = urllib.request.build_opener(proxy_support)
        opener.addheaders = [("User-Agent", user_agent)]
        urllib.request.install_opener(opener)
        try:
            res = urllib.request.urlopen(tarURL, timeout=5).read()
            if len(res) != 0:
                tmp_proxies.append(curr_ip)
        except urllib.error.URLError as er2:
            if hasattr(er2, "code"):
                print("验证代理IP（" + curr_ip + "）时发生错误（错误代码）：" + str(er2.code))
            if hasattr(er2, "reason"):
                print("验证代理IP（" + curr_ip + "）时发生错误（错误原因）：" + str(er2.reason))
        except Exception as er:
            print("验证代理IP（" + curr_ip + "）时发生如下错误）：")
            print(er)
        time.sleep(2)
        return tmp_proxies

    def mulTestProxies(self, init_proxies):
        '''
        功能：多进程验证IP有效性
        @init_proxies：原始未验证代理IP池
        '''
        pool = Pool(processes=7)
        fl_proxies = pool.map(self.testProxy, init_proxies)
        pool.close()
        pool.join()  # 等待进程池中的worker进程执行完毕
        return fl_proxies


if __name__ == '__main__':
    ip_pool = IpPool()
    init_proxies = ip_pool.getProxies()
    tmp_proxies = ip_pool.mulTestProxies(init_proxies)
    proxy_addrs = []
    for tmp_proxy in tmp_proxies:
        if len(tmp_proxy) != 0:
            proxy_addrs.append(tmp_proxy)
