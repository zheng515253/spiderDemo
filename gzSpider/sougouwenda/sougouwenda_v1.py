
"""
搜狗词的爬取
"""
import csv
import os
import random
import re
import time

import requests
from bs4 import BeautifulSoup
import urllib.parse

USER_AGENT_LIST = [
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
            "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        ]

rootPath = os.path.dirname(os.path.realpath(__file__))


class SouGoWenWen_v1:
    spider_name = '搜狗问问'

    def __init__(self):
        self.headers = {
            "User-Agent": random.choice(USER_AGENT_LIST),
            'Cookie': 'sw_uuid=3273955461; ABTEST=0|1562912310|v17; SNUID=80723F2A4E48C33BAF58BBBF4EE8EBF6; IPLOC=CN8100; SUID=CE3C71672313940A000000005D282636; ld=Nyllllllll2N3jYLlllllV1oJ0YlllllBT9D8kllll9lllllRllll5@@@@@@@@@@; SUV=1562912311779681; browerV=3; osV=1; LSTMV=1682%2C173; LCLKINT=4909; taspeed=taspeedexist; pgv_pvi=7106629632; pgv_si=s2438505472',
            'Host': 'www.sogou.com',
            'Referer': 'https://www.sogou.com/sogou?query=%E4%BD%93%E8%82%B2&ie=utf8&_ast=1562381568&_asf=null&w=01029901&pid=sogou-wsse-a9e18cb5dd9d3ab4&duppid=1&cid=&s_from=result_up&insite=wenwen.sogou.com'
        }
        self.url = 'https://www.sogou.com/sogou?query={}&pid=sogou-wsse-a9e18cb5dd9d3ab4&insite=wenwen.sogou.com&duppid=1&rcer=&page={}&ie=utf8'
        self.tag = True

    def request(self, keywords, page):
        time_list = [5, 6, 7, 8]
        time.sleep(random.choice(time_list))
        url = self.url.format(keywords, page)
        print("请求的url：", url)
        items = []
        try:
            resp = requests.get(url=url, headers=self.headers)
            html_str = resp.content
            soup = BeautifulSoup(html_str, 'html.parser')
            items = soup.find_all(id=re.compile('sogou_snapshot_'))
            if len(items) < 10:
                self.tag = False
            if page == 0:
                item_bottom = soup.select('th a')
                items.extend(item_bottom)
        except Exception as e:
            print('失败原因：{}'.format(e))
            return items
        else:
            return items

    def filter_article(self, data):
        """ 过滤文章 """
        filer_list = ['www', 'http', 'com']
        for i in filer_list:
            if data.__contains__(i):
                return False
        return True

    def parsr_article(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        pre_html = soup(class_='replay-info-txt answer_con')
        if pre_html:
            article = pre_html[0].text.strip()
            article.replace('彩票', 'CP')
            article.replace('博彩', 'BC')
            article.replace('时时彩', 'SSC')
            if self.filter_article(article):
                return re.sub('\s+', '。', article)
            else:
                return None
        else:
            return None

    def request_article(self, url):
        headers = {
            'User-Agent': random.choice(USER_AGENT_LIST),
        }
        try:
            resp = requests.get(url=url, headers=headers)
            time_list = [5, 6, 7, 8]
            time.sleep(random.choice(time_list))
            html_str = resp.content.decode()
        except Exception as e:
            print('请求文章失败原因：{}'.format(e))
        else:
            return self.parsr_article(html_str)

    def get_url(self, url):
        url_article = re.search(r'url=([\d\D]+?).htm', url)
        if url_article:
            url_article = url_article.group()
            url = urllib.parse.unquote(url_article).replace('url=', '')
            return url

    def break_rank(self, list_data):
        """ 打乱有序数组"""
        list_index = [i for i in range(len(list_data))]
        random.shuffle(list_index)
        list_new = list()
        for index in list_index:
            list_new.append(list_data[index])
        return list_new

    def carve_up(self, sentence):
        """ 切割句子"""
        sentence_len = len(sentence)
        sentence_list = list()
        if sentence_len > 150:
            cut_list = [i + '。 ' for i in sentence.split("。") if i]
            a = ''
            b = ''
            total_str = ''
            for i in cut_list:
                a += i
                total_str += i
                if len(a) > 150:
                    b += a
                    sentence_list.append(a)
                    a = ''

            end_str = total_str.replace(b, '')
            if end_str:
                sentence_list.append(end_str)
        else:
            sentence_list.append(sentence)
        return sentence_list

    def save_article(self, data, name):
        date = time.strftime('%Y-%m-%d')
        file_path = rootPath + '/spider_data'
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        file_name = file_path + '/' + self.spider_name + '_' + name + '_' + date + ".text"
        with open(file_name, 'a+', encoding='utf-8') as f:
            f.write(data + "\r\n")

    def run(self):  # '娱乐', '棋牌',  '体育', '登陆', '注册', '国际', '在线', '平台'
        keywords_list = ['娱乐', '棋牌',  '体育', '登陆', '注册', '国际', '在线', '平台']
        start_page = 151
        page_total = 201
        for keywords in keywords_list:
            for page in range(start_page, page_total):
                article_list = list()
                tag_list = self.request(keywords, page)
                for i in tag_list:
                    # print('111', i)
                    url = self.get_url(i.attrs['href'])
                    # print('222', url)
                    if url:
                        article = self.request_article(url)
                        if article:
                            art_list_temp = self.carve_up(article)
                            article_list.extend(art_list_temp)
                    else:
                        continue
                for article in self.break_rank(article_list):
                    print("当前文章长度为：{}   ".format(len(article)), article)
                    self.save_article(article, keywords)


if __name__ == '__main__':
    baidu = SouGoWenWen_v1()
    baidu.run()