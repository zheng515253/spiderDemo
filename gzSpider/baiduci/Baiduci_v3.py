
"""
百度资讯的爬取
"""
import csv
import os
import random
import time

import chardet
import jieba
import requests
from bs4 import BeautifulSoup
from collections import Counter

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
stopwords = {}.fromkeys(['a8', '2018', '2018', '201820190605A', '99', 'mzz', '10', '...', '..._',
                         '365', 'PK', 'mg', '888', 'mod', 'Realm', 'of', 'Swords'])


class BaiDuoCi:
    def __init__(self):
        self.headers = {
            "User-Agent": random.choice(USER_AGENT_LIST)
        }
        self.url = 'https://www.baidu.com/s?rtt=1&bsst=1&cl=2&tn=news&word={}&tngroupname=organic_news&rsv_dl=news_b_pn&pn={}'
        self.tag = True

    def request(self, wd, page):
        time.sleep(0.5)
        url = self.url.format(wd, page)
        resp = requests.get(url=url, headers=self.headers)
        html_encode = resp.content
        items = []
        try:
            detect_result = chardet.detect(html_encode)
            encoding = detect_result["encoding"]
            html = str(html_encode, encoding)
            soup = BeautifulSoup(html, 'html.parser')
            items = soup.select('h3 a')
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

    def fenci(self, str_word):
        segs = jieba.cut(str_word, cut_all=False)
        final = ''
        for seg in segs:
            if seg not in stopwords:
                final += seg + ' '
        words_gen = [word for word in jieba.cut(final) if len(word) > 1]
        word_dic = dict(Counter(words_gen))
        word_sorted_list = sorted(word_dic.items(), key=lambda x: x[1], reverse=True)
        return word_sorted_list

    def save_data(self, data, name):
        file_name = rootPath + "/csv_zx/" + name + '.csv'
        out = open(file_name, 'a+', newline='')
        csv_write = csv.writer(out, dialect='excel')
        csv_write.writerow(['热词名称', '数量'])
        for i in data:
            content = [i[0], i[1]]
            csv_write.writerow(content)

    def save_title(self, page, title, keywords):
        file_name = rootPath + '/text_zx/' + keywords + ".text"
        print(file_name)
        with open(file_name, 'a+', encoding="utf-8") as f:
            data = '第{}页的标题为： {}'.format(int(page/10), title)
            print(data)
            f.write(data + '\r\n')

    def run(self):
        keywords_list = ['金城娱乐平台', '黑娱乐平台', '娱乐平台大全', '人娱乐平台', '庆彩娱乐平台', '经纬娱乐平台', 'cnc娱乐平台', 'sky娱乐平台',
                 '宝马娱乐平台', 't6娱乐平台', '亿博娱乐平台', 'k彩娱乐平台', '捕鱼娱乐平台', '帝一娱乐平台', '新火娱乐平台', '吉祥娱乐平台', '如意娱乐平台', '宝盈娱乐平台',
                 '云顶娱乐平台']

        total_count = 501
        for keywords in keywords_list:
            self.tag = True
            all_title_str = ''
            for page in range(0, total_count, 10):
                if not self.tag:
                    break
                list_title = self.request(keywords, page)
                if not list_title:
                    continue
                for i in list_title:
                    title = i.text.strip()
                    self.save_title(page, title, keywords)
                    all_title_str += title + ' '
            word_list = self.fenci(all_title_str)
            self.save_data(word_list, keywords)


if __name__ == '__main__':
    baidu = BaiDuoCi()
    baidu.run()