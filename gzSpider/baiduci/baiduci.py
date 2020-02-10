import random
import time

import redis
import requests
import chardet
import urllib3
import os
import jieba
from collections import Counter
from bs4 import BeautifulSoup

urllib3.disable_warnings()

rootPath = os.path.dirname(os.path.realpath(__file__))

userDictPath = os.path.join(rootPath, 'jiebadic.csv')
if not os.path.exists(userDictPath):
    with open(userDictPath, 'a+') as f:
        f.write("")

jieba.load_userdict(userDictPath)
jieba.initialize()

USER_AGENT_LIST = [
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
            "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        ]

keywords_list = ['娱乐平台大全', '黑娱乐平台', '庆彩娱乐平台', '易购娱乐平台', '金城娱乐平台', '人娱乐平台', '经纬娱乐平台', 'cnc娱乐平台', 'sky娱乐平台',
                 '宝马娱乐平台', 't6娱乐平台', '亿博娱乐平台', 'k彩娱乐平台', '捕鱼娱乐平台', '帝一娱乐平台', '新火娱乐平台', '吉祥娱乐平台', '如意娱乐平台', '宝盈娱乐平台',
                 '云顶娱乐平台]']

for keywords in keywords_list:
    items_list = list()
    for page in range(0, 51, 50):
        url = 'http://www.baidu.com/s?wd=' + keywords + '&rn=50' + 'pn=' + str(page)
        response = requests.get(url)

        htmlEncoded = response.content

        detectResult = chardet.detect(htmlEncoded)

        encoding = detectResult['encoding']

        html = str(htmlEncoded, encoding)

        soup = BeautifulSoup(html, 'html.parser')

        items = soup.select('h3 a')
        items_list.extend(items)
    allTitleStr = ''

    for item in items_list:
        resultRedirectUrl = item.attrs['href']
        print(resultRedirectUrl)
        if 'http://' in resultRedirectUrl or \
                'https://' in resultRedirectUrl:

            itemHeadRes = requests.head(resultRedirectUrl, verify=False)

            itemUrl = itemHeadRes.headers['Location']

            try:

                itemRes = requests.get(itemUrl, verify=False)

                if itemRes.status_code == 200:

                    itemHtmlEncoding = chardet.detect(itemRes.content)['encoding']

                    itemHtml = str(itemRes.content, itemHtmlEncoding, errors='ignore')

                    itemSoup = BeautifulSoup(itemHtml, 'html.parser')

                    if itemSoup.title is not None:
                        itemTitle = itemSoup.title.text.strip()

                        print(itemTitle)

                        allTitleStr += itemTitle + ' '
            except:
                continue

    titleWords = [word for word in jieba.lcut(allTitleStr, cut_all=False) if len(word) > 1]

    titleWordsDic = dict(Counter(titleWords))

    titleWordsSortedList = sorted(titleWordsDic.items(), key=lambda x: x[1], reverse=True)

    for item in titleWordsSortedList:
        if item[0] not in ['中国人民对外友好协会', '1451', 'v1.0', 'hjha333', '.........', '289', 'app1.0', 'g22com', 'g22hf',
                           '..', 'cc', '888dafa', 'ag', 'www ', 'app', 'com ', 'g22', 'www', '888', 'com']:
            print(item[0], ':', item[1])
            data = item[0] + ':' + str(item[1])
            file_name = rootPath + '/' + keywords + ".csv"
            with open(file_name, 'a+') as f:
                f.write(data + "\r\n")
