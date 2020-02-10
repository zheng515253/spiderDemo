# -*- coding : utf-8 -*-
# coding: utf-8
import json
import os
import random
import re
import time

import chardet
import redis
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

rootPath = os.path.dirname(os.path.realpath(__file__))

USER_AGENT_LIST = [
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
    "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
    "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
]


class Music:
    def __init__(self):
        self.headers = {
            "User-Agent": random.choice(USER_AGENT_LIST)
        }
        self.tag = True
        self.rdp = redis.ConnectionPool(host='127.0.0.1', port=6379,  decode_responses=True)
        self.redis_client = redis.StrictRedis(connection_pool=self.rdp)
        self.key = 'music_v1'
        self.key_queue = 'wangyiv1_queue'
        self.url = 'https://y.qq.com/portal/singer_list.html#area=200&page=1&index=1&'

    def ChromeDriverNOBrowser(self):
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        driverChrome = webdriver.Chrome(chrome_options=chrome_options)
        return driverChrome

    def ChromeDriverBrowser(self):
        driverChrome = webdriver.Chrome()
        return driverChrome

    def request(self, url):
        items = []
        try:
            brower = self.ChromeDriverNOBrowser()
            brower.get(url)
            time.sleep(5)
            soup = BeautifulSoup(brower.page_source, 'html.parser')
            items = soup.find_all('li', class_="singer_list_txt__item")
        except Exception as e:
            print('失败原因1：{}'.format(e))
            return items
        else:
            return items

    def request_music(self, sigure):
        items = list()
        url = 'https://u.y.qq.com/cgi-bin/musicu.fcg?data={"comm":{"ct":14,"cv":0},"singer":{"method":"get_singer_detail_info","param":{"sort":5,"singermid":"'+ sigure +'","sin":0,"num":100},"module":"music.web_singer_info_svr"}}'
        try:
            resp = requests.get(url=url, headers=self.headers)
            time.sleep(random.choice([1.5, 2.6, 1.7, 0.9, 4.1, 3.1, 3.5, 3.8]))
            result = resp.content.decode()
            result = json.loads(result)
            song_list = result['singer']['data']['songlist']
            for i in song_list:
                items.append(i['name'])
        except Exception as e:
            print('失败原因2：{}'.format(e))
            return items
        else:
            return items

    def save_title(self, data):
        file_name = rootPath + "音乐名称.text"
        with open(file_name, 'a+', encoding="utf-8") as f:
            f.write(data + '\r\n')

    def get_sigure(self, url_str):
        url_str_1 = url_str.split("#")
        url_str_2 = url_str_1[0].split('/')
        url_str_3 = url_str_2[-1]
        url_str_4 = url_str_3.split('.')
        return url_str_4[0]

    def request_detail(self):
        # base_url = 'https://y.qq.com/portal/singer_list.html#area=200&page='
        # for i in range(1, 28):
        #     tag = True
        #     for j in range(1, 6):
        #         if not tag:
        #             break
        #         url_index = base_url + str(j) +'&index=' + str(i) + '&'
        #         print("当前爬取的url：", url_index)
        #         list_a = self.request(url_index)
        #         if list_a:
        #             print("list_a len:", len(list_a))
        #             if len(list_a) < 80:
        #                 tag = False
        #             for p in list_a:
        #                 url_detail = p.a.attrs['href']
        #                 self.redis_client.rpush(self.key_queue, url_detail)

        while True:
            url = self.redis_client.lpop(self.key_queue)
            if not url:
                break
            sigure = self.get_sigure(url)
            print("当前爬取的url************************************： " + sigure)
            for name in self.request_music(sigure):
                print(name)
                self.redis_client.hset(self.key, name, name)

    def run(self):
        self.request_detail()


if __name__ == '__main__':
    music = Music()
    music.run()