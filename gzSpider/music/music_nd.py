# -*- coding : utf-8 -*-
# coding: utf-8
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
        self.key_queue = 'music_nd_queue'
        self.url = 'http://www.5nd.com/paihang/'

    def ChromeDriverNOBrowser(self):
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        driverChrome = webdriver.Chrome(chrome_options=chrome_options)
        return driverChrome


    def ChromeDriverBrowser(self):
        driverChrome = webdriver.Chrome()
        return driverChrome

    def request(self):
        items = []
        try:
            resp = requests.get(url=self.url, headers=self.headers)
            time.sleep(2)
            html_encode = resp.content
            detect_result = chardet.detect(html_encode)
            encoding = detect_result["encoding"]
            html = str(html_encode, encoding)
            soup = BeautifulSoup(html, 'html.parser')
            soup1 = soup.find_all('div', class_="rankList")
            if soup1:
                items = soup1[0].select('li a')
        except Exception as e:
            print('失败原因1：{}'.format(e))
            return items
        else:
            return items

    def request_music(self, url):
        # resp = requests.get(url=url, headers=self.headers)
        items = list()
        try:
            brower = self.ChromeDriverNOBrowser()
            brower.get(url)
            time.sleep(2)
            soup = BeautifulSoup(brower.page_source, 'html.parser')
            soup2 = soup.find_all('div', class_="songR")
            if soup2:
                items = soup2[0].find_all('a', class_="rankNane")
        except Exception as e:
            print('失败原因2：{}'.format(e))
            return items
        else:
            return items

    def save_title(self, data):
        file_name = rootPath + "音乐名称.text"
        with open(file_name, 'a+', encoding="utf-8") as f:
            f.write(data + '\r\n')

    def request_detail(self):
        # list_a = self.request()
        # if list_a:
        #     for i in list_a:
        #         url = i.attrs['href']
        #         self.redis_client.rpush(self.key_queue, url)
        while True:
            url = self.redis_client.lpop(self.key_queue)
            if not url:
                break
            url = 'http://www.5nd.com' + url
            print("当前爬取的url：" + url)
            for i in self.request_music(url):
                name = i.text.strip()
                name_list = name.split("-")
                name = name_list[0]
                print(name)
                self.redis_client.hset(self.key, name, name)

    def run(self):
        self.request_detail()


if __name__ == '__main__':
    music = Music()
    music.run()