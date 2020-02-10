# -*- coding : utf-8 -*-
# coding: utf-8
"""
判断当前词汇是满足百度搜索搜索需求
"""
import os
import random
import time

import chardet
import requests
from bs4 import BeautifulSoup

from judge_spider.constant import MAX_COUNT, MIN_COUNT, NEWS_WAREHOUSE

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


class JudgeSpierv2:
    def __init__(self):
        self.baidu_headers = {
            "User-Agent": random.choice(USER_AGENT_LIST),
            'Cokie': 'BAIDUID=F10B7427F80F49A6333C69BEFA3828D3:FG=1; PSTM=1563506567; BD_UPN=12314753; BIDUPSID=6A5E4F6889061C700606CA09656EC680; BDUSS=C1uaH44M1U4SEV-TmFVZW5tOEl1WUJrUTV5SEUxSWNoSE9Wemw4QnhkZTBhMlpkSVFBQUFBJCQAAAAAAAAAAAEAAABKqZs-v-zA1rXE0rvQ3gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAALTePl203j5dZ; H_PS_PSSID=1464_21103_18560_29522_29521_29098_29568_28834_29220_26350_22157; BDORZ=B490B5EBF6F3CD402E515D22BCDA1598; yjs_js_security_passport=ac9b418a1319b94d06b9a7515f3b28a08ddb23cb_1565158020_js; H_PS_645EC=d6detbk3NTTsGc6gXTAxdksNBqJSnfr4vxEpjw6%2BrcivI2wamuRW9V1kSPNTIFMGefnF',
            'Host': 'www.baidu.com',
        }
        self.baidu_url = 'http://www.baidu.com/s?wd={}'
        self.sougou_headers = {
            "User-Agent": random.choice(USER_AGENT_LIST),
            "Host": 'www.sogou.com',
            'Cokie': 'sw_uuid=2629301006; ssuid=7723011388; ABTEST=0|1563534275|v17; IPLOC=CN8100; SUID=CE3C71672613910A000000005D31A3C3; ld=bZllllllll2NGz5RlllllV1eoDGlllllBT9D8kllll9lllll9Zlll5@@@@@@@@@@; SUV=00563C7367713CCE5D31A3C5E5761182; usid=66focjNKf0tVGbU3; SNUID=2FDD9385E1E76E60FF3913B3E280B534; wuid=AAFpoKchKQAAAAqHS0ud8wEAkwA=; CXID=2206A2792D739576C5313048AFE0CE8C; SGS_FE_WAID=WAID2019080700000000000009483690; front_screen_resolution=1920*1080; FREQUENCY=1565164054332_2; sct=1; browerV=3; osV=1',
        }
        self.sougou_url = 'http://www.sogou.com/web?query={}'

    def get_file_name(self):
        file_list = list()
        path = rootPath + '/' + 'text'
        for root, dirs, files in os.walk(path):
            for file in files:
                file_list.append(os.path.join(root, file))
        return file_list

    def get_baidu_title(self, words):
        items = []
        items1 = []
        try:
            resp = requests.get(url=self.baidu_url.format(words), headers=self.baidu_headers, timeout=5)
            time.sleep(random.choice([0.8, 0.5, 0.3, 0.1, 0.2, 0.11, 0.16]))
            html_encode = resp.content
            detect_result = chardet.detect(html_encode)
            encoding = detect_result["encoding"]
            html = str(html_encode, encoding)
            soup = BeautifulSoup(html, 'html.parser')
            items = soup.select('h3 a')
            soup1 = soup.find_all(id='content_left')
            if soup1:
                items1 = soup1[0].select('a span')
        except Exception as e:
            print('失败原因：{}'.format(e))
            return items, items1
        else:
            return items, items1

    def request_baidu(self, words):
        words_list, sensitive_list = self.get_baidu_title(words)
        if words_list:
            a = 0
            b = 0
            for i in words_list:
                title = i.text.strip()
                if title.__contains__(words):
                    b += 1
                if self.is_forbidde(title):
                    a += 1
            for j in sensitive_list:
                title = j.text.strip()
                if self.is_forbidde(title):
                    a += 1
            if b >= MIN_COUNT and a <= MAX_COUNT:
                return False
            else:
                return True
        return True

    def get_words_list(self):
        file_path_list = self.get_file_name()
        for file in file_path_list:
            file_path = file.replace("\\", "/")
            f = open(file_path, encoding="GBK")
            while True:
                word = f.readline()
                if word == "":
                    f.close()
                    break
                word = word.strip()
                print("当前词汇为：", word)
                if self.request_baidu(word):
                    continue
                print("百度搜索符合要求词汇：", word)
                self.save_good_word(word)

    def save_good_word(self, word):
        file_name = rootPath + '/text_result/' + "符合要求的词.text"
        with open(file_name, 'a+', encoding="utf-8") as f:
            f.write(word + '\r\n')

    def save_del_word(self, word):
        file_name = rootPath + '/text_result/' + "不符合要求的词汇.text"
        with open(file_name, 'a+', encoding="utf-8") as f:
            f.write(word + '\r\n')

    def is_forbidde(self, words):
        forbidden_list = NEWS_WAREHOUSE
        for i in forbidden_list:
            if words.__contains__(i):
                return True
        return False

    def run(self):
        self.get_words_list()


if __name__ == '__main__':
    judge_spider = JudgeSpierv2()
    judge_spider.run()