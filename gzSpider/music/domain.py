import os
import random
import re
import time

import chardet
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
# 导入协程gevent模块
# 执行猴子补丁，让网络库可以异步执行网络IO任务
# 创建协程池
from gevent.pool import Pool
from gevent.monkey import patch_all
patch_all()

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


class Demo:

    def __init__(self):
        self.root_path = os.path.dirname(os.path.realpath(__file__))

        self.headers = {
            "User-Agent": random.choice(USER_AGENT_LIST),
            'Host': 'dns.aizhan.com',
            'Referer': 'https://dns.aizhan.com/',
        }
        self.url = 'https://dns.aizhan.com/{}/{}/'
        self.total = 0

    def file_name(self, user_dir):
        file_list = list()
        for root, dirs, files in os.walk(user_dir):
            for file in files:
                # if os.path.splitext(file)[1] == '.txt':
                file_list.append(os.path.join(root, file))
        return file_list

    def change_name(self):
        path = self.root_path + '/' + 'dome'
        new_path = self.root_path + '/' + 'new_pic'
        file_list = self.file_name(path)

        for file in file_list:
            file_path = file.replace("\\", "/")
            with open(file_path, 'rb') as f:
                pic_content = f.read()

    def ChromeDriverNOBrowser(self):
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        driverChrome = webdriver.Chrome(chrome_options=chrome_options)
        return driverChrome

    def ChromeDriverBrowser(self):
        driverChrome = webdriver.Chrome()
        return driverChrome

    def request_total(self, url):
        url = self.url.format(url, 1)
        try:
            brower = self.ChromeDriverNOBrowser()
            brower.get(url)
            time.sleep(4)
            soup = BeautifulSoup(brower.page_source, 'html.parser')
            soup2 = soup.find_all('span', class_="red")
            self.total = int(soup2[0].text.strip())
        except Exception as e:
            print('失败原因：{}'.format(e))
            return
        else:
            return

    def request_demo(self, url, i):
        url = self.url.format(url, i)
        items = []
        try:
            brower = self.ChromeDriverNOBrowser()
            brower.get(url)
            time.sleep(SLEEP_TIEM)
            soup = BeautifulSoup(brower.page_source, 'html.parser')
            soup1 = soup.find_all('div', class_='dns-content')
            soup2 = soup.find_all('span', class_="red")
            self.total = int(soup2[0].text.strip())
            if soup1:
                items = soup1[0].select('tr')
        except Exception as e:
            print('失败原因：{}'.format(e))
            return items
        else:
            return items

    def get_data(self, list_html):
        items = list()
        try:
            for i in range(1, len(list_html)):
                try:
                    td_html = list_html[i]
                    regex = re.compile('target="_blank">([\w\W]+?)</a>')
                    domain = regex.findall(str(td_html))[0]
                    regex_br = re.compile('align="absmiddle" alt="([\w\W]+?)"')
                    br_pr_list = regex_br.findall(str(td_html))
                    br = br_pr_list[0]
                    pr = br_pr_list[1]
                    items.append((domain, br, pr))
                except Exception as e:
                    print("解析失败原因：{}".format(e))
        except Exception as e:
            print("解析失败原因：{}".format(e))
            return items
        else:
            return items

    def save_title(self, data, url):
        file_name = rootPath + "/domain_data/" + str(url) + ".text"
        with open(file_name, 'a+', encoding="utf-8") as f:
            f.write(data + '\r\n')

    def parse(self, url, i):
        tr_list = self.request_demo(url, i)
        if tr_list:
            items = self.get_data(tr_list)
            if items:
                for i in items:
                    domain, br, pr = i
                    content = domain + "  " + br + "  " + pr
                    print(content)
                    # self.save_title(content, url)

    def get_total(self):
        if self.total//10 == self.total/10:
            return self.total//10
        else:
            result = self.total // 10 + 1
            return result

    def run(self,):
        url_list = ['www.baidu.com', 'www.110088.com']
        for url in url_list:
            self.request_total(url)
            total = self.get_total()
            print("ip总数问为：{}, 总页数为：{}".format(self.total, total))
            pool_g = Pool(GEVENT_COUNT)
            for i in range(1, total+1):
                pool_g.apply_async(self.parse, [url, i])
            pool_g.join()


if __name__ == '__main__':
    change_pic_name = Demo()
    change_pic_name.run()