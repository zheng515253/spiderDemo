"""
英雄联盟
"""
import time
import chardet
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


class Hero:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64)AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 UBrowser/6.2.3964.2 Safari/537.36",
            "Origin": "https://cn.pornhub.com",
        }

    def ChromeDriverNOBrowser(self):
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        driverChrome = webdriver.Chrome(chrome_options=chrome_options)
        return driverChrome

        # 有界面的就简单了
    def ChromeDriverBrowser(self):
        driverChrome = webdriver.Chrome()
        return driverChrome

    def request_browser(self, url):
        brower = self.ChromeDriverNOBrowser()
        brower.get(url)
        time.sleep(0.5)
        soup = BeautifulSoup(brower.page_source, 'html.parser')
        print(soup)

    def request(self, url):
        resp = requests.get(url=url, headers=self.headers, timeout=5)
        html_encode = resp.content
        items = []
        try:
            detect_result = chardet.detect(html_encode)
            encoding = detect_result["encoding"]
            html = str(html_encode, encoding)
            soup = BeautifulSoup(html, 'html.parser')
            print(soup)
            soup1 = soup.find_all('div', class_='bfTable')

        except Exception as e:
            print('失败原因：{}'.format(e))
            return items
        else:
            return items

    def parse_game(self, html):
        game_list = html.find_all("span")
        game1_list = game_list[0].select("i")
        item1_name = game1_list[1].text.strip()
        item1_score = game1_list[2].text.strip()
        item2_score = game1_list[3].text.strip()
        print(item2_score)

    def parse(self, html):
        span_list = html.find_all("span")
        time = span_list[1].text.strip()
        itme1 = span_list[2].text.strip()
        state = span_list[3].text.strip()
        print(state)
        game = self.parse_game(span_list[4])

    def run(self):
        url = 'https://live.leisu.com/'
        url_1 = 'https://www.esportlivescore.cn/d_2019-08-09_g_leagueoflegends.html'
        list_li = self.request(url_1)


if __name__ == '__main__':
    hero = Hero()
    hero.run()