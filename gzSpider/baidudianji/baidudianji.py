"""
百度点击
"""
import random
import re
import time

from bs4 import BeautifulSoup

from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains  # 引入ActionChains鼠标操作类

SLEEP_TIME = [2.1, 2.2, 2.3, 2.5, 2.7, 2.8, 2.9, 3, 3.3, 3.5, 3.6, 3.77, 3.9, 4.1, 4.2, 4.5, 4.6, 4.7, 4.8, 4.9]


class Baidudianji:
    """ 百度点击"""
    spider_name = "百度点击模拟"

    def __init__(self):
        pass

    def get_page_number(self):
        """ 获取页码"""
        page_number_list = list()
        num = random.randint(2, 5)
        for i in range(0, num):
            num = random.randint(0, 10)
            page_number_list.append(num)
        page_number_list = list(set(page_number_list))
        page_number_list.sort()
        return page_number_list

    def get_digit(self):
        """ 获取点击标签"""
        digit_list = list()
        num = random.randint(2, 4)
        for i in range(0, num):
            num = random.randint(1, 11)
            digit_list.append(num)
        digit_list = list(set(digit_list))
        digit_list.sort()
        return digit_list

    def request_detail(self, driver, soup):
        items = soup.find_all('h3', {"class": "t"})
        url_list = list()
        for i in items:
            url_list.append(i.a['href'])
        for i in self.get_digit():
            url = random.choice(url_list)
            url_list.remove(url)
            driver.get(url)
            time.sleep(random.choice(SLEEP_TIME))

    def request_page(self, driver, page_url_list, keywords):
        url = 'https://www.baidu.com/s?wd={}&pn={}0&oq=%E5%A8%B1%E4%B9%90&tn=baiduhome_pg&ie=utf-8&usm=2&rsv_idx=2&rsv_pq=8d44a77400013616&rsv_t=ff6b8iHfFNQHc25P7SB03hMRiZV9u4Rm%2BfwhX3I7s%2BaYWcVrv5GxykdB%2Fn1Mvt1sGhWc'
        for i in self.get_page_number():
            # driver.back()
            # local = driver.find_element_by_xpath('//div[@id="page"]/a[{}]'.format(i))
            # ActionChains(driver).move_to_element(local).perform()
            # ActionChains(driver).context_click(local).perform()
            # time.sleep(random.choice(SLEEP_TIME))
            # driver.find_element_by_xpath('//div[@id="page"]/a[{}]'.format(i)).click()
            url = url.format(keywords, i)
            driver.get(url)
            time.sleep(random.choice(SLEEP_TIME))
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            if soup:
                self.request_detail(driver, soup)

    def request(self, keywords):
        driver = webdriver.Chrome()
        driver.get("http://www.baidu.com")
        driver.maximize_window()
        time.sleep(random.choice(SLEEP_TIME))
        driver.find_element_by_id('kw').send_keys(u'{}'.format(keywords))
        time.sleep(random.choice(SLEEP_TIME))
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        if soup:
            self.request_detail(driver, soup)
            soup_page = soup.find(id="page")
            page_list = soup_page.find_all("a")
            page_url_list = list()
            for i in page_list:
                page_url_list.append(i['href'])
            self.request_page(driver, page_url_list, keywords)
        driver.set_window_size(800, 480)
        time.sleep(random.choice(SLEEP_TIME))
        driver.quit()

    def run(self):  # , '娱乐', '棋牌', '登陆', '注册', '国际', '在线', '平台'
        keywords_list = ['体育']
        for keywords in keywords_list:
            time.sleep(2)
            self.request(keywords)


if __name__ == '__main__':
    baidu_obj = Baidudianji()
    baidu_obj.run()
