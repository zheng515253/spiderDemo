import os
import random
import re
import time

import redis
import requests

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


class BaseObjectSpider:
    spider_name = 'base_spider'

    def __init__(self):
        self.headers = random.choice(USER_AGENT_LIST)
        self.rdp = redis.ConnectionPool(host='127.0.0.1', port=6379, decode_responses=True)
        self.redis_client = redis.StrictRedis(connection_pool=self.rdp)
        self.key = ''
        self.queue = ''

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

    def save_content(self, data, name):
        """ 保存数据 """
        date = time.strftime('%Y-%m-%d')
        file_path = rootPath + '/spider_data'
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        file_name = file_path + '/' + self.spider_name + '_' + name + '_' + date + ".text"
        with open(file_name, 'a+', encoding='utf-8') as f:
            f.write(data + "\r\n")

    def copy_url(self):
        """ 将数据从copy到队列里面 """
        url_list = self.redis_client.hkeys(self.key)
        for i in url_list:
            self.redis_client.lpush(self.queue, i)

    def file_name(self, user_dir):
        """ 获取当前目录下的所有文件名称 """
        file_list = list()
        for root, dirs, files in os.walk(user_dir):
            for file in files:
                file_list.append(os.path.join(root, file))
        return file_list

    def get_content(self, user_dir):
        """ 获取当前目录下的所有文件内容 """
        file_list = self.file_name(user_dir)
        for file in file_list:
            file_path = file.replace("\\", "/")
            print("文章名：", file_path)
            f = open(file_path, encoding="utf-8")
            while True:
                content = f.readline()
                if content == "":
                    break
                content = content.strip()

    def save_data(self, data):
        """ 档文件超过一定的量是重新建立一个文件 """
        j = 0
        next_num = ''
        file_name = rootPath + "/pass.txt"
        dic = open(file_name, "a")  # 50
        for i in data:
            now_num = int(i[1])
            if now_num != next_num:
                dic.close()
                j += 1
                file_name = rootPath + "pass_v" + str(j) + ".txt"
                dic = open(file_name, "a")
            content = "".join(i) + "\r\n"
            dic.write(content)
            next_num = now_num

    def start_request(self, url):
        item = list()
        try:
            resp = requests.get(url=url, headers=self.headers)
            time_list = [5, 6, 7, 8]
            time.sleep(random.choice(time_list))
            html_str = resp.content.decode()
            regex = re.compile("""meta name="description" content="([\d\D]+?)" />""")
            item = regex.findall(html_str)
        except Exception as e:
            print("失败原因为：{}".format(e))
            return item
        else:
            return item

    def run(self):
        while True:
            url = self.redis_client.lpop(self.queue)
            if url == "":
                break
            url = url.strip()


if __name__ == '__main__':
    spider = BaseObjectSpider()
    spider.run()