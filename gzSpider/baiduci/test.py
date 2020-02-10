import chardet
import requests
from bs4 import BeautifulSoup

list1 = ['中关村在线', '新浪', '百家号', '新浪', 'IT168', '新浪', '新浪', '中华网', '凤凰娱乐', '网易', '网易', '凤凰娱乐', '中华网', '百家号', '川北在线',
         '百家号', '新浪', '新浪', '百家号', '楚秀网', '四川在线', '网易', '百家号', '百家号', '百家号', '网易', '川北在线', '百家号', '百家号', '百家号', '凤凰娱乐',
         '百家号', '百家号', '西安区县新闻网', '网易', '百家号', '西安区县新闻网', '川北在线', '百家号', '西安区县新闻网', '楚秀网', '百家号', '百家号', '百家号', '百家号',
         '百家号', '百家号', '竞报体育', '百家号', '腾讯大楚网', '百家号', '中华网', '楚秀网', '百家号', '网易', '中原网视', '百家号', '百家号', '网易', '西安区县新闻网',
         '游迅网', '楚秀网', '百家号', '百家号', '百家号', '西安区县新闻网', '新浪', '百家号', '楚秀网', '百家号', '驱动之家', '万家热线', '百家号', '百家号', '百家号',
         '百家号', '游民星空', '百家号', '百家号', '百家号', '川北在线', '楚秀网', '百家号', '万家热线', '百家号', '百家号', '百家号', '百家号', '中华网', '百家号',
         '网易', '百家号', '百家号', '新浪新闻', '百家号', '环球网', '上海热线', '川北在线', '搜狐新闻', '凤凰娱乐', '百家号', '网易', '百家号', '新浪', '上海热线',
         '百家号', '百家号', '楚秀网', '川北在线', '福州新闻网', '百家号', '中金在线', '上海热线', '百家号', '百家号', '中华网', '新浪', '搜狐新闻', '上观', '前瞻网',
         '楚秀网', '百家号', '中华网', '平安健康网', '搜狐新闻', '百家号', '楚秀网', '上海热线', '腾讯网', '西安区县新闻网', '新浪', '西安区县新闻网', '川北在线', '百家号',
         '百家号', '和讯网', '百家号', '百家号', '中华网', '百家号', '网易', '百家号', '楚秀网', '百家号', '百家号', '百家号', '百家号', 'Techweb', '懂球帝',
         '西安区县新闻网', '百家号', '川北在线', '网易', '百家号', '搜狐新闻', '中华网', '上海热线', '新浪', '网易', '百家号', '百家号', '百家号', '百家号', '新浪新闻',
         '网易', '万家热线', '百家号', 'Techweb', '百家号', '百家号', '新浪', '百家号', '百家号', '百家号', '腾讯娱乐', '百家号', 'Techweb', '凤凰娱乐',
         '百家号', '万家热线', '腾讯网', '百家号', '百家号', '上海热线', '百家号', '凤凰娱乐', '中国日报网', '平安健康网', '百家号', '上海热线', '中华网', '百家号',
         '百家号', '百家号', '和讯网', '百家号', '百家号', '新浪', '新浪', 'Techweb', '中华网', '中原网视', '百家号', '百家号', '百家号', '站长之家', '百家号',
         '百家号', '百家号', '中华网', '百家号', '万家热线', '中华网', '西安区县新闻网', '百家号', '新浪新闻', '百家号', '百家号', '中华网', '上海热线', '百家号', '百家号',
         '中国广播网', '百家号', '百家号', '新浪', '百家号', '百家号', '百家号', '网易', '上海热线', 'Techweb', '中华网', '百家号', '百家号', 'Techweb',
         '百家号', '百家号', '百家号', '上海热线', '百家号', '东方网', '上海热线', '中华网', '中国小康网', '百家号', '百家号', '百家号', '上海热线', '平安健康网', '中华网',
         '搜狐新闻', '百家号', '百家号', '新浪新闻', '网易', '网易', '百家号', '新浪', '前瞻网', '百家号', '西安区县新闻网', '深圳热线', '百家号', '西安区县新闻网',
         '八桂网', '百家号', '百家号', '西安区县新闻网', '百家号', '百家号', 'Techweb', '百家号', '百家号', '百家号', '平安健康网', '上海热线', '西安区县新闻网',
         '百家号', '综投网', '中国广播网', '中华网', '新浪', '百家号', '百家号', '中原网视', '上海热线', '上海热线', '人民网', '南方网', '百家号', '澎湃新闻', '百家号',
         '新浪新闻', '北青网', '百家号', '西安区县新闻网', '中华网', '中华网', '上海热线', '百家号', '网易', '万家热线', '百家号', '中华网', '网易', '百家号', '中华网',
         '凤凰娱乐', '百家号', '凤凰娱乐', '百家号', '川北在线', '百家号', '百家号', '百家号', '百家号', '百家号', '腾讯财经', '百家号', '凤凰娱乐', '百家号', '百家号',
         '百家号', '网易', '百家号', '百家号', '股城网', '百家号', '中华网', '楚秀网', '百家号', '百家号', '百家号', '百家号', '世界风力..', '健康界', '百家号',
         '百家号', '西安区县新闻网', '百家号', '新浪新闻', '新浪', '网易', '半岛网', '川北在线', '百家号', '中华网', '百家号', '搜狐', '楚秀网', '大河报', '中华网',
         '百家号', '网易', '百家号', '中国网', '大众网', '百家号', '百家号', '网易', '光明网', '百家号', '新浪新闻', '百家号', '百家号', '上海热线', '百家号', '百家号',
         '百家号', '凤凰娱乐', '百家号', '百家号', '新浪', '百家号', '新浪', '网易', '网易', '百家号', '百家号', '新浪新闻', '百家号', '中华网', '百家号', '百家号',
         '新浪', '百家号', '百家号', 'Techweb', '百家号', '百家号', '百家号', '百家号', '百家号', '新浪', '百家号', '百家号', '百家号', '百家号', '新浪体育',
         '百家号', '百家号', '网易', '网易', '上观', '澎湃新闻', '新浪', '百家号', '上海热线', '百家号', '百家号', '中国经济网', '百家号', '百家号', '网易', '百家号',
         '百家号', '东南网厦门频道', '百家号', '百家号', '百家号', '百家号', '百家号', '百家号', '百家号', '百家号', '百家号', '百家号', '网易', '中华网', '百家号',
         '百家号', '网易', '中华网', '深圳热线', '怕输网', '中华网', '百家号', '东北网', '百家号', '网易', '百家号', '网易', '百家号', '凤凰娱乐', '中关村在线',
         '百家号', '百家号', '新浪新闻', '百家号', '百家号', '百家号', '百家号', '百家号', '百家号', '川北在线', '搜狐新闻', '百家号', '百家号', '爪游控', '凤凰娱乐',
         '网易', '百家号', '百家号', '新浪', '爪游控', '网易', '凤凰娱乐', '百家号', '百家号', '凤凰娱乐', '新浪', '中华网', '百家号', '上海热线', '百家号', '百家号',
         '上海热线', '百家号', '36kr', '百家号', '百家号', '百家号', '百家号', '百家号', '新浪', '百家号', '中华网', '百家号', '川北在线', '中国广播网', '百家号',
         '北青网', '百家号', '百家号', '闽南网', '百家号', '南方网', '中国广播网', '百家号', '百家号', '百家号', '百家号', '百家号', '百家号', '中华网', '百家号',
         '大众网', '百家号', '百家号', '正北方网', '凤凰娱乐', '百家号', '中华网', '百家号', '网易', '楚秀网', '腾讯大楚网', '百家号', '百家号', '凤凰娱乐', '中国经济导报',
         '百家号', '网易', '央视网', '百家号', '网易', '百家号', '网易', '百家号', '百家号', '网易']

dict_1 = {'新浪': "get_content_sina", '百家号': "get_content_baijiahao",  '中华网': 'get_content_china',
          '凤凰娱乐': "get_content_ifeng", '网易': "get_content_163", '川北在线': "get_content_guangyuanol",
          '西安区县新闻网': 'get_content_wmxa', '新浪新闻': 'get_content_sina_news',  '上海热线': 'get_content_online'}


class TenCentReDian:
    def __init__(self):
        self.headers_content = {
            'Referer': 'http://top.baidu.com/news?fr=topcategory_c513',
            'Cookie': 'BAIDUID=F10B7427F80F49A6333C69BEFA3828D3:FG=1; PSTM=1563506567; BIDUPSID=6A5E4F6889061C700606CA09656EC680; BDORZ=B490B5EBF6F3CD402E515D22BCDA1598; H_PS_PSSID=1464_21103_18560_29522_29521_29098_29568_28834_29220_26350_22157; Hm_lvt_79a0e9c520104773e13ccd072bc956aa=1565934372; bdshare_firstime=1565934372409; vit=1; BDRCVFR[z91LIEeorFR]=mbxnW11j9Dfmh7GuZR8mvqV; delPer=0; PSINO=7; Hm_lpvt_79a0e9c520104773e13ccd072bc956aa=1565935300',
            'User-Agent': 'Mozilla/5.0(Windows NT 10.0; Win64; x64) AppleWebKit/537.36(KHTML,like Gecko) Chrome/76.0.3809.100 Safari/537.36'
        }

        self.list_content = ['新浪', '百家号', '中华网', '凤凰娱乐', '网易', '川北在线', '西安区县新闻网', '新浪新闻', '上海热线']

    def get_content_sina(self, url):
        """ 新浪 """
        resp = requests.get(url, headers=self.headers_content)
        html_encode = resp.content
        items = list()
        try:
            detect_result = chardet.detect(html_encode)
            encoding = detect_result["encoding"]
            html = str(html_encode, encoding)
            soup = BeautifulSoup(html, 'html.parser')
            soup1 = soup.find_all('div', class_="article")
            if soup1:
                soup2 = soup1[0].select('p')
                for i in soup2:
                    content = i.text.strip()
                    if content:
                        items.append(content)
        except Exception as e:
            print('失败原因：{}'.format(e))
            return items
        else:
            return items

    def get_content_baijiahao(self, url):
        """ 百家号 """
        resp = requests.get(url, headers=self.headers_content)
        html_encode = resp.content
        items = list()
        try:
            detect_result = chardet.detect(html_encode)
            encoding = detect_result["encoding"]
            html = str(html_encode, encoding)
            soup = BeautifulSoup(html, 'html.parser')
            soup1 = soup.find_all('div', class_="article-content")
            if soup1:
                soup2 = soup1[0].select('p span')
                for i in soup2:
                    content = i.text.strip()
                    if content:
                        items.append(content)
        except Exception as e:
            print('失败原因：{}'.format(e))
            return items
        else:
            return items

    def get_content_china(self, url):
        """ 中华网 """
        resp = requests.get(url, headers=self.headers_content)
        html_encode = resp.content
        items = list()
        try:
            detect_result = chardet.detect(html_encode)
            encoding = detect_result["encoding"]
            html = str(html_encode, encoding)
            soup = BeautifulSoup(html, 'html.parser')
            soup1 = soup.find_all(id="chan_newsDetail")
            if soup1:
                soup2 = soup1[0].select('p')
                for i in soup2:
                    content = i.text.strip()
                    if content:
                        items.append(content)
        except Exception as e:
            print('失败原因：{}'.format(e))
            return items
        else:
            return items

    def get_content_ifeng(self, url):
        """ 凤凰娱乐 """
        resp = requests.get(url, headers=self.headers_content)
        html_encode = resp.content
        items = list()
        try:
            detect_result = chardet.detect(html_encode)
            encoding = detect_result["encoding"]
            html = str(html_encode, encoding)
            soup = BeautifulSoup(html, 'html.parser')
            soup1 = soup.find_all('div', class_="js_selection_area")
            if soup1:
                soup2 = soup1[0].select('p')
                for i in soup2:
                    content = i.text.strip()
                    if content:
                        items.append(content)
        except Exception as e:
            print('失败原因：{}'.format(e))
            return items
        else:
            return items

    def get_content_163(self, url):
        """ 网易 """
        resp = requests.get(url, headers=self.headers_content)
        html_encode = resp.content
        items = list()
        try:
            detect_result = chardet.detect(html_encode)
            encoding = detect_result["encoding"]
            html = str(html_encode, encoding)
            soup = BeautifulSoup(html, 'html.parser')
            soup1 = soup.find_all('div', class_="post_text")
            if soup1:
                soup2 = soup1[0].select('p span')
                for i in soup2:
                    content = i.text.strip()
                    if content:
                        items.append(content)
        except Exception as e:
            print('失败原因：{}'.format(e))
            return items
        else:
            return items

    def get_content_guangyuanol(self, url):
        """ 川北在线"""
        resp = requests.get(url, headers=self.headers_content)
        html_encode = resp.content
        items = list()
        try:
            detect_result = chardet.detect(html_encode)
            encoding = detect_result["encoding"]
            html = str(html_encode, encoding)
            soup = BeautifulSoup(html, 'html.parser')
            soup1 = soup.find_all('div', class_="wb_nr")
            if soup1:
                soup2 = soup1[0].select('div')
                for i in soup2:
                    content = i.text.strip()
                    if content:
                        items.append(content)
        except Exception as e:
            print('失败原因：{}'.format(e))
            return items
        else:
            return items

    def get_content_wmxa(self, url):
        """西安区县新闻网 """
        resp = requests.get(url, headers=self.headers_content)
        html_encode = resp.content
        items = list()
        try:
            detect_result = chardet.detect(html_encode)
            encoding = detect_result["encoding"]
            html = str(html_encode, encoding)
            soup = BeautifulSoup(html, 'html.parser')
            soup1 = soup.find_all('div', class_="post_text")
            if soup1:
                soup2 = soup1[0].select('p span')
                for i in soup2:
                    content = i.text.strip()
                    if content:
                        items.append(content)
        except Exception as e:
            print('失败原因：{}'.format(e))
            return items
        else:
            return items

    def get_content_sina_news(self, url):
        """ 新浪新闻 """
        resp = requests.get(url, headers=self.headers_content)
        html_encode = resp.content
        items = list()
        try:
            detect_result = chardet.detect(html_encode)
            encoding = detect_result["encoding"]
            html = str(html_encode, encoding)
            soup = BeautifulSoup(html, 'html.parser')
            soup1 = soup.find_all('div', class_="article")
            if soup1:
                soup2 = soup1[0].select('p')
                for i in soup2:
                    content = i.text.strip()
                    if content:
                        items.append(content)
        except Exception as e:
            print('失败原因：{}'.format(e))
            return items
        else:
            return items

    def get_content_online(self, url):
        """ 上海热线 """
        resp = requests.get(url, headers=self.headers_content)
        html_encode = resp.content
        items = list()
        try:
            detect_result = chardet.detect(html_encode)
            encoding = detect_result["encoding"]
            html = str(html_encode, encoding)
            soup = BeautifulSoup(html, 'html.parser')
            soup1 = soup.find_all('div', class_="post_text")
            if soup1:
                soup2 = soup1[0].select('p')
                for i in soup2:
                    content = i.text.strip()
                    if content:
                        items.append(content)
        except Exception as e:
            print('失败原因：{}'.format(e))
            return items
        else:
            return items

    def run(self):
        # 新浪
        url_1 = 'https://finance.sina.com.cn/stock/relnews/us/2019-08-15/doc-ihytcitm9452883.shtml'
        # 百家号
        url_2 = ' https://baijiahao.baidu.com/s?id=1641931239821125431&wfr=spider&for=pc'
        # 中华网
        url_3 = 'https://news.china.com/socialgd/10000169/20190815/36836913.html'
        # 凤凰娱乐
        url_4 = 'http://ent.ifeng.com/a/20190815/43469597_0.shtml'
        # 网易
        url_5 = 'http://news.163.com/19/0815/19/EML5MEA200019K82.html'
        # 川北在线
        url_6 = 'http://www.guangyuanol.cn/news/shehui/2019/0815/985334.html'
        # '西安区县新闻网'
        url_7 = 'http://news.wmxa.cn/society/201908/644761.html'
        # 新浪新闻
        url_8 = 'https://news.sina.com.cn/o/2019-08-15/doc-ihytcern1012502.shtml'
        # 上海热线
        url_9 = 'https://news.online.sh.cn/news/gb/content/2019-08/15/content_9365747.htm'

        # result = self.get_content_sina(url_1)
        # print(result)

        # result = self.get_content_baijiahao(url_2)
        # print(result)

        result = self.get_content_china(url_3)
        print(result)

        # result = self.get_content_ifeng(url_4)
        # print(result)

        # result = self.get_content_163(url_5)
        # print(result)

        # result = self.get_content_guangyuanol(url_6)
        # print(result)

        # result = self.get_content_sina_news(url_8)
        # print(result)

        # result = self.get_content_online(url_9)
        # print(result)


if __name__ == '__main__':
    tencen = TenCentReDian()
    tencen.run()

