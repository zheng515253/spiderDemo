import requests


class TenCent:
    def __init__(self):
        self.headers = {
            'Referer': 'https://news.163.com/',
            'Sec-Fetch-Mode': 'no-cors',
            'User-Agent': 'Mozilla/5.0(Windows NT 10.0; Win64; x64) AppleWebKit/537.36(KHTML,like Gecko) Chrome/76.0.3809.100 Safari/537.36'
        }

        self.url = 'https://temp.163.com/special/00804KVA/cm_sports_03.js?callback=data_callback'

    def request(self):
        resp = requests.get(self.url, headers=self.headers)
        print(resp.content.decode('gbk'))

    def run(self):
        self.request()


if __name__ == '__main__':
    tencen = TenCent()
    tencen.run()