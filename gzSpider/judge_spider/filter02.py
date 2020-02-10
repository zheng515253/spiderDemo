#!encoding=utf-8
#desc:百度禁词判断
import requests
import time
import random,sys,io,re
import aiohttp,asyncio
from fake_useragent import UserAgent
from lxml import etree
#设置默认编码
#sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='gb18030')

#配置url

base_domain = 'http://www.baidu.com/'

with open('keys2.txt', encoding='UTF-8') as f:
    keys = f.read()
keys=keys.splitlines()

#获取ban的词汇，并存入列表
with open('ban.txt', encoding='UTF-8') as f:
    banKeys = f.read()
banKeys=banKeys.splitlines()
banKeys=["("+key+")" for key in banKeys]
#拼接banKeys成一个正则字符串，如[新华网,人民网]
pattern_banKeys=('|').join(banKeys)
print(pattern_banKeys)

urls=[]
[urls.append((base_domain + 's?wd=' + key,key )) for key in keys]
file = open('pcUserAgent.txt')
userAgents = file.readlines()
file.close()



file = open('ban.txt', encoding='UTF-8')
bans = file.readlines()
file.close()
#使用随机头信息
ua=UserAgent()
header = {
    'User-Agent': ua.random
}
#代理设置
proxies = {}
proxy_set = set()


def change_proxy():
    global proxy_set, proxies, userAgents
    if not proxy_set:
        try:
            res = requests.get(
                'http://piping.mogumiao.com/proxy/api/get_ip_bs?appKey=4fd65c39757e4ad9bfe24cd4da67eb69&count=10&expiryDate=0&format=1&newLine=2',
                timeout=5)
            resJson = res.json()
        except:
            print('请求错误')
            time.sleep(1)
            return change_proxy()

        if res.status_code != 200:
            print('获取http代理错误  10秒后重新获取')
            time.sleep(10)
            return change_proxy()

        if resJson['code'] != '0':
            print('获取http代理错误   10秒后重新获取 ' + resJson['msg'])
            time.sleep(10)
            return change_proxy()

        for http in resJson['msg']:
            proxy_set.add('http://' + http['ip'] + ':' + http['port'])

    header['User-Agent'] = userAgents[random.randint(0, len(userAgents) - 1)].strip()
    proxies['http'] = proxy_set.pop()
    print('更换 ua 代理' + proxies['http'])
    return True



async def main(pool):
    sem=asyncio.Semaphore(pool)
    async with aiohttp.ClientSession() as session:
        tasks=[]
        [tasks.append(controlSem(sem,url,session)) for url in urls]
        await asyncio.wait(tasks)

#限制并发
async def controlSem(sem,url,session):
    async with sem:
        await fetch(url,session)
#url[0]=采集url
#url[1]=采集关键词
async def fetch(url,session):

    headers={"Host":"www.baidu.com","User-Agent":ua.random}
    async with session.get(url[0],headers=headers) as res:
        contents=await res.text()
        #根据type判断来源并进行相应处理
        checkKeywordsByBaidu(contents,url[1])

error = 0

def checkKeywordsByBaidu(resHtml,key):
    print('-' * 30)
    print(key)
    if 'antispider' in resHtml:
        keys.append(key)
        print('百度 已屏蔽')
        #change_proxy()

    # ban_flag = False
    # for ban in bans:
    #     ban = ban.strip()
    #     if ban in resHtml:
    #         ban_flag = True
    #         print('失败 ' + ban)
    #         f = open('baiduError.txt', 'a+', encoding='utf-8')
    #         f.write(key + ' ---- ' + ban + '\n')
    #         f.close()
    #
    #         break
    # if ban_flag == False:
    #     print('成功')
    #     with open('baiduSuccess.txt', 'a+', encoding='utf-8') as f:
    #         f.write(key + '\n')
    #ban词检测
    checkBan(key,resHtml)

def checkBan(key,resHtml):
    pattern=re.compile(pattern_banKeys)
    checkedCount=len(re.findall(pattern,resHtml))
    if checkedCount >=5:
        print(key+'失败')
        f = open('baiduError.txt', 'a+', encoding='utf-8')
        f.write(key + '\n')
        f.close()
    else:
        #判断搜索到结果数量是否大于500
        #print(resHtml)
        searchCount=getSearchCount(resHtml)
        print(key,searchCount)
        print('成功')
        with open('baiduSuccess.txt', 'a+', encoding='utf-8') as f:
            f.write(key + '\n')
        # if searchCount<=500:
        #     print(key + '搜索结果太少，失败')
        #     f = open('baiduError.txt', 'a+', encoding='utf-8')
        #     f.write(key + '\n')
        #     f.close()
        # else:
        #     print('成功')
        #     with open('baiduSuccess.txt', 'a+', encoding='utf-8') as f:
        #         f.write(key + '\n')

#获取关键词搜索结果数量
def getSearchCount(resHtml):
    pattern = re.compile(r'百度为您找到相关结果约([0-9,]*)个')
    result = re.findall(pattern, resHtml)
    result = re.sub(',', '', result[0])
    return int(result)



async def main(pool):
    sem=asyncio.Semaphore(pool)
    async with aiohttp.ClientSession() as session:
        tasks=[]
        [tasks.append(controlSem(sem,url,session)) for url in urls]
        await asyncio.wait(tasks)

if __name__=='__main__':
    loop=asyncio.get_event_loop()
    loop.run_until_complete(main(50))
    print('全部执行完成')
