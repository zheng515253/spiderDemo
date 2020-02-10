import requests
import time
import random,sys,io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='gb18030')
base_domain = 'http://www.sogou.com/'

file = open('pcUserAgent.txt')
userAgents = file.readlines()
file.close()

file = open('keys.txt', encoding='UTF-8')
keys = file.readlines()
file.close()

file = open('ban.txt', encoding='UTF-8')
bans = file.readlines()
file.close()

header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'
}

proxies = {}
proxy_set = set()


def change_proxy():
    global proxy_set, proxies, userAgents

    if not proxy_set:
        try:
            res = requests.get(
            'http://piping.mogumiao.com/proxy/api/get_ip_bs?appKey=4fd65c39757e4ad9bfe24cd4da67eb69&count=10&expiryDate=0&format=1&newLine=2', timeout=5)
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


error = 0

for key in keys:
    key = key.strip()

    if not key:
        continue

    print('-' * 30)
    print(key)

    if error > 3:
        error = 0
        change_proxy()

    try:
        res = requests.get(base_domain + 'web?query=' + key, headers=header, proxies=proxies, timeout=5)
    except:
        error += 1
        keys.append(key)
        print('请求失败')
        continue

    if res.status_code != 200:
        error += 1
        keys.append(key)
        print('请求错误: ' + res.status_code)
        continue

    error = 0

    resHtml = res.content.decode()
    if 'antispider' in resHtml:
        keys.append(key)
        print('sogou 已屏蔽')
        change_proxy()
        continue

    ban_flag = False
    for ban in bans:
        ban = ban.strip()
        if ban in resHtml:
            ban_flag = True
            print('失败 ' + ban)
            f = open('error.txt', 'a+', encoding='utf-8')
            f.write(key + ' ---- ' + ban + '\n')
            f.close()
            break

    if ban_flag:
        continue

    print('成功')
    f = open('success.txt', 'a+', encoding='utf-8')
    f.write(key + '\n')
    f.close()
    # print(resHtml)

print('全部执行完成')
