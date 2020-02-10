from multiprocessing import Pool
from gevent.pool import Pool
import gevent
from gevent import monkey
monkey.patch_all()

import requests


def test1():
    for i in range(10000):
        rep = requests.get("http://www.baidu.com/")
        print("test1", rep.status_code)


def test2():
    for i in range(10000):
        rep = requests.get("http://www.baidu.com/")
        print("test2", rep.status_code)


def coroutine():
    gevent.joinall([
        gevent.spawn(test1),
        gevent.spawn(test2)
    ])


if __name__ == "__main__":
    p = Pool()
    for i in range(4):
        p.apply_async(coroutine, args=())
    p.join()