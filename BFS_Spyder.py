# -*- coding: utf-8 -*-


import datetime
import time
import re
import requests
import gevent 
from gevent.queue import Queue, Empty
from pymongo import MongoClient
from gevent import monkey
monkey.patch_all()


def get_url(queue):
    '''
    获取单个链接中的所有链接并过滤在g_list列表中已经存在的链接
    '''
    global g_all_list
    global g_list
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 Edge/16.16299'}
    while not queue.empty():
        link = queue.get_nowait()
        try:
            req = requests.get('https://en.wikipedia.org/wiki/'+str(link), headers=headers).text
            link_list = re.findall('<a.*?href=\"/wiki/([^:#=<>]*?)\".*?</a>', req)
            #通过比较得到本页面与已过滤列表中不同的链接
            diff_list = list(set(link_list)-set(g_list))
            g_all_list.extend(diff_list)
            g_list.extend(diff_list)
        except Exception as e:
            print('url:' + link + '获取失败', e)
            
def list_to_queue(unique_list):
    '''
    把列表转换成队列
    '''
    global queue
    for each in unique_list:
        queue.put_nowait(each)

def list_to_dict():
    '''
    把列表转换成字典，再存储到列表中
    '''
    global g_list
    global db_list
    _list = list(set(g_list))
    for each in _list:
        datas = {'link': 'https://en.wikipedia.org/wiki/'+each}
        db_list.append(datas)
        
if __name__ == '__main__':
    #连接MongoDB数据库
    client = MongoClient('localhost', 27017)
    #创建数据库
    db = client.wiki
    #创建数据表
    collection = db.url
    primary_url = 'Wikipedia'
    #全局变量列表存放所有过滤过的链接
    g_list = [primary_url]
    #全局变量列表存放每一层所有的链接
    g_all_list = []
    #全局变量列表存放即将写入数据库中的链接
    db_list = []
    start_time = time.time()
    date = datetime.datetime.now()
    print('开始时间：' + str(date) + '\n')
    #自定义队列长度，尽量定义大点
    queue = Queue(1000000000)
    #将维基百科主页面地址放入到队列中
    queue.put(primary_url)
    #自定义BFS的层数
    depth = 1
    #BFS的层数最大值为3
    while depth<3:
        g_all_list = []
        print('depth:' + str(depth) + '\t开始抓取' + '\n')
        #使用协程加快网络IO
        ls = []
        for i in range(10):
            ls.append(gevent.spawn(get_url, queue))
        gevent.joinall(ls)
        queue.queue.clear()
        #使用协程加快列表到队列的转化
        gevent.spawn(list_to_queue, g_all_list).join()
        date = datetime.datetime.now()
        length = len(g_all_list)
        print('time:' + str(date) + '\n')
        print(str(length) + '条列表项成功转化为队列\n')
        depth += 1
    end_time = time.time()
    print('-'*30)
    print('全部链接抓取完成，花费时间：' + str(end_time - start_time) + 's\n')
    date =  datetime.datetime.now()
    print('结束时间：' + str(date) + '\n')
    print('-'*30)
    print('-'*30)
    print('-'*30)
    #使用协程加快列表到字典的转化
    gevent.spawn(list_to_dict).join()
    print('写入数据库的列表长度：', len(db_list))
    print('开始写入数据库wiki，数据表url中')
    db_start_time = time.time()
    #将列表中的数据写入到MongoDB中
    collection.insert_many(db_list)
    db_end_time = time.time()
    print('全部数据写入数据库花费时间：', db_end_time - db_start_time)
    
