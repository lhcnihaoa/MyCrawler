from urllib import parse
from urllib import error
from bs4 import BeautifulSoup
import requests
import threading
import string
import random
import csv
import os
import time
import Settings


class Distinct():
    def __init__(self, name, href):
        self.name = name
        self.href = href


class Region():
    def __init__(self, name, href, distinct, unit_price=0, tot_area=0):
        self.name = name
        self.href = href
        self.distinct = distinct
        self.unit_price = unit_price
        self.tot_area = tot_area



class House_info():
    def __init__(self, res_qua, house_type, area, unit_price, tot_price, addr, link):
        self.res_qua = res_qua
        self.house_type = house_type
        self.area = area
        self.unit_price = unit_price
        self.tot_price = tot_price
        self.addr = addr
        self.link = link

#定义爬虫线程
class Crawler(threading.Thread):
    def __init__(self, lock):
        threading.Thread.__init__(self)
        self.lock = lock

    def run(self):
        global region_queue
        global house_data
        while len(region_queue):
            region = region_queue.pop(0)
            url = parse.quote(
                'http://sh.lianjia.com' +
                region.href,
                safe=string.printable)
            house_info = get_houseinfo(url)
            region.tot_area = sum(float(house.area) for house in house_info)
            if region.tot_area:
                region.unit_price = sum(float(house.tot_price) for house in house_info) / region.tot_area
            self.lock.acquire()
            save_detaildata(house_info)
            save_regiondata(region)
            self.lock.release()

# 获得区列表
def get_distinctlist(html):
    distinct_html = html.find_all(
        "div", {"class": "option-list gio_district"})[0].find_all("a")
    distinct_list = []
    for distinct in distinct_html:
        if distinct.get_text() != "不限":
            temp = Distinct(distinct.get_text(), distinct.attrs["href"])
            distinct_list.append(temp)
    return distinct_list

# 获得小分区列表


def get_regionlist(distinct_list):
    region_list = []
    for distinct in distinct_list:
        url = parse.quote(
            'http://sh.lianjia.com' +
            distinct.href,
            safe=string.printable)
        html = gethtml(url)
        region_html = html.find_all(
            "div", {"class": "option-list sub-option-list gio_plate"})[0].find_all("a")
        for region in region_html:
            if region.get_text() != "不限":
                temp = Region(region.get_text(), region.attrs["href"], distinct.name)
                region_list.append(temp)
    return region_list

# 获取房屋户型、售价等
def get_houseinfo(url):
    house_info = []
    html = gethtml(url)
    house_html = html.find_all("div", {"class": "info-panel"})
    for house in house_html:
        #爬取链接、小区、房型、面积、总价、单价、地址等信息
        house_link = "http://sh.lianjia.com" + house.find_all("a", {"name": "selectDetail"})[0].attrs["href"]
        house_detailpage = gethtml(house_link)
        try:
            res_qua = house_detailpage.find_all("a", {"class": "propertyEllipsis ml_5"})[0].get_text(strip=True)
            house_type = house_detailpage.find_all("div", {"class": "room"})[0].get_text(strip=True)
            area = house_detailpage.find_all("div", {"class": "area"})[0].get_text(strip=True).strip("平")
            tot_price = house_detailpage.find_all("div", {"class": "price"})[0].get_text(strip=True).strip("万")
            unit_price = float(tot_price)/float(area)
            addr = house_detailpage.find_all("p", {"class": "addrEllipsis fl ml_5"})[0].attrs["title"]
        except:
            print(house_detailpage)
        # print(res_qua,house_type,area,unit_price,tot_price,addr,house_link)
        house_info.append(
            House_info(
                res_qua,
                house_type,
                area,
                unit_price,
                tot_price,
                addr,
                house_link))
    if html.find_all("a", {"gahref": "results_next_page"}):
        next_pagr = html.find_all("a", {"gahref": "results_next_page"})[0].attrs["href"]
        url = parse.quote('http://sh.lianjia.com' + next_pagr, safe=string.printable)
        house_info += get_houseinfo(url)
    return house_info

# 下载给定地址页面
def gethtml(url):
    global ip_list
    time.sleep(random.uniform(1.5, 3.0))
    ip_item = random.choice(ip_list)
    headers = randHeader()
    proxies = {ip_item["type"]: ip_item["type"]+"://"+ip_item["ip"]+":"+ip_item["port"], }
    try:
        html = requests.get(url, proxies=proxies, headers=headers).content
    except error.URLError as e:
        print("the url is wrong!" + str(e.reason))
        return None
    try:
        bsObj = BeautifulSoup(html, "lxml")
    except AttributeError as e:
        return None
    print(random.randint(1,10))
    return bsObj

#存储房型详细信息
def save_detaildata(house_info):
    with open("D:/MyPythonProjects/house_info_detail.csv", 'a+', newline='') as csvFile:
        writer = csv.writer(csvFile)
        if os.path.getsize('D:/MyPythonProjects/house_info_detail.csv') == 0:
            writer.writerow(['小区', '户型', '面积', '单价', '总价', '地址', '链接'])
        for house in house_info:
            writer.writerow([house.res_qua, house.house_type, house.area, house.unit_price, house.tot_price, house.addr, house.link])

#存储区域详细信息
def save_regiondata(region):
    with open("D:/MyPythonProjects/house_info_region.csv", 'a+', newline='') as csvFile:
        writer = csv.writer(csvFile)
        if os.path.getsize('D:/MyPythonProjects/house_info_region.csv') == 0:
            writer.writerow(['行政区', '区域', '总面积', '平均单价'])
        writer.writerow([region.distinct, region.name, region.tot_area, region.unit_price])

#读取代理IP池
def get_ippool():
    with open("D:/MyPythonProjects/ip_record.csv", 'r') as csvFile:
        reader = csv.reader(csvFile)
        headers = next(reader)
        ip_list = []
        for row in reader:
            record = {}
            record["ip"] = row[0]
            record["port"] = row[1]
            record["type"] = row[2]
            ip_list.append(record)
    return ip_list


def randHeader():
    head_connection = ['Keep-Alive', 'close']
    head_accept = ['text/html, application/xhtml+xml, */*']
    head_accept_language = ['zh-CN,fr-FR;q=0.5', 'en-US,en;q=0.8,zh-Hans-CN;q=0.5,zh-Hans;q=0.3']
    head_user_agent = ['Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
                       'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.95 Safari/537.36',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; rv:11.0) like Gecko)',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.2) Gecko/2008070208 Firefox/3.0.1',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070309 Firefox/2.0.0.3',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070803 Firefox/1.5.0.12',
                       'Opera/9.27 (Windows NT 5.2; U; zh-cn)',
                       'Mozilla/5.0 (Macintosh; PPC Mac OS X; U; en) Opera 8.0',
                       'Opera/8.0 (Macintosh; PPC Mac OS X; U; en)',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.12) Gecko/20080219 Firefox/2.0.0.12 Navigator/9.0.0.6',
                       'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Win64; x64; Trident/4.0)',
                       'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)',
                       'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E)',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Maxthon/4.0.6.2000 Chrome/26.0.1410.43 Safari/537.1 ',
                       'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E; QQBrowser/7.3.9825.400)',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0 ',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.92 Safari/537.1 LBBROWSER',
                       'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; BIDUBrowser 2.x)',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/3.0 Safari/536.11']

    header = {
        'Connection': head_connection[0],
        'Accept': head_accept[0],
        'Accept-Language': head_accept_language[1],
        'User-Agent': random.choice(head_user_agent),
        'Accept-Encoding': 'gzip, deflate, sdch',
    }
    return header

ip_list = get_ippool()

if __name__ == "__main__":
    url = parse.quote('http://sh.lianjia.com/ershoufang/', safe=string.printable)

    html = gethtml(url)

    distinct_list = get_distinctlist(html)

    region_list = get_regionlist(distinct_list)

    region_queue = region_list
    mutex = threading.Lock()
    Crawler_list = []
    for i in range(8):
        crl = Crawler(mutex)
        Crawler_list.append(crl)
    for Crawler in Crawler_list:
        Crawler.setDaemon(True)
        Crawler.start()
    for Crawler in Crawler_list:
        Crawler.join()
