# -*- coding: utf-8 -*-
import datetime

import MySQLdb
import scrapy
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options

class RoomSpider(scrapy.Spider):
    '''爬取房型信息'''
    name = 'room'
    hotel_url = 'http://hotel.meituan.com/'
    user_agent_list = [ \
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1" \
        "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11", \
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6", \
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6", \
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1", \
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5", \
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5", \
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3", \
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3", \
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3", \
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3", \
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3", \
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3", \
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3", \
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3", \
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3", \
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24", \
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"
    ]
    today = datetime.date.today()
    today_param = str(today).replace('-', '')
    tomorrow = today + datetime.timedelta(days=1)

    def start_requests(self):
        '''
        爬取酒店列表页面，获得酒店id
        :return:
        '''
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        driver = webdriver.Chrome(chrome_options=chrome_options)
        hotel_ids = self.get_hotel_id()
        for hotel_id in hotel_ids:
            try:
                url = self.hotel_url + '%s/?ci=%s&co=%s' % (hotel_id, self.today, self.tomorrow)
                print(url)
                driver.get(url)
                html = driver.page_source.encode()
                room_info=self.get_hotel_room_info(html)
                self.save_room_info(room_info)
            except:
                continue
        driver.close()

    def get_hotel_room_info(self, html):
        '''
        获得酒店房型信息
        '''
        soup = BeautifulSoup(html, 'html.parser')
        uls = soup.find_all(attrs={'class': 'deal-margin-left hotel-service-info'})
        type_name = ''
        type_website = ''
        type_bathroom = ''
        type_window = ''
        type_people_num = 0
        type_area = ''
        type_bed = ''
        room_info = []

        hotel_name = soup.find_all(attrs={'class': 'fs26 fc3 pull-left bold'})[0].string
        print(hotel_name)

        for ul in uls:
            type_name = ul.li.string
            spans = ul.find_all('span')

            for i in range(0, len(spans), 2):
                if (spans[i].string == '上网'):
                    type_website = spans[i + 1].string
                if (spans[i].string == '卫浴'):
                    type_bathroom = spans[i + 1].string
                if (spans[i].string == '窗户'):
                    type_window = spans[i + 1].string
                if (spans[i].string == '可住'):
                    type_people_num = int((spans[i + 1].string)[:-1])
                if (spans[i].string == '面积'):
                    type_area = spans[i + 1].string
                if (spans[i].string == '床型'):
                    type_bed = spans[i + 1].string

            type_info = {'hotel_name': hotel_name, 'type_name': type_name, 'type_website': type_website,
                         'type_bathroom': type_bathroom, 'type_window': type_window, 'type_people_num': type_people_num,
                         'type_area': type_area, 'type_bed': type_bed}
            room_info.append(type_info)
        return room_info

    def save_room_info(self, hotel_room_info):
        '''
        将酒店房型信息存到数据库中
        '''
        conn = MySQLdb.connect(host='rm-bp172z8x1m3m16m0pto.mysql.rds.aliyuncs.com', user='doushen',
                               passwd='greencherry', db='meituan', port=3306, charset="utf8mb4")  # 链接数据库
        cur = conn.cursor()

        for type_info in hotel_room_info:
            cur.execute(
                "INSERT INTO hotel_room(hotel_name,type_name,type_website,type_bathroom,type_window,type_people_num,type_bed,type_area,update_time) VALUES ('%s','%s','%s','%s','%s',%d,,'%s','%s','%s')" % (
                    type_info['hotel_name'], type_info['type_name'], type_info['type_website'],
                    type_info['type_bathroom'], type_info['type_window'], type_info['type_people_num'],
                    type_info['type_bed'], type_info['type_area'], type_info['update_time'],))

        conn.commit()
        print('ok')
        cur.close()
        conn.close()

    def get_hotel_id(self):
        '''
        获取所有酒店id
        :return:
        '''
        conn = MySQLdb.connect(host='rm-bp172z8x1m3m16m0pto.mysql.rds.aliyuncs.com', user='doushen',
                               passwd='greencherry', db='meituan', port=3306, charset="utf8mb4")  # 链接数据库
        cur = conn.cursor()

        cur.execute("SELECT hotel_id from hotel_ids;")
        results = cur.fetchall()
        result = []
        for r in results:
            result.append(r[0])
        cur.close()
        conn.close()
        return result
