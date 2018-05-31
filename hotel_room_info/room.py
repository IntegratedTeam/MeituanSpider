# -*- coding: utf-8 -*-
import datetime

import MySQLdb
import scrapy
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class RoomSpider():
    '''爬取房型信息'''
    name = 'room'
    hotel_url = 'http://hotel.meituan.com/'

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

        hotel_ids = self.get_hotel_id()
        for hotel_id in hotel_ids:
            try:
                driver = webdriver.Chrome(chrome_options=chrome_options)
                url = self.hotel_url + '%s/?ci=%s&co=%s' % (hotel_id, self.today, self.tomorrow)
                print(url)
                driver.get(url)
                WebDriverWait(driver,20,0.5).until(EC.presence_of_element_located((By.CLASS_NAME,"price-number")))
                html = driver.page_source.encode()
                room_info=self.get_hotel_room_info(html)
                self.save_room_info(room_info)
                driver.close()
            except:
                continue

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
                         'type_area': type_area, 'type_bed': type_bed,'update_time':self.today}
            room_info.append(type_info)

        return room_info

    def save_room_info(self, hotel_room_info):
        '''
        将酒店房型信息存到数据库中
        '''
        print(len(hotel_room_info))
        conn = MySQLdb.connect(host='rm-bp172z8x1m3m16m0pto.mysql.rds.aliyuncs.com', user='doushen',
                                                      passwd='greencherry', db='meituan', port=3306,charset="utf8")  # 链接数据库
        cur = conn.cursor()
        for type_info in hotel_room_info:
            cur.execute(
                "INSERT INTO hotel_room(hotel_name,type_name,type_website,type_bathroom,type_window,type_people_num,type_bed,type_area,update_time) VALUES ('%s','%s','%s','%s','%s',%d,'%s','%s','%s')" % (
                    type_info['hotel_name'], type_info['type_name'], type_info['type_website'],
                    type_info['type_bathroom'], type_info['type_window'], type_info['type_people_num'],
                    type_info['type_bed'], type_info['type_area'], type_info['update_time']))

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
                                                      passwd='greencherry', db='meituan', port=3306,charset="utf8")  # 链接数据库
        cur = conn.cursor()
        cur.execute("SELECT hotel_id from hotel_ids;")
        results = cur.fetchall()
        result = []
        for r in results:
            result.append(r[0])
        cur.close()
        conn.close()
        return result


if __name__ == '__main__':
    spider=RoomSpider()
    spider.start_requests()