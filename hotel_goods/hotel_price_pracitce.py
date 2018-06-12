# -*- coding: utf-8 -*-
import datetime
from time import sleep

import MySQLdb
import re
import scrapy
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class GoodsSpider():
    '''爬取商品信息'''
    name = 'room'
    hotel_url = 'http://hotel.meituan.com/'

    today = datetime.date.today()
    today = today + datetime.timedelta(days=3)
    # today_param = str(today).replace('-', '')
    tomorrow = today + datetime.timedelta(days=1)

    def start_requests(self):
        '''
        使用selenium爬取酒店房间商品信息
        :return:
        '''
        options = webdriver.ChromeOptions()
        options.add_argument('lang=zh_CN.UTF-8')
        options.add_argument(
            'user-agent="Mozilla/5.0 (iPod; U; CPU iPhone OS 2_1 like Mac OS X; ja-jp) AppleWebKit/525.18.1 (KHTML, like Gecko) Version/3.1.1 Mobile/5F137 Safari/525.20"')

        # chrome_options.add_argument('--headless')
        # chrome_options.add_argument('--disable-gpu')
        # driver = webdriver.Chrome(chrome_options=chrome_options)

        driver = webdriver.Chrome(chrome_options=options)

        hotel_ids = self.get_hotel_id()
        for hotel_id in hotel_ids:
            try:
                url = self.hotel_url + '%s/?ci=%s&co=%s' % (hotel_id, self.today, self.tomorrow)
                print(url)
                driver.get(url)
                WebDriverWait(driver,20,0.5).until(EC.presence_of_element_located((By.CLASS_NAME,"price-number")))

                html = driver.page_source.encode()
                goods_info=self.get_hotel_goods_info(html)
                update_info=self.get_update_info(html)

                self.save_info(goods_info,update_info)

                # driver.close()
            except:
                continue

    def get_hotel_goods_info(self, html):
        '''
        获得酒店房型商品价格信息
        '''
        soup = BeautifulSoup(html, 'html.parser')
        divs = soup.find_all(attrs={'class': 'book-info'})
        type_name = ''
        goods_name = ''
        goods_window = ''
        goods_breakfast = ''
        cancel = ''
        goods_price = 0.0
        goods_info = []

        hotel_name = soup.find_all(attrs={'class': 'fs26 fc3 pull-left bold'})[0].string
        print(hotel_name)

        for div in divs:
            type_name = div.ul.li.string
            tables=div.find_all('table')
            for table in tables:
                tds=table.find_all('td')
                goods_name=tds[0].span.string
                goods_window=tds[1].string
                if(goods_window=='None'):
                    goods_window='无窗户'
                goods_breakfast=tds[2].string
                if (goods_breakfast == 'None'):
                    goods_breakfast='无早餐'
                cancel=tds[3].span.text
                cancel=cancel.strip().replace(' ','').replace('\n',';')
                goods_price=float(tds[5].em.string)
                info = {'hotel_name': hotel_name, 'type_name': type_name, 'goods_name': goods_name,
                         'goods_window': goods_window, 'goods_breakfast': goods_breakfast, 'cancel': cancel,
                         'goods_price': goods_price, 'update_time':self.today}
                goods_info.append(info)

        return goods_info

    def get_update_info(self,html):
        '''
        获取每日更新的最低价，评分，评分等级，评论数，和日期
        :param html:
        :return:
        '''
        soup = BeautifulSoup(html, 'html.parser')
        hotel_name = soup.find_all(attrs={'class': 'fs26 fc3 pull-left bold'})[0].string
        grade=float(soup.find_all(attrs={'class': 'other-detail-line1-score'})[0].span.string)
        grade_des=soup.find_all(attrs={'class': 'other-detail-line1-level'})[0].string
        grade_des = grade_des.strip()
        min_price=float(soup.find_all(attrs={'class': 'start-price'})[0].em.span.string)
        comment_cnt=int((soup.find_all(attrs={'class': 'openComment'})[0].string)[2:-3])
        update_time=self.today
        update_info={'hotel_name':hotel_name,'grade':grade,'grade_des':grade_des,'min_price':min_price,'comment_cnt':comment_cnt,'update_time':update_time}

        return update_info

    def save_info(self, goods_info,update_info):
        '''
        将酒店房型信息存到数据库中
        '''
        print(len(goods_info))
        conn = MySQLdb.connect(host='rm-bp172z8x1m3m16m0pto.mysql.rds.aliyuncs.com', user='doushen',
                                                      passwd='greencherry', db='meituan', port=3306,charset="utf8")  # 链接数据库
        cur = conn.cursor()
        cur.execute("UPDATE hotel_info SET grade=%f ,grade_des='%s' , min_price=%f , comment_cnt=%d , update_time='%s' WHERE name='%s';"%(update_info['grade'],update_info['grade_des'],update_info['min_price'],update_info['comment_cnt'],update_info['update_time'],update_info['hotel_name']))

        for info in goods_info:
            cur.execute(
                "INSERT INTO hotel_goods(hotel_name,type_name,goods_name,goods_window,goods_breakfast,cancel,goods_price,update_time) VALUES ('%s','%s','%s','%s','%s','%s','%f','%s')" % (
                    info['hotel_name'], info['type_name'], info['goods_name'],
                    info['goods_window'], info['goods_breakfast'],
                    info['cancel'], info['goods_price'], info['update_time']))

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
        # result=[723210,5174170]
        cur.close()
        conn.close()
        return result


if __name__ == '__main__':
    spider=GoodsSpider()
    spider.start_requests()