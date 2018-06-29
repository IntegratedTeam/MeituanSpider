# -*- coding: utf-8 -*-
import random

import MySQLdb
import datetime
import scrapy
import json


class PictureSpider(scrapy.Spider):
    name = 'picture'
    start_url = 'https://ihotel.meituan.com/hbsearch/HotelSearch'
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
        爬取酒店列表页面
        :return:
        '''
        headers = self.get_headers()

        for i in range(51):
            url = self.start_url + ('?utm_medium=pc&version_name=999.9&cateId=20&attr_28=129&cityId=55&offset=%d&limit=20&startDay=%s&endDay=%s&sort=defaults&uuid=941293904FEFB0102E06F3B8A94589C98CF4A2CBF8DF0BF957D92E52B7E44034%%401528775454166' )% (i * 20, self.today_param, self.today_param)
            yield scrapy.Request(url, callback=self.parse, headers=headers)

    def parse(self, response):
        '''
        获得酒店的经纬度
        :param response:
        :return:
        '''
        jsonresponse = json.loads(response.body_as_unicode())
        hotellist = jsonresponse['data']['searchresult']
        hotel_pictures=[]
        for hotel in hotellist:
            # try:
                hotel_id = hotel['poiid']
                hotel_pic=hotel['frontImg']
                name=hotel['name']
                hotel_picture={}
                hotel_picture['hotel_id']=hotel_id
                hotel_picture['hotel_name'] = name
                hotel_picture['hotel_pic']=hotel_pic.replace('w.h','320.0')
                hotel_pictures.append(hotel_picture)
        self.save_hotel_pic(hotel_pictures)

    def get_headers(self):
        '''
        生成http头，避免反爬机制
        :return:
        '''
        ua = random.choice(self.user_agent_list)  # 随机抽取User-Agent
        headers = {
            'Accept-Encoding': 'gzip, deflate, sdch, br',
            'Accept-Language': 'zh-CN,zh;q=0.8',
            'Connection': 'keep-alive',
            'Referer': 'http://hotel.meituan.com/',
            'User-Agent': ua
        }
        return headers

    def get_params(self, page_num):
        '''
        获取访问每一页酒店列表的参数
        :return:
        '''
        params = {}
        params['utm_medium'] = 'pc'
        params['version_name'] = 999.9
        params['cateId'] = 20
        params['attr_28'] = 129
        params['cityId'] = 55
        params['offset'] = page_num * 20
        params['limit'] = 20
        params['startDay'] = self.today_param
        params['endDay'] = self.today_param
        params['sort'] = 'defaults'
        return params

    def save_hotel_pic(self, hotel_pictures):
        conn = MySQLdb.connect(host='rm-bp1674o31r1yi66t00o.mysql.rds.aliyuncs.com', user='happy_root',
                               passwd='Nju_Happy_Root_2015', db='meituan', port=3306, charset="utf8")  # 链接数据库
        cur = conn.cursor()
        for hotel_picture in hotel_pictures:
            print(hotel_picture['hotel_name'])
            print(hotel_picture['hotel_pic'])
            cur.execute(
            "INSERT INTO hotel_picture(hotel_id,hotel_name,hotel_pic) VALUES (%d,'%s','%s')" % (
                hotel_picture['hotel_id'],hotel_picture['hotel_name'],hotel_picture['hotel_pic']))

        conn.commit()
        print('ok')
        cur.close()
        conn.close()