# -*- coding: utf-8 -*-
import json
import random
import re

import MySQLdb
import scrapy
import datetime
from bs4 import BeautifulSoup


class MeituanHotelSpiderSpider(scrapy.Spider):
    name = 'meituan_hotel_spider'
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
    today_param=str(today).replace('-','')
    tomorrow = today + datetime.timedelta(days=1)


    def start_requests(self):
        '''
        爬取酒店列表页面，获得酒店id
        :return:
        '''
        headers = self.get_headers()

        for i in range(51):
            url = self.start_url+'?utm_medium=pc&version_name=999.9&cateId=20&attr_28=129&cityId=55&offset=%d&limit=20&startDay=%s&endDay=%s&sort=defaults'%(i*20,self.today_param,self.today_param)
            yield scrapy.Request(url, callback=self.parse, headers=headers)

    def parse(self, response):
        '''
        获得每页酒店id
        :param response:
        :return:
        '''
        # with open('url.txt', 'a') as f:
        #     f.write(response.text)
        jsonresponse = json.loads(response.body_as_unicode())
        hotellist=jsonresponse['data']['searchresult']
        for hotel in hotellist:
            try:
                hotel_id = hotel['poiid']
                tag=';'.join(hotel['poiAttrTagList'])
                min_price=hotel['lowestPrice']
                consumption_num=hotel['poiSaleAndSpanTag']
                hotel_list_info={'hotel_id':hotel_id,'tag':tag,'min_price':min_price,'consumption_num':consumption_num}
                url = self.hotel_url + '%s/?ci=%s&co=%s' % (hotel_id, self.today, self.tomorrow)
                yield scrapy.Request(url, callback=self.parse_hotel_info, headers=self.get_headers(),meta=hotel_list_info)
            except:
                continue


    def parse_hotel_info(self, response):
        '''
        获得具体的酒店信息
        :param response:
        :return:
        '''
        # hotel = {}
        # hotel_room = self.get_hotel_room(response) #房型列表
        hotel_basic_info = self.get_hotel_basic_info(response)  # 基本信息
        # hotel['hotel_basic_info'] = hotel_basic_info
        # hotel['hotel_room'] = hotel_room

        self.save_hotel_info(hotel_basic_info)


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

    def get_params(self,page_num):
        '''
        获取访问每一页酒店列表的参数
        :return:
        '''
        params={}
        params['utm_medium']='pc'
        params['version_name']=999.9
        params['cateId'] =20
        params['attr_28'] =129
        params['cityId'] =55
        params['offset'] =page_num*20
        params['limit'] =20
        params['startDay'] =self.today_param
        params['endDay'] =self.today_param
        params['sort'] ='defaults'
        return params

    def get_hotel_room(self,response):
        '''
        获取酒店房型信息
        :param response:
        :return:
        '''
        hotel_room=[]
        hotel_detail_room = {}  # 房型信息


    def get_hotel_basic_info(self,response):
        '''
        获取酒店基本信息
        :param response:
        :return:
        '''
        hotel_id = int(response.url[25:-29])
        name = response.css('.fs26::text').extract()[0]

        tag=response.meta['tag']
        # if(len(response.css('.hotel-type .fs12::text').extract())>0):
        #     tag =response.css('.hotel-type .fs12::text').extract()[0]

        grade = float(response.css('.other-detail-line1-score .fs34::text').extract()[0])

        grade_des =''
        if(len(response.css('.other-detail-line1-level::text').extract())>0):
            grade_des = response.css('.other-detail-line1-level::text').extract()[0]
            grade_des=grade_des.strip()

        address=''
        if(len(response.css('.fs12.mt6.mb10 span::text').extract())>0):
            address = response.css('.fs12.mt6.mb10 span::text').extract()[0]

        area=''
        if(len(response.css('.fs12.mt6.mb10 a::text').extract())>0):
            area = response.css('.fs12.mt6.mb10 a::text').extract()[0]

        min_price=response.meta['min_price']
        # min_price = float(response.css('.start-price.price-color em span::text').extract()[0])
        facility = ';'.join(response.css('.service-icon.text-center.pull-left .fs12.fc9::text').extract())
        consumption_num = response.meta['consumption_num']

        comment_cnt=0
        if(len(response.css('.openComment::text').extract())>0):
            comment_cnt = int(response.css('.openComment::text').extract()[0][2:-3])

        contact=''
        if(len(response.css('.ml20.mr20.pt20.pb20.clear dd span::text').extract())>0):
            contact = response.css('.ml20.mr20.pt20.pb20.clear dd span::text').extract()[0]

        info=''
        if(len(response.css('.ml20.mr20.pt20.pb20.clear dd span::text').extract())>1):
            info = response.css('.ml20.mr20.pt20.pb20.clear dd span::text').extract()[1]

        introduction=''
        if(len(response.css('.ml20.mr20.pt20.pb20.clear dd span::text').extract())>2):
            introduction = response.css('.ml20.mr20.pt20.pb20.clear dd span::text').extract()[2]

        policy=''
        if(len(response.css('.ml20.mr20.pt20.pb20.clear dd span::text').extract())>3):
            policy = response.css('.ml20.mr20.pt20.pb20.clear dd span::text').extract()[3]

        update_time = self.today

        hotel_basic_info = {'hotel_id': hotel_id, 'name': name, 'tag':tag,'grade': grade, 'grade_des': grade_des,
                            'address': address, 'area': area, 'min_price':min_price, 'facility': facility,
                             'consumption_num':consumption_num,'comment_cnt': comment_cnt, 'contact': contact,
                            'info': info, 'introduction': introduction, 'policy': policy, 'update_time': update_time}
        return hotel_basic_info


    def save_hotel_info(self,hotel_basic_info):
        conn = MySQLdb.connect(host='rm-bp172z8x1m3m16m0pto.mysql.rds.aliyuncs.com', user='doushen',
                               passwd='greencherry', db='meituan', port=3306,charset="utf8")  # 链接数据库
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO hotel_info(hotel_id,name,tag,grade,grade_des,address,area,min_price,facility,consumption_num,comment_cnt,contact,info,introduction,policy,update_time) VALUES (%d,'%s','%s',%f,'%s','%s','%s',%f,'%s','%s',%d,'%s','%s','%s','%s','%s')" % (
                hotel_basic_info['hotel_id'], hotel_basic_info['name'], hotel_basic_info['tag'],
                hotel_basic_info['grade'], hotel_basic_info['grade_des'], hotel_basic_info['address'],
                hotel_basic_info['area'], hotel_basic_info['min_price'], hotel_basic_info['facility'],
                hotel_basic_info['consumption_num'], hotel_basic_info['comment_cnt'], hotel_basic_info['contact'],
                hotel_basic_info['info'], hotel_basic_info['introduction'], hotel_basic_info['policy'],
                hotel_basic_info['update_time']))

        conn.commit()
        cur.close()
        conn.close()