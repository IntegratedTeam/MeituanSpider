# -*- coding: utf-8 -*-
import datetime
import json
import random

import re

import MySQLdb
import scrapy


class CommentSpider(scrapy.Spider):
    '''
    爬取每日更新的评论
    '''
    name = 'comment'
    comment_url = 'http://ihotel.meituan.com/group/v1/poi/comment/'
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
    yesterday=today-datetime.timedelta(days=1)

    def start_requests(self):
        '''
        爬取酒店列表页面，获得酒店id
        :return:
        '''
        headers = self.get_headers()
        hotel_ids=self.get_hotel_id()
        for id in hotel_ids:
            try:
                url = self.comment_url + '%d?sortType=time&noempty=1&withpic=0&filter=all&limit=100&offset=0' % (
                        id)
                yield scrapy.Request(url, callback=self.parse_hotel_comment, headers=headers)
            except:
                continue

    def parse_hotel_comment(self, response):
        '''
        获得具体的酒店评论信息
        :param response:
        :return:
        '''

        hotel_comment_info = self.get_hotel_comment_info(response)
        self.save_comment_info(response, hotel_comment_info)

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

    def get_hotel_comment_info(self, response):
        '''
        每日更新昨天的酒店评论信息
        :param response:
        :return:
        '''
        hotel_comment_info = []
        jsonresponse = json.loads(response.body_as_unicode())
        feedback = jsonresponse['data']['feedback']
        for comment in feedback:
            comment_time = datetime.datetime.strptime(comment['feedbacktime'], "%Y-%m-%d")
            if(not self.compare_time(comment_time)):
                continue
            hotel_name = (comment['shopname']).replace("（","(").replace("）",")")
            comment_star = comment['score']
            comment_tag = (";".join(re.findall(r'#.*?#', comment['comment']))).replace("'","")
            comment_content = (comment['comment']).replace("'","")
            update_time = self.today
            comment_info = {'hotel_name': hotel_name, 'comment_star': comment_star, 'comment_time': comment_time,
                            'comment_tag': comment_tag, 'comment_content': comment_content, 'update_time': update_time}
            hotel_comment_info.append(comment_info)
        return hotel_comment_info

    def save_comment_info(self, response, hotel_comment_info):
        '''
        将酒店评论存到数据库中
        :param hotel_comment_info:
        :return:
        '''
        conn = MySQLdb.connect(host='rm-bp172z8x1m3m16m0pto.mysql.rds.aliyuncs.com', user='doushen',
                               passwd='greencherry', db='meituan', port=3306, charset="utf8mb4")  # 链接数据库
        cur = conn.cursor()

        for comment_info in hotel_comment_info:
            cur.execute(
                "INSERT INTO hotel_comments_more(hotel_name,comment_star,comment_time,comment_tag,comment_content,update_time) VALUES ('%s',%d,'%s','%s','%s','%s')" % (
                    comment_info['hotel_name'], comment_info['comment_star'], comment_info['comment_time'],
                    comment_info['comment_tag'], comment_info['comment_content'], comment_info['update_time']))

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

        cur.execute("SELECT hotel_id from hotel_id;")
        results = cur.fetchall()
        result = []
        for r in results:
            result.append(r[0])
        cur.close()
        conn.close()
        return result


    def compare_time(self,time):
        '''
        将输入时间time和昨天的时间作比较，如果和昨天的时间相等就返回ture，否则为false
        :param time:
        :return:
        '''
        # if(self.yesterday==time.date()):
        #     return True
        # else:
        #     return False

        result=False
        #判断时间是否为6月
        t1=datetime.date(2018,6,1)
        t2=datetime.date(2018,6,30)
        if(time.date()>=t1 and time.date()<=t2):
            result=True
        return  result
