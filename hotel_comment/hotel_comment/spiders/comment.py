# -*- coding: utf-8 -*-
import datetime
import json
import random

import re

import MySQLdb
import scrapy


class CommentSpider(scrapy.Spider):
    name = 'comment'
    start_url = 'https://ihotel.meituan.com/hbsearch/HotelSearch'
    hotel_url = 'http://hotel.meituan.com/'
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

    def start_requests(self):
        '''
        爬取酒店列表页面，获得酒店id
        :return:
        '''
        headers = self.get_headers()

        for i in range(51):
            url = self.start_url + '?utm_medium=pc&version_name=999.9&cateId=20&attr_28=129&cityId=55&offset=%d&limit=20&startDay=%s&endDay=%s&sort=defaults' % (
                i * 20, self.today_param, self.today_param)
            yield scrapy.Request(url, callback=self.parse, headers=headers)

    def parse(self, response):
        '''
        获得每页酒店id
        :param response:
        :return:
        '''
        jsonresponse = json.loads(response.body_as_unicode())
        hotellist = jsonresponse['data']['searchresult']
        for hotel in hotellist:
            try:
                hotel_id = hotel['poiid']
                min_price = hotel['lowestPrice']
                hotel_name = hotel['name']
                comment_cnt = int(hotel['commentsCountDesc'][0:-3])
                consumption_num = hotel['poiSaleAndSpanTag']
                hotel_list_info = {'hotel_id': hotel_id, 'hotel_name': hotel_name, 'comment_cnt': comment_cnt,
                                   'min_price': min_price,
                                   'consumption_num': consumption_num}
                count = comment_cnt
                if (count > 1000):
                    count = 1000
                for i in range(0, count, 100):
                    url = self.comment_url + '%d?sortType=time&noempty=1&withpic=0&filter=all&limit=100&offset=%d' % (
                        hotel_id, i)
                    yield scrapy.Request(url, callback=self.parse_hotel_comment, headers=self.get_headers(),
                                         meta=hotel_list_info)
            except:
                continue

    def parse_hotel_comment(self, response):
        '''
        获得具体的酒店评论信息
        :param response:
        :return:
        '''
        if (self.check_hotel_name(response)):
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
            'Referer': 'https://gupiao.baidu.com/',
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

    def get_hotel_comment_info(self, response):
        '''
        获得酒店评论信息
        :param response:
        :return:
        '''
        hotel_comment_info = []
        jsonresponse = json.loads(response.body_as_unicode())
        feedback = jsonresponse['data']['feedback']
        for comment in feedback:
            hotel_name = comment['shopname']
            comment_star = comment['score']
            comment_time = datetime.datetime.strptime(comment['feedbacktime'], "%Y-%m-%d")
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
                "INSERT INTO hotel_comments(hotel_name,comment_star,comment_time,comment_tag,comment_content,update_time) VALUES ('%s',%d,'%s','%s','%s','%s')" % (
                    response.meta['hotel_name'], comment_info['comment_star'], comment_info['comment_time'],
                    comment_info['comment_tag'], comment_info['comment_content'], comment_info['update_time']))

        conn.commit()

        print('ok')
        cur.close()
        conn.close()

    def check_hotel_name(self, response):
        '''
        判断酒店是否已经保存过
        :param response:
        :return:
        '''
        # conn = MySQLdb.connect(host='rm-bp172z8x1m3m16m0pto.mysql.rds.aliyuncs.com', user='doushen',
        #                        passwd='greencherry', db='meituan', port=3306, charset="utf8mb4")  # 链接数据库
        # cur = conn.cursor()
        #
        hotel_name = response.meta['hotel_name']
        # cur.execute("SELECT count(*) FROM hotel_comments WHERE hotel_name='%s'" % hotel_name)
        # results = cur.fetchall()
        # result = results[0][0] <= 0
        # cur.close()
        # conn.close()
        # print(hotel_name+':'+str(result))
        hotel_names = ["南京维元国际大酒店", "肯定之星连锁酒店(新街口店)",
                       "Zsmart智尚酒店南京夫子庙店",
                       "莫泰酒店(南京广州路随园大厦店)(原南京大学广州路店)",
                       "硕达宾馆(夫子庙新街口店)", "金汇大酒店（晶丽酒店管理)",
                       "锦江之星(南京新街口店)", "7天优品酒店(南京新街口地铁站店)",
                       "南京天丰大酒店", "南京商茂国际酒店", "乐华宾馆(夫子庙中华路店)",
                       "汉庭(南京汉中门地铁站店)", "布丁酒店(南京夫子庙地铁站店)",
                       "清沐连锁酒店(南京新街口大洋百货丰富路店)",
                       "尚客优快捷酒店(南京夫子庙店)",
                       "唯爱之星主题宾馆(新街口上海路地铁站店)",
                       "南京中心大酒店",
                       "汉庭(南京新街口中心店)",
                       "尚客优快捷酒店(南京新街口中山南路店)",
                       "如家商旅酒店(南京新街口店)",
                       "莫泰酒店(南京新街口商业中心明瓦廊店)",
                       "速8酒店(珠江路浮桥地铁站1912店)",
                       "如家酒店(南京夫子庙龙蟠中路电视台店)",
                       "莫泰酒店(南京夫子庙大光路店)",
                       "百时快捷酒店(南京新街口店)",
                       "如家酒店(南京新街口张府园地铁站店)",
                       "布丁酒店(南京鼓楼医院珠江路地铁站店)",
                       "清沐连锁酒店(南京1912珠江路百脑汇店)",
                       "布丁酒店(南京夫子庙张府园地铁站店)",
                       "布丁酒店(南京夫子庙常府街地铁站店)",
                       "如家酒店(南京新街口店)",
                       "7天连锁酒店(南京夫子庙御道街店)",
                       "布丁酒店(南京省中医院上海路地铁站店)",
                       "格林豪泰快捷酒店(南京玄武湖中山陵景区店)",
                       "银百快捷酒店",
                       "格林豪泰快捷酒店(南京新街口地铁站店)",
                       "如家酒店(南京中山东路解放路店)",
                       "悦尚宾馆(军区总院南京夫子庙店)",
                       "7天连锁酒店(南京夫子庙大光路店)",
                       "易佰连锁旅店(南京夫子庙地铁站建康路店)",
                       "和颐酒店(南京明故宫店)",
                       "南京苏宁雅悦行政公寓",
                       "弗思特连锁酒店(九华山地铁站店)",
                       "雨后阳光国际青年旅舍",
                       "格林联盟酒店(南京鸡鸣寺地铁站东南大学店)",
                       "清沐连锁酒店(南京五台山广州路随园大厦店)",
                       "南京钞库街十八号生活酒店",
                       "Zsmart智尚酒店南京夫子庙大光路店",
                       "7天连锁酒店(南京新街口常府街地铁站店)",
                       "汉庭(南京中山东路总统府店)",
                       "时光机国际青年旅舍(王府大街新街口地铁站店)",
                       "南京金陵饭店",
                       "星e站快捷酒店(新街口珠江路店)",
                       "汉庭(南京夫子庙中华门店)",
                       "7天连锁酒店(南京总统府西安门地铁站店)",
                       "如家酒店(南京南航大学明故宫瑞金路店)",
                       "中山大厦",
                       "骏怡连锁酒店(南京珠江路东南大学店)",
                       "海友酒店(南京夫子庙白下路店)",
                       "7天连锁酒店(南京1912酒吧区浮桥地铁站店)",
                       "义都精品酒店(夫子庙御道街店)",
                       "月色风尚·影院酒店(新街口城开国际店)",
                       "肯定连锁酒店(1912珠江路浮桥地铁站店)",
                       "清沐连锁酒店(南京1912珠江路西大影壁店)",
                       "南京夫子庙亚朵酒店",
                       "幸运草客栈",
                       "南京涵田城市酒店",
                       "如家酒店(南京新街口大行宫地铁站总统府店)",
                       "清沐精品酒店(南京丹凤街店)",
                       "如家酒店(南京新街口中心店)",
                       "肯定精选酒店(新街口中心店)",
                       "南京凯铂精品酒店",
                       "肯定精品酒店(新街口常府街地铁站店)",
                       "清沐精选酒店(新街口杨公井店)",
                       "7天连锁酒店(南京珠江路东南大学店)",
                       "古都文化酒店(南京新街口店)",
                       "南京猫咖主题青旅",
                       "鸟巢主题酒店(原爱潮酒店张府园分店)",
                       "背包青年旅舍(夫子庙店)",
                       "汉庭(南京常府街店)",
                       "速8酒店(南京舜天总统府店)",
                       "7天连锁酒店(南京夫子庙地铁站景区店)",
                       "南京未来树生活酒店",
                       "年发168酒店(珠江路军区总医院店)",
                       "如家商旅(南京夫子庙通济门店)",
                       "小庄花园青年旅舍(新街口中心店)",
                       "如家酒店(新街口珠江路地铁站德基广场店)",
                       "汉庭(南京明故宫瑞金路店)",
                       "全季酒店(南京龙蟠中路店)",
                       "传家酒店(新街口店)",
                       "清竹之星快捷宾馆",
                       "格林豪泰快捷酒店(南京夫子庙太平南路店)",
                       "好运来旅店(上海路地铁站店)",
                       "凯克酒店式公寓(南大君临国际店)",
                       "心之恋情侣主题酒店(原万爱酒店解放路店)",
                       "如家酒店(南京夫子庙三山街地铁站店)",
                       "万客居宾馆(夫子庙店)"]
        if (hotel_name in hotel_names):
            result = False
            print(hotel_name + ':' + str(result))
        else:
            result = True
        return result
