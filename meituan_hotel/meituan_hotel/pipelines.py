# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import MySQLdb


class MeituanHotelPipeline(object):
    # conn = None
    # cur = None
    #
    # def open_spider(self, spider):
    #     '''
    #     open_spider表示当一个爬虫被调用时，pipline启动的方法
    #     :param spider:
    #     :return:
    #     '''
    #     self.conn = MySQLdb.connect(host='rm-bp172z8x1m3m16m0pto.mysql.rds.aliyuncs.com', user='doushen',
    #                                 passwd='greencherry', db='meituan', port=3306)  # 链接数据库
    #     self.cur = self.conn.cursor()
    #
    # def close_spider(self, spider):
    #     '''
    #     close_spider表示当一个爬虫结束时，所调用的方法
    #     :param spider:
    #     :return:
    #     '''
    #
    #     self.cur.close()
    #     self.conn.close()


    def process_item(self, item, spider):
        # hotel_basic_info=item
        #
        # self.cur.execute(
        #         "INSERT INTO hotel_info(hotel_id,name,tag,grade,grade_des,address,area,min_price,facility,consumption_num,comment_cnt,contact,info,introduction,policy,update_time) VALUES (%d,'%s','%s',%f,'%s','%s','%s',%f,'%s','%s',%d,'%s','%s','%s','%s','%s')" % (
        #             hotel_basic_info['hotel_id'], hotel_basic_info['name'], hotel_basic_info['tag'],
        #             hotel_basic_info['grade'], hotel_basic_info['grade_des'], hotel_basic_info['address'],
        #             hotel_basic_info['area'], hotel_basic_info['min_price'], hotel_basic_info['facility'],
        #             hotel_basic_info['consumption_num'], hotel_basic_info['comment_cnt'], hotel_basic_info['contact'],
        #             hotel_basic_info['info'], hotel_basic_info['introduction'], hotel_basic_info['policy'],
        #             hotel_basic_info['update_time']))
        #
        # self.conn.commit()


        return item
