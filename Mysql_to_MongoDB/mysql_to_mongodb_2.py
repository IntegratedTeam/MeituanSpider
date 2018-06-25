# -*- coding: utf-8 -*-
# 此文件转移过去的酒店评论

import csv
import pymongo
import MySQLdb
from Mysql_to_MongoDB.mongodb_conn import MongoConn

conn = MySQLdb.connect(host='rm-bp172z8x1m3m16m0pto.mysql.rds.aliyuncs.com', user='doushen',
                       passwd='greencherry', db='meituan', port=3306, charset="utf8")  # 链接数据库
cur = conn.cursor()
myconn=MongoConn()
csv_file=csv.reader(open('hotel.csv','r',encoding='utf-8'))
flag=True
i=0
for row in csv_file:
    i+=1
    if(flag):
        flag=False
        continue
    meituan=row[0]
    print(meituan)
    print(i)
    cur.execute("select * from hotel_comments where hotel_name='%s'" % (meituan))
    results = cur.fetchall()
    datas=[]
    for result in results:
        if(result[5]==''):continue
        data={}
        data['hotel_nane']=result[1]
        data['comment_star']=result[2]
        data['comment_time']=result[3]
        data['comment_tag']=result[4]
        data['comment_content']=result[5]
        datas.append(data)
    if (len(datas)>0):
        myconn.db['hotel_comments'].insert(datas)

cur.close()
conn.close()