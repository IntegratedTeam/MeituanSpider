# -*- coding: utf-8 -*-
import csv

import MySQLdb

conn = MySQLdb.connect(host='rm-bp172z8x1m3m16m0pto.mysql.rds.aliyuncs.com', user='doushen',
                       passwd='greencherry', db='meituan', port=3306, charset="utf8")  # 链接数据库
cur = conn.cursor()

csv_file=csv.reader(open('hotel.csv','r',encoding='utf-8'))
flag=True
for row in csv_file:
    if(flag):
        flag=False
        continue
    meituan=row[0]
    ctrip=row[1]
    qunai=row[2]
    cur.execute("INSERT INTO all_hotel(meituan,ctrip,qunai) VALUES ('%s','%s','%s')" % (meituan,ctrip,qunai))


conn.commit()
cur.close()
conn.close()
