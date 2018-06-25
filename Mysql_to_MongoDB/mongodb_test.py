# -*- coding: utf-8 -*-
import datetime

from Mysql_to_MongoDB.mongodb_conn import MongoConn

myconn=MongoConn()
datas=[{'time':datetime.datetime.today()}]
myconn.db['hotel_comments'].insert(datas)