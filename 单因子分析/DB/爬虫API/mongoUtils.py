# -*- coding:utf-8 -*-
import os
import sys

# 把当前文件所在文件夹的父文件夹路径加入到PYTHONPATH

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymongo
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)


# 接后续代码

def db_connection(collectionsName):
    """ Connection to local mongo db service."""

    try:
        myclient = pymongo.MongoClient("mongodb://root:Aa!122911@43.136.59.164:26028/")
        mydb = myclient["localv25"]
        mongod_con = mydb[collectionsName]
    except Exception as e:
        print('数据库连接异常！！！')
    return mongod_con
