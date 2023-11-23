# -*- coding:utf-8 -*-
import sys
import os
# 把当前文件所在文件夹的父文件夹路径加入到PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.other import mongoUtils as mu


def init():
    mongod_con = mu.db_connection("spread_daily_temp")
    return mongod_con

def find_one_from_mongodb(condition):
    mongod_con = init()
    
    """ query info from mongodb """
    x = mongod_con.find_one(condition)
    return x

def batch_insert_to_mongodb(dataList):
    mongod_con = init()
    
    """ save info to mongodb """
    for each_item in dataList:
        x = mongod_con.insert_one(each_item)

def insert_one_to_mongodb(data):
    mongod_con = init()
    
    """ save info to mongodb """
    x = mongod_con.insert_one(data)

def update_to_mongodb(condition,data):
    mongod_con = init()
    
    """ update info to mongodb """
    x = mongod_con.update(condition,{"$set":data})

def delete_one_from_mongodb(condition):
    mongod_con = init()

    """ delete_one_from_mongodb """
    x = mongod_con.delete_one(condition)

def delete_many_from_mongodb(condition):
    mongod_con = init()

    """ delete_one_from_mongodb """
    x = mongod_con.delete_many(condition)
