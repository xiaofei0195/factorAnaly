# -*- coding:utf-8 -*-
import sys
import os
# 把当前文件所在文件夹的父文件夹路径加入到PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import mongoUtils as mu


def init():
    mongod_con = mu.db_connection("trading_info_daily")
    return mongod_con

def find_all_from_mongodb(condition):
    mongod_con = init()
    
    # """ query info from mongodb """
    date_cursor = mongod_con.find(condition)
    arr = []
    for x in date_cursor:
        # 将每条数据添加到数组中
        arr.append(x)
    return arr
    
def find_from_mongodb_with_condition(conditon,sortCon):
    mongod_con = init()

    """ query info from mongodb """
    x = mongod_con.find(conditon).sort(sortCon,-1)
    return x


def find_one_from_mongodb(condition):
    mongod_con = init()
    
    """ query info from mongodb """
    x = mongod_con.find_one(condition)
    return x

def insert_one_to_mongodb(data):
    mongod_con = init()
    
    """ save info to mongodb """
    x = mongod_con.insert_one(data)


def batch_insert_to_mongodb(dataList):
    mongod_con = init()
    
    """ save info to mongodb """
    for each_item in dataList:
        x = mongod_con.insert_one(each_item)


def update_to_mongodb(condition,data):
    mongod_con = init()
    
    """ update info to mongodb """
    x = mongod_con.update(condition,{"$set":data})


def update_many_to_mongodb(condition,data):
    mongod_con = init()

    """ update info to mongodb """
    x = mongod_con.update_many(condition,{"$set":data})


def delete_one_from_mongodb(condition):
    mongod_con = init()

    """ delete_one_from_mongodb """
    x = mongod_con.delete_one(condition)

def delete_many_from_mongodb(condition):
    mongod_con = init()

    """ delete_one_from_mongodb """
    x = mongod_con.delete_many(condition)


# if __name__ == '__main__':

    # pl = ['CY','WH','RS','FB','WH','WR','CY','AG','AU','SC','B']
    # for p in pl:
    #     queryCon = {}
    #     queryCon['productCode'] = p
    #     queryCon['mon1'] = '11'
    #     queryCon['mon2'] = '01'
    #     ret = find_one_from_mongodb(queryCon)
    #     print(ret)
