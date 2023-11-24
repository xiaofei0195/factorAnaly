# -*- coding:utf-8 -*-
import sys
import os
# 把当前文件所在文件夹的父文件夹路径加入到PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import datetime
import requests
import utils.other.comUtils as comUtils
from mapper import futureDailyTradingInfoMapper as dailyTradingInfo, \
    futureDailyFullCarryInfoMapper as fullCarryMapper

productNameList = ['沪铜', '沪镍', '沪锡', '沪铅', '沪银', '沪铝', '沪锌', '铁矿', '不锈钢', '螺纹', '热卷', '锰硅', '硅铁', '焦煤', '焦炭', '豆一',
                   '棕榈', '豆油', '菜油', '豆粕', '菜粕', '玉米', '淀粉', '棉花', '油菜籽', '鸡蛋', '生猪', '棉纱', '聚丙烯', '乙二醇', '短纤', '沥青',
                   '塑料', '原油', '燃油', '低燃油', '液化气', '橡胶', '20号胶', '玻璃', 'PVC', '纯碱', '甲醇', 'PTA', '尿素', '纸浆', '苯乙烯']
productExcludeList = ['沪金', '沪银', '国际铜', '焦煤', '焦炭', '纤维板', '胶合板', '胶合板', '油菜籽', '棉纱', '生猪', '红枣', '花生', '早稻', '晚稻',
                      '粳米', '普麦', '强麦']
productSpecAgriList = ['棉花', '白糖', '淀粉', '玉米', '菜油', '豆一', '豆粕', '菜粕', '豆油']
selectList = ['聚丙烯', '乙二醇', '短纤', '豆一', '菜粕', '鸡蛋', '菜油']


productNameAndCodeList = [{'productName': '沪铜', 'productCode': 'CU'}, {'productName': '沪镍', 'productCode': 'NI'},
                          {'productName': '沪锡', 'productCode': 'SN'}, {'productName': '沪铅', 'productCode': 'PB'},
                          {'productName': '沪银', 'productCode': 'AG'}, {'productName': '沪铝', 'productCode': 'AL'},
                          {'productName': '沪锌', 'productCode': 'ZN'}, {'productName': '铁矿', 'productCode': 'I'},
                          {'productName': '不锈钢', 'productCode': 'SS'}, {'productName': '螺纹', 'productCode': 'RB'},
                          {'productName': '热卷', 'productCode': 'HC'}, {'productName': '锰硅', 'productCode': 'SM'},
                          {'productName': '硅铁', 'productCode': 'SF'}, {'productName': '焦煤', 'productCode': 'JM'},
                          {'productName': '焦炭', 'productCode': 'J'}, {'productName': '豆一', 'productCode': 'A'},
                          {'productName': '棕榈', 'productCode': 'P'}, {'productName': '豆油', 'productCode': 'Y'},
                          {'productName': '菜油', 'productCode': 'OI'}, {'productName': '豆粕', 'productCode': 'M'},
                          {'productName': '菜粕', 'productCode': 'RM'}, {'productName': '玉米', 'productCode': 'C'},
                          {'productName': '淀粉', 'productCode': 'CS'}, {'productName': '棉花', 'productCode': 'CF'},
                          {'productName': '油菜籽', 'productCode': 'RS'}, {'productName': '鸡蛋', 'productCode': 'JD'},
                          {'productName': '生猪', 'productCode': 'LH'}, {'productName': '棉纱', 'productCode': 'CY'},
                          {'productName': '聚丙烯', 'productCode': 'PP'}, {'productName': '乙二醇', 'productCode': 'EG'},
                          {'productName': '短纤', 'productCode': 'PF'}, {'productName': '沥青', 'productCode': 'BU'},
                          {'productName': '塑料', 'productCode': 'L'}, {'productName': '原油', 'productCode': 'SC'},
                          {'productName': '燃油', 'productCode': 'FU'}, {'productName': '低燃油', 'productCode': 'LU'},
                          {'productName': '液化气', 'productCode': 'PG'}, {'productName': '橡胶', 'productCode': 'RU'},
                          {'productName': '20号胶', 'productCode': 'NR'}, {'productName': '玻璃', 'productCode': 'FG'},
                          {'productName': 'PVC', 'productCode': 'V'}, {'productName': '纯碱', 'productCode': 'SA'},
                          {'productName': '甲醇', 'productCode': 'MA'}, {'productName': 'PTA', 'productCode': 'TA'},
                          {'productName': '尿素', 'productCode': 'UR'}, {'productName': '纸浆', 'productCode': 'SP'},
                          {'productName': '苯乙烯', 'productCode': 'EB'}]


# 查询fullCarry（通过交易法门）
def geFullCarryByJYFM(beginCode,endCode):
    # 获取当天日期yyyy-MM-dd
    nowDate = datetime.datetime.now()
    today = nowDate.strftime("%Y-%m-%d")

    r = requests.post('https://www.jiaoyifamen.com/tools/api//future/full/carry', data={'beginCode': beginCode,'endCode': endCode,"ratio":4})
    retData = r.json()
    # print(retData)
    # 获取现货价格
    data = retData['data']
    table_data = data['table_data']
    # print(table_data)

    for fullCarryInfo in table_data:
        spreadRatio = fullCarryInfo['spreadRatio']
        productName = fullCarryInfo['productName']
        if productName == '低硫燃料油':
            productName = '低燃油'
        if productName == '液化石油气':
            productName = '液化气'

        if type(spreadRatio) is float:
            carryData = {}
            carryData['productName'] = productName
            productCode = comUtils.getCodeByName(productName)
            carryData['productCode'] = productCode
            carryData['tradingDay'] = today
            carryData['beginCode'] = beginCode
            carryData['endCode'] = endCode

            carryData['turnoverCost'] = fullCarryInfo['turnoverCost']#转抛成本
            carryData['spreadRatio'] = fullCarryInfo['spreadRatio']#价差占比
            carryData['receiptValidityPeriod'] = fullCarryInfo['receiptValidityPeriod']#仓单有效期

            fullCarryMapper.insert_one_to_mongodb(carryData)

# geFullCarryByJYFM("01","03")

# 通过日期，历史交易信息表，查询fullCarry
def geFullCarryHistoryByJYFM(tradingDay,productCode,beginCode,endCode):
    #1、查询交易信息，获取当日，当前品种，套利组合的价差
    dailyCon = {}
    dailyCon['tradingDay'] = tradingDay
    dailyCon['productCode'] = productCode
    dailyCon['mon1'] = beginCode
    dailyCon['mon2'] = endCode
    retDailyTraData = dailyTradingInfo.find_one_from_mongodb(dailyCon)

    #2、查询fullcarry表，获取转抛成本
    carryCon = {}
    carryCon['productCode'] = productCode
    carryCon['beginCode'] = beginCode
    carryCon['endCode'] = endCode
    retCarryData = fullCarryMapper.find_one_from_mongodb(carryCon)

    #3、如果价差 > 转抛成本，则插入新纪录到fullcarry表中
    if (retDailyTraData != None and retCarryData !=None):
        spread = float(retDailyTraData['spread'])
        turnoverCost = retCarryData['turnoverCost']
        if abs(spread) > turnoverCost:
            #计算价差占比
            spreadRatio = (abs(spread)/turnoverCost)*100
            productName = comUtils.getNameByCode(productCode)

            if type(spreadRatio) is float and spreadRatio > 80:
                carryData = {}
                carryData['productName'] = productName
                productCode = comUtils.getCodeByName(productName)
                carryData['productCode'] = productCode
                carryData['tradingDay'] = tradingDay
                carryData['beginCode'] = beginCode
                carryData['endCode'] = endCode

                carryData['turnoverCost'] = retCarryData['turnoverCost']#转抛成本
                carryData['spreadRatio'] = spreadRatio#价差占比
                carryData['receiptValidityPeriod'] = retCarryData['receiptValidityPeriod']#仓单有效期

                fullCarryMapper.insert_one_to_mongodb(carryData)

# tradingDayList = ["2022-12-05","2022-12-06","2022-12-07","2022-12-08","2022-12-09"]
# for tradingDay in tradingDayList:
#     for pro in productNameAndCodeList:
#         productCode = pro['productCode']
#         productName = pro['productName']
#         if productName not in productExcludeList:
#             geFullCarryHistoryByJYFM(tradingDay,productCode,"01","03")

#更新基差趋势到交易信息表
# tradingDayList = ["2022-11-28","2022-11-29","2022-11-30","2022-12-01","2022-12-02"]
# # tradingDayList = ["2022-12-05","2022-12-06","2022-12-07","2022-12-08","2022-12-09"]
# for tradingDay in tradingDayList:
#     for product in productNameAndCodeList:
#         productName = product['productName']
#         if productName not in productExcludeList:
#             con = {}
#             con['tradingDay'] = tradingDay
#             con['productCode'] = product['productCode']
#             ret = dailyTradingInfo.find_one_from_mongodb(con)
#             if ret != None:
#                 #更新到当日fullcarry表中
#                 con = {}
#                 con['tradingDay'] = ret['tradingDay']
#                 con['productCode'] = ret['productCode']
#                 con['beginCode'] = ret['mon1']
#                 con['endCode'] = ret['mon2']
#                 retOld = fullCarryMapper.find_one_from_mongodb(con)
#                 if retOld == None:
#                     geFullCarryHistoryByJYFM(ret['tradingDay'],ret['productCode'],ret['mon1'],ret['mon2'])
#
#                 data = {}
#                 data['basisStru'] = ret['basisStru']
#                 data['futureBasisRatio'] = ret['futureBasisRatio']
#                 fullCarryMapper.update_to_mongodb(con,data)
