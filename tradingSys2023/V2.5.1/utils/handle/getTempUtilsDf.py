# -*- coding:utf-8 -*-
import os
import sys

import pandas as pd
import requests
from bs4 import BeautifulSoup  # 一个解析库，用来解析网页结构

# 把当前文件所在文件夹的父文件夹路径加入到PYTHONPATH
from utils.other import constant, comUtils

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mapper.temp import priceStruDailyTempMapper as priceStruTempMp, basisDailyTempMapper as basisDailyTempMp, \
    warehouseDailyTempMapper as warehouseDailyTempMp, spreadDailyTempMapper as spreadDailyTempMp
from mapper.clean import baseInfoMapper as baseMp, cashPriceMapper as cashPriceMp

'''
#################################################
# 期限结构TEMP
#################################################
'''


def getPriceStruDailyTemp(baseInfo, tradingDay):
    productCode = baseInfo['productCode']
    productName = baseInfo['productName']

    queryCon = {'tradingDay': tradingDay, 'productCode': productCode}
    retData = priceStruTempMp.find_one_from_mongodb(queryCon)

    if retData == None:
        data = comUtils.fetch_data_from_api(constant.PriceStruDailyUrl, productCode)
        if data is None:
            errorMsg = "品种：{productName}，查询期限结构（交易法门），接口异常！"
            print(errorMsg)
            return None

        bean = {'tradingDay': tradingDay, 'productCode': productCode, 'productName': productName, 'data': data}
        priceStruTempMp.insert_one_to_mongodb(bean)
        return data
    else:
        return retData


'''
#################################################
# 基差日报TEMP
#################################################
'''


def getBasisDailyTemp(baseInfo, tradingDay):
    productCode = baseInfo['productCode']
    productName = baseInfo['productName']

    queryCon = {'tradingDay': tradingDay, 'productCode': productCode}
    retData = basisDailyTempMp.find_one_from_mongodb(queryCon)

    if retData == None:
        data = comUtils.fetch_data_from_api(constant.BasisDailyUrl, productCode)
        if data is None:
            errorMsg = "品种：{productName}，查询主力基差（交易法门），接口异常！"
            print(errorMsg)
            return None

        bean = {'tradingDay': tradingDay, 'productCode': productCode, 'productName': productName, 'data': data}
        basisDailyTempMp.insert_one_to_mongodb(bean)
        return data
    else:
        return retData


'''
#################################################
# 仓单分析TEMP
#################################################
'''


def getWarehouseDailyTemp(baseInfo, tradingDay):
    productCode = baseInfo['productCode']
    productName = baseInfo['productName']

    queryCon = {'tradingDay': tradingDay, 'productCode': productCode}
    retData = warehouseDailyTempMp.find_one_from_mongodb(queryCon)

    if retData == None:
        data = comUtils.fetch_data_from_api(constant.WarehouseDailyUrl, productCode)
        if data is None:
            errorMsg = "品种：{productName}，查询仓单日报（交易法门），接口异常！"
            print(errorMsg)
            return None

        delCon = {'tradingDay': tradingDay, 'productCode': productCode}
        bean = {'tradingDay': tradingDay, 'productCode': productCode, 'productName': productName, 'data': data}
        warehouseDailyTempMp.delete_many_from_mongodb(delCon)
        warehouseDailyTempMp.insert_one_to_mongodb(bean)
        return data
    else:
        return retData


'''
#################################################
# 现货价格（交易法门基差日报）指定品种
#################################################
'''


def getCashPriceByJYFM(tradingDay):
    # 从基差日报中，查询现货
    param = {'day': tradingDay}
    data = comUtils.fetch_data_from_api_post(constant.CashPriceByJYFMUrl, param)

    baseCon = {}
    baseInfoList = baseMp.find_all_from_mongodb(baseCon)
    baseInfoDf = pd.DataFrame(baseInfoList)

    # 获取现货价格
    if data is not None:
        table_data = data['table_data']
        tableDateDf = pd.DataFrame(table_data)
        if len(tableDateDf) <= 0:
            return
        tableDateDf['productName'].replace('低硫燃料油', '低燃油', inplace=True)
        tableDateDf['productName'].replace('液化石油气', '液化气', inplace=True)
        tableDateDf['productName'].replace('PP', '聚丙烯', inplace=True)

        # 基本信息和现货价格信息合并
        data_raw_df = pd.merge(tableDateDf, baseInfoDf, on=['productName'], how='left')
        data_raw_df['tradingDay'] = tradingDay
        data_df = data_raw_df.loc[:, ["tradingDay", "productCode", "productName", "cashPrice"]]

        # 先删除后新增
        delCon = {'tradingDay': tradingDay}
        cashPriceMp.delete_many_from_mongodb(delCon)

        data_dict = data_df.to_dict("records")
        cashPriceMp.insert_many_to_mongodb(data_dict)

        return data_dict


# 现货价格（生意社期货现表）指定品种
def getCashPriceBySYS(tradingDay, selectList):
    url = f"http://www.100ppi.com/sf2/day-{tradingDay}.html"
    r = requests.get(url)
    retData = r.text
    bs = BeautifulSoup(retData, "html.parser")
    table = bs.find_all("tr")

    # [{品种名称，现货价格},{}]结果集
    productList = []
    for row in table:
        trStr = row.text.strip()

        if len(trStr) > 50 and not trStr.startswith('http'):
            trList = [td.replace('\xa0', '') for td in trStr.split("\n")]
            productName = trList[0]
            cashPrice = trList[1]
            productName = constant.product_mapping.get(productName, productName)

            if productName in selectList:
                productDic = {
                    'productName': productName,
                    'cashPrice': float(cashPrice),
                    'tradingDay': tradingDay
                }
                if productName == '鸡蛋':
                    productDic['cashPrice'] *= 500
                if productName == '玻璃':
                    productDic['cashPrice'] *= 79.98

                baseCon = {'productName': productName}
                retBase = baseMp.find_one_from_mongodb(baseCon)
                productDic['productCode'] = retBase['productCode']
                productList.append(productDic)

                if productList:
                    delCon = {'tradingDay': tradingDay, 'productCode': {'$in': [p['productCode'] for p in productList]}}
                cashPriceMp.delete_many_from_mongodb(delCon)
                cashPriceMp.insert_many_to_mongodb(productList)

    return productList


'''
#################################################
# 跨期价差TEMP
#################################################
'''


def getSpreadDailyTemp(baseInfo, tradingDay, mon1, mon2):
    productCode = baseInfo['productCode']
    productName = baseInfo['productName']

    queryCon = {'tradingDay': tradingDay, 'productCode': productCode, 'mon1': mon1, 'mon2': mon2}
    retData = spreadDailyTempMp.find_one_from_mongodb(queryCon)

    if retData == None:

        try:
            param = {'type1': productCode, 'code1': mon1, 'type2': productCode, 'code2': mon2}
            data = comUtils.fetch_data_from_api_post(constant.SpreadDailyUrl, param)
        except Exception as e:
            errorMsg = '品种：' + productName + '，查询近月远月价差（交易法门），接口异常！' + str(e)
            print(errorMsg)
            return None

        bean={'tradingDay': tradingDay, 'productCode': productCode, 'productName': productName, 'mon1': mon1, 'mon2': mon2}
        spreadDailyTempMp.delete_one_from_mongodb(bean)

        bean['data'] = data
        spreadDailyTempMp.insert_one_to_mongodb(bean)
        return data
    else:
        return retData


def getCashPrice(tradingDay):
    queryCon = {'tradingDay': tradingDay}
    retDataList = cashPriceMp.find_all_from_mongodb(queryCon)

    if len(retDataList) >= 53:
        # print(tradingDay + ' NUM = 55 is Existed')
        return retDataList
    else:
        print(tradingDay + ' NUM = ' + str(len(retDataList)) + ' cashPrice retry')

    try:
        productListJYFM = getCashPriceByJYFM(tradingDay)
        if not productListJYFM:
            productListJYFM = []

        productListSYS = getCashPriceBySYS(tradingDay, constant.selectList)
        if not productListSYS:
            productListSYS = []

        if not productListJYFM:
            print('日期：' + tradingDay + '，getCashPrice from JYFM，异常！')
        if not productListSYS:
            print('日期：' + tradingDay + '，getCashPrice from SYS，异常！')

        productList = productListJYFM + productListSYS

        if len(productList) < 55:
            return None

        return productList

    except Exception as e:
        errorMsg = '日期：' + tradingDay + '，getCashPrice，异常！' + str(e)
        print(errorMsg)
        return None
