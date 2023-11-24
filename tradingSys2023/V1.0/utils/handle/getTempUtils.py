# -*- coding:utf-8 -*-
import os
import sys

import requests
from bs4 import BeautifulSoup  # 一个解析库，用来解析网页结构

# 把当前文件所在文件夹的父文件夹路径加入到PYTHONPATH
from utils.other import constant, comUtils

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mapper.temp import priceStruDailyTempMapper as priceStruTempMp, basisDailyTempMapper as basisDailyTempMp, \
    warehouseDailyTempMapper as warehouseDailyTempMp, spreadDailyTempMapper as spreadDailyTempMp
from mapper.clean import baseInfoMapper as baseMp, cashPriceMapper as cashPriceMp


# 期限结构表TEMP
def getPriceStruDailyTemp(baseInfo, tradingDay):
    productCode = baseInfo['productCode']
    productName = baseInfo['productName']

    queryCon = {}
    queryCon['tradingDay'] = tradingDay
    queryCon['productCode'] = productCode
    retData = priceStruTempMp.find_one_from_mongodb(queryCon)
    if retData == None:
        url = constant.PriceStruDailyUrl
        try:
            r = requests.post(url, data={'type': productCode, 'day': tradingDay})
        except Exception as e:
            errorMsg = '品种：' + productName + '，查询期限结构（交易法门），接口异常！' + str(e)
            print(errorMsg)
            return None

        retData = r.json()
        data = retData['data']

        bean = {}
        bean['tradingDay'] = tradingDay
        bean['productCode'] = productCode
        bean['productName'] = productName
        bean['data'] = data
        priceStruTempMp.insert_one_to_mongodb(bean)
        return data
    else:
        return retData


# 基差分析表TEMP
def getBasisDailyTemp(baseInfo, tradingDay):
    productCode = baseInfo['productCode']
    productName = baseInfo['productName']

    queryCon = {}
    queryCon['tradingDay'] = tradingDay
    queryCon['productCode'] = productCode
    retData = basisDailyTempMp.find_one_from_mongodb(queryCon)
    if retData == None:
        try:
            r = requests.get(constant.BasisDaily + productCode)
        except Exception as e:
            errorMsg = '品种：' + productName + '，查询主力基差（交易法门），接口异常！' + str(e)
            print(errorMsg)
            return None

        retData = r.json()
        data = retData['data']

        bean = {}
        bean['tradingDay'] = tradingDay
        bean['productCode'] = productCode
        bean['productName'] = productName
        bean['data'] = data
        basisDailyTempMp.insert_one_to_mongodb(bean)
        return data
    else:
        return retData


# 仓单分析TEMP
def getWarehouseDailyTemp(baseInfo, tradingDay):
    productCode = baseInfo['productCode']
    productName = baseInfo['productName']

    queryCon = {'tradingDay': tradingDay, 'productCode': productCode}
    retData = warehouseDailyTempMp.find_one_from_mongodb(queryCon)

    if retData is None:
        data = comUtils.fetch_data_from_api(productCode)
        if data is None:
            errorMsg = f"品种：{productName}，查询仓单日报（交易法门），接口异常！"
            print(errorMsg)
            return None

        delCon = {'tradingDay': tradingDay, 'productCode': productCode}
        bean = {'tradingDay': tradingDay, 'productCode': productCode, 'productName': productName, 'data': data}
        warehouseDailyTempMp.delete_many_from_mongodb(delCon)
        warehouseDailyTempMp.insert_one_to_mongodb(bean)
        return data

    else:
        return retData


##测试
# if __name__ == '__main__':
#     # baseInfo = {"productCode":"BU","productName":"沥青"}
#     # baseInfo = {"productCode":"EB","productName":"苯乙烯"}
#     baseInfo = {"productCode":"SF","productName":"硅铁"}
#     getWarehouseDailyTemp(baseInfo,"2023-05-31")


# 现货价格（交易法门基差日报）指定品种
def getCashPriceByJYFM(tradingDay):
    # queryCon = {}
    # queryCon['tradingDay'] = tradingDay
    # retDataList = cashPriceMp.find_all_from_mongodb(queryCon)

    r = requests.post('https://www.jiaoyifamen.com/tools/api//future-basis/daily', data={'day': tradingDay})
    retData = r.json()

    # 获取现货价格
    data = retData['data']
    table_data = data['table_data']

    productList = []
    for item in table_data:

        productName = item['productName']
        if productName == '低硫燃料油':
            productName = '低燃油'
        if productName == '液化石油气':
            productName = '液化气'

        cashDic = {}
        cashDic['productName'] = productName

        baseCon = {}
        baseCon['productName'] = productName
        retBase = baseMp.find_one_from_mongodb(baseCon)
        cashDic['productCode'] = retBase['productCode']

        cashDic['tradingDay'] = tradingDay
        cashDic['cashPrice'] = item['cashPrice']

        # 先删除后新增
        delCon = {}
        delCon['tradingDay'] = tradingDay
        delCon['productCode'] = retBase['productCode']
        cashPriceMp.delete_many_from_mongodb(delCon)

        cashPriceMp.insert_one_to_mongodb(cashDic)
        productList.append(cashDic)

    return productList


# 现货价格（生意社期货现表）指定品种
def getCashPriceBySYS(tradingDay, selectList):
    r = requests.get('http://www.100ppi.com/sf2/day-' + tradingDay + '.html')
    retData = r.text

    bs = BeautifulSoup(retData, "html.parser")  # 解析网页
    table = bs.find_all("tr")  # 定位信息

    productList = []  # [{品种名称，现货价格},{}]结果集
    for i in range(len(table)):
        productDic = {}  # {品种名称，现货价格}

        trStr = table[i].text.strip()
        if len(trStr) > 50 and not trStr.startswith('http'):
            trList = trStr.split('\n')
            trNewList = []
            for td in trList:
                tdNew = td.replace('\xa0', '')
                trNewList.append(tdNew)
            productName = trNewList[0]
            cashPrice = trNewList[1]

            if productName == '涤纶短纤':
                productName = '短纤'
            if productName == '低硫燃料油':
                productName = '低燃油'
            if productName == '液化石油气':
                productName = '液化气'
            if productName == '石油沥青':
                productName = '沥青'
            if productName == '燃料油':
                productName = '燃油'
            if productName == '天然橡胶':
                productName = '橡胶'
            if productName == '甲醇MA':
                productName = '甲醇'
            if productName == '聚氯乙烯':
                productName = 'PVC'
            if productName == '聚乙烯':
                productName = '塑料'

            if productName == '铜':
                productName = '沪铜'
            if productName == '锌':
                productName = '沪锌'
            if productName == '铜':
                productName = '沪铜'
            if productName == '铝':
                productName = '沪铝'
            if productName == '铅':
                productName = '沪铅'
            if productName == '镍':
                productName = '沪镍'
            if productName == '白银':
                productName = '沪银'
            if productName == '锡':
                productName = '沪锡'

            if productName == '棕榈油':
                productName = '棕榈'
            if productName == '玉米淀粉':
                productName = '淀粉'
            if productName == '菜籽粕':
                productName = '菜粕'
            if productName == '菜籽油OI':
                productName = '菜油'

            if productName == '螺纹钢':
                productName = '螺纹'
            if productName == '热轧卷板':
                productName = '热卷'

            if productName in selectList:
                productDic['productName'] = productName
                if productName == '鸡蛋':
                    cashPrice = float(cashPrice) * 500
                if productName == '玻璃':
                    cashPrice = float(cashPrice) * 79.98
                productDic['cashPrice'] = float(cashPrice)

                baseCon = {}
                baseCon['productName'] = productName
                retBase = baseMp.find_one_from_mongodb(baseCon)
                productDic['productCode'] = retBase['productCode']

                productDic['tradingDay'] = tradingDay

                # 先删除后新增
                delCon = {}
                delCon['tradingDay'] = tradingDay
                delCon['productCode'] = retBase['productCode']
                cashPriceMp.delete_many_from_mongodb(delCon)

                cashPriceMp.insert_one_to_mongodb(productDic)
                productList.append(productDic)
    return productList


# 查询近月远月价差TEMP
def getSpreadDailyTemp(baseInfo, tradingDay, mon1, mon2):
    productCode = baseInfo['productCode']
    productName = baseInfo['productName']

    queryCon = {}
    queryCon['tradingDay'] = tradingDay
    queryCon['productCode'] = productCode
    queryCon['mon1'] = mon1
    queryCon['mon2'] = mon2
    retData = spreadDailyTempMp.find_one_from_mongodb(queryCon)
    # if retData == None:
    if True:
        url = 'https://www.jiaoyifamen.com/tools/api//future/spread/free'
        try:
            r = requests.post(url, data={'type1': productCode, 'code1': mon1, 'type2': productCode, 'code2': mon2})
        except Exception as e:
            errorMsg = '品种：' + productName + '，查询近月远月价差（交易法门），接口异常！' + str(e)
            print(errorMsg)
            return None

        retData = r.json()
        data = retData['data']

        bean = {}
        bean['tradingDay'] = tradingDay
        bean['productCode'] = productCode
        bean['productName'] = productName
        bean['mon1'] = mon1
        bean['mon2'] = mon2
        spreadDailyTempMp.delete_one_from_mongodb(bean)

        bean['data'] = data
        spreadDailyTempMp.insert_one_to_mongodb(bean)
        return data
    else:
        return retData


def getCashPrice(tradingDay):
    # 所有品种---【现货价格】
    queryCon = {}
    queryCon['tradingDay'] = tradingDay
    retDataList = cashPriceMp.find_all_from_mongodb(queryCon)

    if len(retDataList) == 48:
        print(tradingDay + ' NUM = 48 is Existed')
        return retDataList
    else:
        print(tradingDay + ' NUM = ' + str(len(retDataList)) + ' cashPrice retry')

    try:
        # print('获取交易法门品种---现货价格')
        productListJYFM = getCashPriceByJYFM(tradingDay)
        if len(productListJYFM) == 0 or productListJYFM == None:
            productListJYFM = []

        # print('获取生意社指定品种---现货价格')
        selectList = ['聚丙烯', '乙二醇', '短纤', '塑料', '豆一', '菜粕', '油菜籽', '鸡蛋', '生猪', '棉纱', '菜油', '棉花', '白糖']
        productListSYS = getCashPriceBySYS(tradingDay, selectList)
        if productListSYS == None:
            productListSYS = []

        if len(productListJYFM) == 0:
            print('日期：' + tradingDay + '，getCashPrice from JYFM，异常！')

        if len(productListSYS) == 0:
            print('日期：' + tradingDay + '，getCashPrice from SYS，异常！')

        productList = productListJYFM + productListSYS
        return productList

    except Exception as e:
        errorMsg = '日期：' + tradingDay + '，getCashPrice，异常！' + str(e)
        print(errorMsg)
        return None
