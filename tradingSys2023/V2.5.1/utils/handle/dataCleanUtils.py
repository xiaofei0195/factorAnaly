# -*- coding:utf-8 -*-
import os
import sys

import numpy as np

try:
    import talib
except:
    print('请安装TA-Lib库')
    # 安装talib请看文档https://www.myquant.cn/docs/gm3_faq/154?
    sys.exit(-1)

# 把当前文件所在文件夹的父文件夹路径加入到PYTHONPATH
from utils.handle import dateUtils

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mapper.temp import priceStruDailyTempMapper as priceStruTempMp, basisDailyTempMapper as basisDailyTempMp, \
    warehouseDailyTempMapper as warehouseDailyTempMp, spreadDailyTempMapper as spreadDailyTempMp
from mapper.clean import priceStruDailyMapper as priceStruMp, basisDailyMapper as basisDailyMp, \
    warehouseDailyMapper as warehouseDailyMp, spreadDailyMapper as spreadDailyMp, cashPriceMapper as cashPriceMp


def cleanToPriceStruDaily(baseInfo, tradingDay, mon1):
    productCode = baseInfo['productCode']
    productName = baseInfo['productName']

    bean = {}
    bean['tradingDay'] = tradingDay
    bean['productCode'] = baseInfo['productCode']
    result = priceStruTempMp.find_one_from_mongodb(bean)
    try:
        if result != None:
            data = result['data']

            # 转实时计算
            slope = 1
            priceSet = data['todayData']

            # 获取近月价格
            for priceMp in priceSet:
                if str(mon1) in str(priceMp['instrument']):
                    mon1Price = priceMp['closePrice']
                    break

            if len(priceSet) > 3:
                price1 = float(priceSet[0]['closePrice'])
                price2 = float(priceSet[1]['closePrice'])
                price3 = float(priceSet[2]['closePrice'])
                price4 = float(priceSet[3]['closePrice'])
                # print(str(price1) + '/' + str(price2) + '/' + str(price3) + '/' + str(price4))
                if price1 == 0:
                    price1 = price2

                if price1 >= price2 and price2 >= price3 and price3 >= price4:
                    structure = 'BACK'
                elif price1 <= price2 and price2 <= price3 and price3 <= price4:
                    structure = 'CONT'
                else:
                    structure = 'OTHER'

                # 计算曲线倾斜度 （近月-远月）/近月
                slope = float(format((price1 - price3) / price1, '.2f'))

            else:
                structure = 'NG'

            # 先删除，再新增
            delCon = {}
            delCon['tradingDay'] = tradingDay
            delCon['productCode'] = productCode
            priceStruMp.delete_one_from_mongodb(delCon)

            bean = {}
            bean['productName'] = productName
            bean['productCode'] = productCode
            bean['structure'] = structure
            bean['slope'] = slope
            bean['tradingDay'] = tradingDay
            bean['mon1Price'] = mon1Price
            priceStruMp.insert_one_to_mongodb(bean)

            return bean
        else:
            '品种：' + productName + '，getTempPriceStruDaily，异常！'
    except Exception as e:
        errorMsg = '品种：' + productName + '，cleanToPriceStruDaily，异常！' + str(e)
        print(errorMsg)
        return None


##基差分析，数据处理
def cleanToBasisDaily(baseInfo, tradingDay):
    productCode = baseInfo['productCode']
    productName = baseInfo['productName']

    tempCon = {}
    tempCon['productCode'] = baseInfo['productCode']
    result = basisDailyTempMp.find_one_from_mongodb(tempCon)
    try:
        if result != None:
            data = result['data']
            basisValue = data['basisValue']
            category = data['category']

            # 获取待查询的日期下标
            if category != None:
                tradingDayIndex = category.index(tradingDay)

                if tradingDayIndex != None:

                    # 先删除，再新增
                    delCon = {}
                    delCon['tradingDay'] = tradingDay
                    delCon['productCode'] = productCode
                    basisDailyMp.delete_one_from_mongodb(delCon)

                    bean = {}
                    bean['tradingDay'] = tradingDay
                    bean['productName'] = productName
                    bean['productCode'] = productCode

                    # 计算基差结构
                    lengh = len(category)
                    if lengh > 1:
                        lastDay = category[lengh - 1]
                        bean['lastDay'] = lastDay
                    if lengh > 20:
                        basisStru = calBasisStruV2(basisValue, tradingDayIndex)
                        bean['basisStru'] = basisStru
                    basisDailyMp.insert_one_to_mongodb(bean)
                    return bean

    except Exception as e:
        errorMsg = '品种：' + productName + '，cleanToBasisDaily，异常！' + str(e)
        print(errorMsg)
        return None


def calBasisStruV2(basisValue, index):
    # 异常值处理
    basisStru = 'NG'
    basisValueNew = []
    for index, value in enumerate(basisValue):
        if value == 'NaN':
            value = basisValue[index - 1]
        basisValueNew.append(value)

    subListA = basisValueNew[index - 1:index + 1]
    subListB = basisValueNew[0:index + 1]

    avgMA2 = sum(subListA) / len(subListA)
    avgMA6 = sum(subListB) / len(subListB)
    # print('avgMA2:' + str(avgMA2))
    # print('avgMA6:' + str(avgMA6))

    if avgMA2 >= avgMA6:
        basisStru = 'UP'

    if avgMA2 < avgMA6:
        basisStru = 'DOWN'

    return basisStru


# 计算近月基差趋势、现货价格趋势、近月价格趋势
def calMon1BasisStru(productCode, dayRange, recentList):
    cashPriceList = []
    mon1BasisValueList = []
    for index, day in enumerate(dayRange):

        # 查询指定日期的现货价格
        queryCashPriceCon = {}
        queryCashPriceCon['tradingDay'] = day
        queryCashPriceCon['productCode'] = productCode
        retData = cashPriceMp.find_one_from_mongodb(queryCashPriceCon)
        if retData == None:
            print(productCode,day,"查询现货价格 无数据  error!!!")
            return "200","200"

        if retData != None:
            cashPrice = retData['cashPrice']

            # 提取指定日期的近月价格
            mon1Price = recentList[index]
            # 计算近月基差
            mon1BasisValue = float(cashPrice) - float(mon1Price)
            mon1BasisValueList.append(mon1BasisValue)
            # 汇总现货价格集合
            cashPriceList.append(cashPrice)

    # 计算近月基差趋势
    mon1BasisStru = calBasisStruV2(mon1BasisValueList, len(dayRange) - 1)
    # 计算现货价格趋势
    cashPriceStru = calBasisStruV2(cashPriceList, len(dayRange) - 1)

    return mon1BasisStru, cashPriceStru


# 计算月差趋势
# def calSpreadStru(productCode, dayRange, mon1, mon2):
#     spreadList = []
#     for index, day in enumerate(dayRange):
#
#         # 查询指定日期的月差
#         queryspreadCon = {}
#         queryspreadCon['tradingDay'] = day
#         queryspreadCon['productCode'] = productCode
#         queryspreadCon['mon1'] = mon1
#         queryspreadCon['mon2'] = mon2
#         retSpreadData = spreadDailyMp.find_one_from_mongodb(queryspreadCon)
#
#         if retSpreadData != None:
#             spread = retSpreadData['spread']
#             # 汇总月差集合
#             spreadList.append(spread)
#         # 开头历史为空，置为当前月差
#         else:
#             spreadList.append(spread)
#     # 计算月差趋势
#     spreadStru = calBasisStruV2(spreadList, len(dayRange) - 1)
#     return spreadStru


# 查询近月远月价差、指定日期区间的近月价格集合
# 1、初始化前15日数据 updFlag=N
# 2、已初始化清洗数据 updFlag=Y
def cleanToSpreadDaily(baseInfo, tradingDay, mon1, mon2, dayRange, updFlag):
    try:
        productCode = baseInfo['productCode']
        productName = baseInfo['productName']
        basisAnyDays = len(dayRange)

        tempCon = {}
        tempCon['productCode'] = baseInfo['productCode']
        tempCon['tradingDay'] = tradingDay
        tempCon['mon1'] = mon1
        tempCon['mon2'] = mon2
        result = spreadDailyTempMp.find_one_from_mongodb(tempCon)

        dstCon = {}
        dstCon['productCode'] = productCode
        dstCon['productName'] = productName
        dstCon['tradingDay'] = result['tradingDay']
        dstCon['mon1'] = result['mon1']
        dstCon['mon2'] = result['mon2']
        dstResult = spreadDailyMp.find_one_from_mongodb(dstCon)

        if result != None:

            # 获取日期集合
            data = result['data']
            dataCategory = data['dataCategory']
            category = data['category']
            recentPrice = data['recentPrice']

            if (mon1[0] == '0'):
                codeNew = mon1.replace('0', '')
            else:
                codeNew = mon1

            # 通过当前日期，获取指定日期下标
            dayExlYear = tradingDay[-5:]
            dataIndex = dataCategory.index(dayExlYear)

            yearData = 0
            if int(codeNew) <= 12 and int(codeNew) >= 1:
                yearData = data['year2023']
            else:
                print("系统错误，月份error")

            retDic = {}

            # 通过日期下标，获取价差
            try:
                spread = yearData[dataIndex]
            except Exception as e:
                print(productCode,tradingDay,"spread = yearData[dataIndex] line273  error!")

            if spread == 'NaN':
                print(productCode,"系统错误，下标：", dataIndex, "无数据，取下一个数据")
                spread = yearData[dataIndex]
                if spread == 'NaN':
                    print(productCode,"系统错误，下标+1：", dataIndex, "无数据，取下一个数据")
                    spread = yearData[dataIndex + 1]

            retDic['spread'] = spread

            # category获取日期下标
            try:
                index = category.index(tradingDay)
            except Exception as e:
                print(productCode,tradingDay,"index = category.index(tradingDay) line288  error!")

                # print('日期下标从' + tradingDay + '调整为减少1天')
                # dateStrShift = dateUtils.dateStrShift(tradingDay, -1)
                # index = category.index(dateStrShift)

            # recentPrice获取指定日期区间的近月价格集合
            ###############################################################
            # 计算近月价格趋势
            mon1PriceStru = []
            priceList = []
            recentPriceCur = recentPrice[0:index + 1]

            for price in recentPriceCur:
                if price != "NaN":
                    priceList.append(price)
            dataArr = np.array(priceList)
            mon1Ma2 = talib.SMA(dataArr, timeperiod=2)
            mon1Ma5 = talib.SMA(dataArr, timeperiod=5)
            mon1Ma10 = talib.SMA(dataArr, timeperiod=10)
            mon1Ma20 = talib.SMA(dataArr, timeperiod=20)

            # 判断趋势
            if mon1Ma2[-1] >= mon1Ma10[-1]:
                mon1PriceStru = 'UP'
            if mon1Ma2[-1] < mon1Ma10[-1]:
                mon1PriceStru = 'DOWN'
            ###############################################################

            # yearData获取指定日期区间的近月价格集合
            ###############################################################
            # 计算月差趋势
            spreadStru = 'NG'
            yearDataList = []
            yearDataCur = yearData[0:dataIndex]
            for value in yearDataCur:
                if value != "NaN":
                    yearDataList.append(value)
            spreadDataArr = np.array(yearDataList)
            spreadMa2 = talib.SMA(spreadDataArr, timeperiod=2)
            spreadMa5 = talib.SMA(spreadDataArr, timeperiod=5)
            spreadMa10 = talib.SMA(spreadDataArr, timeperiod=10)
            spreadMa20 = talib.SMA(spreadDataArr, timeperiod=20)

            # 判断趋势
            if spreadMa2[-1] >= spreadMa10[-1]:
                spreadStru = 'UP'
            if spreadMa2[-1] < spreadMa10[-1]:
                spreadStru = 'DOWN'
            ###############################################################

            recentList = recentPrice[index - (basisAnyDays - 1):index + 1]
            retDic['dayRange'] = dayRange
            retDic['recentList'] = recentList
            # 计算近月基差集合
            try:
                mon1BasisStru, cashPriceStru = calMon1BasisStru(productCode, dayRange, recentList)
            except Exception as e:
                print(productCode,tradingDay,"calMon1BasisStru line346  error!")


            retDic['mon1BasisStru'] = mon1BasisStru
            retDic['cashPriceStru'] = cashPriceStru
            retDic['mon1PriceStru'] = mon1PriceStru
            retDic['productCode'] = productCode
            retDic['productName'] = productName
            retDic['tradingDay'] = result['tradingDay']
            retDic['mon1'] = result['mon1']
            retDic['mon2'] = result['mon2']

            # 先删除，再插入
            delCon = {}
            delCon['productCode'] = productCode
            delCon['tradingDay'] = result['tradingDay']
            delCon['mon1'] = result['mon1']
            delCon['mon2'] = result['mon2']
            spreadDailyMp.delete_one_from_mongodb(delCon)
            spreadDailyMp.insert_one_to_mongodb(retDic)

            # # 当日未清洗过，直接插入
            # if dstResult == None:
            #     spreadDailyMp.insert_one_to_mongodb(retDic)


            # 已清洗过 则更新月差结构入库
            if updFlag == 'Y':
                updCon = {}
                updCon['productCode'] = productCode
                updCon['tradingDay'] = tradingDay
                updCon['mon1'] = result['mon1']
                updCon['mon2'] = result['mon2']
                updData = {}
                updData['spreadStru'] = spreadStru
                spreadDailyMp.update_to_mongodb(updCon, updData)

    except Exception as e:
        errorMsg = '品种：' + productName + '，cleanToSpreadDaily，total异常！' + str(e)
        print(errorMsg)
        return None

    dataCon = {}
    dataCon['productCode'] = productCode
    dataCon['tradingDay'] = result['tradingDay']
    dataCon['mon1'] = result['mon1']
    dataCon['mon2'] = result['mon2']
    dataResult = spreadDailyMp.find_one_from_mongodb(dataCon)
    return dataResult


##仓单分析，数据处理
def cleanToWarehouseDaily(baseInfo, tradingDay):
    try:
        productCode = baseInfo['productCode']
        productName = baseInfo['productName']

        tempCon = {}
        tempCon['tradingDay'] = tradingDay
        tempCon['productCode'] = baseInfo['productCode']
        result = warehouseDailyTempMp.find_one_from_mongodb(tempCon)

        if result == None:
            return

        data = result['data']
        dataCategory = data['dataCategory']
        category = data['category']
        lenDay = len(category)

        # 获取待查询的日期下标
        if dataCategory != None:
            dayExlYear = tradingDay[-5:]
            tradingDayIndex = dataCategory.index(dayExlYear)
            if tradingDayIndex != None:

                # 获取各年份当日历史仓单数值
                totalYearNum = lenDay / 297
                yearList = []
                if totalYearNum >= 5:
                    yearList = ["year2023", "year2022", "year2021", "year2020", "year2019", "year2018", "year2017"]
                if totalYearNum >= 4 and totalYearNum < 5:
                    yearList = ["year2023", "year2022", "year2021", "year2020", "year2019", "year2018"]
                if totalYearNum >= 3 and totalYearNum < 4:
                    yearList = ["year2023", "year2022", "year2021", "year2020", "year2019"]
                if totalYearNum >= 2 and totalYearNum < 3:
                    yearList = ["year2023", "year2022", "year2021", "year2020"]
                if totalYearNum >= 1 and totalYearNum < 2:
                    yearList = ["year2023", "year2022", "year2021"]
                if totalYearNum >= 0 and totalYearNum < 1:
                    yearList = ["year2023", "year2022"]

                hisValueList = []
                for yearNum in yearList:
                    hisYear = data[yearNum]
                    hisValue = hisYear[tradingDayIndex]
                    if hisValue != "NaN":
                        hisValueList.append(hisValue)

                # 计算仓单历史百分位
                # 目前值11.77举例：历史估值水平=（11.77-历史最小）/（历史最高-历史最小）
                hisValueMin = min(hisValueList)
                hisValueMax = max(hisValueList)
                curValue = hisValueList[0]
                if hisValueMax - hisValueMin == 0:
                    hisPercentile = 0
                if hisValueMax - hisValueMin != 0:
                    hisPercentile = float(
                        format((float((curValue - hisValueMin)) * 100) / float((hisValueMax - hisValueMin)), '.2f'))

                # 计算仓单趋势
                yearValueList = data[yearList[0]]
                yearValueListDis = []
                for value in yearValueList:
                    if value != 'NaN':
                        valueDouble = float(value)
                        yearValueListDis.append(valueDouble)
                dataArr = np.array(yearValueListDis)
                ma2 = talib.SMA(dataArr, timeperiod=2)
                ma5 = talib.SMA(dataArr, timeperiod=5)
                ma10 = talib.SMA(dataArr, timeperiod=10)
                ma20 = talib.SMA(dataArr, timeperiod=20)

                # 打印出来每一个数据
                # print(dataArr)
                # print("ma2:",ma2[-1])
                # print("ma5:",ma5[-1])
                # print("ma10:",ma10[-1])
                # print("ma20:",ma20[-1])

                # 判断趋势
                if ma2[-1] >= ma10[-1]:
                    warehouseStru = 'UP'
                if ma2[-1] < ma10[-1]:
                    warehouseStru = 'DOWN'

                # 先删除，再新增
                delCon = {}
                delCon['tradingDay'] = tradingDay
                delCon['productCode'] = productCode
                warehouseDailyMp.delete_one_from_mongodb(delCon)

                bean = {}
                bean['tradingDay'] = tradingDay
                bean['productName'] = productName
                bean['productCode'] = productCode
                bean['hisPercentile'] = hisPercentile
                bean['curValue'] = yearValueListDis[-1]
                bean['wareMa5'] = ma5[-1]
                bean['wareMa10'] = ma10[-1]
                bean['wareMa20'] = ma20[-1]
                bean['warehouseStru'] = warehouseStru

                warehouseDailyMp.insert_one_to_mongodb(bean)
                return bean


    except Exception as e:
        errorMsg = '品种：' + productName + '，cleanToWarehouseDaily，异常！【可忽略】' + str(e)
        print(errorMsg)
        return None

##测试
# if __name__ == '__main__':
# baseInfo = {"productCode":"SF","productName":"硅铁"}
# # baseInfo = {"productCode":"BU","productName":"沥青"}
# # baseInfo = {"productCode":"EB","productName":"苯乙烯"}
# cleanToWarehouseDaily(baseInfo,"2023-05-31")
