# -*- coding:utf-8 -*-
import datetime
import sys
import os
# 把当前文件所在文件夹的父文件夹路径加入到PYTHONPATH
from datetime import time
import time as t


from utils.handle import dataCleanUtils, getTempUtils, dataCalUtils, dateUtils

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mapper.clean import baseInfoMapper as baseMp
from mapper import tradingInfoDailyMapper as tradingInfoDailyMp


def run():
    tradingDayList = []
    # tradingDayList = dateUtils.getTradingDayList('2023-08-07', 30)
    tradingDayList = dateUtils.getTradingDayList('2023-09-18', 5)

    # if '2023-06-25' in tradingDayList:
    #     tradingDayList.remove()
    print('tradingDayList:', tradingDayList)

    mon1 = '10'
    mon2 = '12'
    shiftDays = 10
    # 从基本信息库中，查询所有品种编码和名称
    baseCon = {}
    # baseCon['productCode'] = 'CF'
    retBaseList = baseMp.find_all_from_mongodb(baseCon)

    for tradingDay in tradingDayList:

        print('*****************************************************************')
        print('日期：', tradingDay)


        ######################################################################################################
        # 获取当日前15天的现货数据
        ######################################################################################################
        # 不存在，则重新查询
        # 存在，则跳过
        print('获取【' + tradingDay + '】前15天现货数据')
        print(t.strftime('%Y-%m-%d %H:%M:%S',t.localtime(t.time())))

        dayRange = dateUtils.getTradingDayList(tradingDay, shiftDays)
        if '2023-06-25' in dayRange:
            dayRange.remove('2023-06-25')

        print('dayRange:', dayRange)
        for day in dayRange:
            retCashList = getTempUtils.getCashPrice(day)
            if retCashList == None:
                continue
            # print(day," retCashList length: ",len(retCashList))

        ######################################################################################################
        # 数据暂存到TEMP表
        ######################################################################################################
        print('数据暂存到TEMP表')
        print(t.strftime('%Y-%m-%d %H:%M:%S',t.localtime(t.time())))

        errorList = []
        for baseInfo in retBaseList:
            # print('当前品种：' + baseInfo['productName'])
            # print('getPriceStruDailyTemp 中')
            priceTemp = getTempUtils.getPriceStruDailyTemp(baseInfo, tradingDay)
            if priceTemp == None:
                errorList.append(baseInfo['productCode'])
                continue

            # print('getBasisDailyTemp 中')
            basisTemp = getTempUtils.getBasisDailyTemp(baseInfo, tradingDay)
            if basisTemp == None:
                errorList.append(['productCode'])
                continue

            # print('getSpreadDailyTemp 中')
            spreadTemp = getTempUtils.getSpreadDailyTemp(baseInfo, tradingDay, mon1, mon2)
            if spreadTemp == None:
                errorList.append(['productCode'])
                continue

            # print('getWarehouseDailyTemp 中')
            warehouseTemp = getTempUtils.getWarehouseDailyTemp(baseInfo, tradingDay)
            if warehouseTemp == None:
                errorList.append(['productCode'])
                continue

        ######################################################################################################
        # 数据清洗到目标表
        ######################################################################################################
        print('数据清洗到目标表')
        print(t.strftime('%Y-%m-%d %H:%M:%S',t.localtime(t.time())))

        for baseInfo in retBaseList:
            if baseInfo['productCode'] in errorList:
                print('品种' + baseInfo['productCode'] + '异常，跳过清洗')
                continue

            # print('当前品种：' + baseInfo['productName'])

            tradingInfo = {}
            tradingInfo['productName'] = baseInfo['productName']
            tradingInfo['productCode'] = baseInfo['productCode']
            tradingInfo['tradingDay'] = tradingDay

            # 【期限结构structure、结构斜率slope、近月价格mon1Price】
            # print('cleanToPriceStruDaily 中')
            retPriceStruMp = dataCleanUtils.cleanToPriceStruDaily(baseInfo, tradingDay, mon1)
            if retPriceStruMp != None:
                tradingInfo['structure'] = retPriceStruMp['structure']  # 重点指标1---期限结构
                tradingInfo['slope'] = retPriceStruMp['slope']
                tradingInfo['mon1Price'] = retPriceStruMp['mon1Price']
            else:
                continue

            # 【近月基差趋势mon1BasisStru、现货趋势cashPriceStru、近月价格趋势mon1PriceStru、月差趋势spreadStru】
            # print('cleanToSpreadDaily 中')
            retSpreadMp = dataCleanUtils.cleanToSpreadDaily(baseInfo, tradingDay, mon1, mon2, dayRange, 'Y')
            if retSpreadMp != None:
                tradingInfo['mon1BasisStru'] = retSpreadMp['mon1BasisStru']
                tradingInfo['cashPriceStru'] = retSpreadMp['cashPriceStru']
                tradingInfo['mon1PriceStru'] = retSpreadMp['mon1PriceStru']
                tradingInfo['spread'] = retSpreadMp['spread']
                tradingInfo['spreadStru'] = retSpreadMp['spreadStru']
                tradingInfo['dayRange'] = dayRange
                tradingInfo['recentList'] = retSpreadMp['recentList']
                tradingInfo['mon1'] = retSpreadMp['mon1']
                tradingInfo['mon2'] = retSpreadMp['mon2']
            else:
                continue

            # 【仓单数据warehouse、指定日期区间的近月价格集合】
            # print('cleanToWarehouseDaily 中')
            retWarehouseMp = dataCleanUtils.cleanToWarehouseDaily(baseInfo, tradingDay)
            if retWarehouseMp != None:
                tradingInfo['hisPercentile'] = retWarehouseMp['hisPercentile']
                tradingInfo['warehouseStru'] = retWarehouseMp['warehouseStru']
            else:
                continue

            # 汇总到每日交易信息
            # print('dailyTradingIinfoSave 中')
            dataCalUtils.dailyTradingIinfoSave(tradingInfo)

        #################
        # 交易池落库
        # 策略1：
        # 近月基差率>3%  盈亏比（近月基差/近远价差）>1
        #################
        # 规则：
        # 1、先通过productCode mon1 mon2查询当前品种 是否在交易池中，
        # 2、不存在，并且满足交易条件，则直接插入
        # 3、存在，跟踪近月基差率
        # 满足交易则直接插入
        # 否则，则软删除
        dataCalUtils.dailyPoolSave(tradingDay, mon1, mon2)
        print('交易池落库END' + '\n')

        #################
        # 邮件推送
        #################
        # dataCalUtils.sendEmail(mon1, mon2, tradingDay)


if __name__ == '__main__':
    run()

