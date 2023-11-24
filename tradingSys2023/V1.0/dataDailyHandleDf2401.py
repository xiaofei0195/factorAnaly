# -*- coding:utf-8 -*-
import os
import sys
import time
from multiprocessing.dummy import Pool as Pool

from utils.handle import dataCleanUtilsDf, getTempUtilsDf, dataCalUtilsDf, dateUtils

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mapper.clean import baseInfoMapper as baseMp

maxPool = 40


def m1(baseInfo, tradingDay, errorList, mon1, mon2):
    priceTemp = getTempUtilsDf.getPriceStruDailyTemp(baseInfo, tradingDay)
    if priceTemp == None:
        errorList.append(baseInfo['productCode'])
        return

    basisTemp = getTempUtilsDf.getBasisDailyTemp(baseInfo, tradingDay)
    if basisTemp == None:
        errorList.append(['productCode'])
        return

    spreadTemp = getTempUtilsDf.getSpreadDailyTemp(baseInfo, tradingDay, mon1, mon2)
    if spreadTemp == None:
        errorList.append(['productCode'])
        return

    warehouseTemp = getTempUtilsDf.getWarehouseDailyTemp(baseInfo, tradingDay)
    if warehouseTemp == None:
        errorList.append(['productCode'])
        return


def m2(baseInfo, tradingDay, errorList, mon1, mon2):
    if baseInfo['productCode'] in errorList:
        print('品种' + baseInfo['productCode'] + '异常，跳过清洗')
        return

    tradingInfo = {
        'productName': baseInfo['productName'],
        'productCode': baseInfo['productCode'],
        'tradingDay': tradingDay
    }

    # 【期限结构structure、结构斜率slope、近月价格mon1Price】
    retPriceStruMp = dataCleanUtilsDf.cleanToPriceStruDaily(baseInfo, tradingDay, mon1)
    if retPriceStruMp is None:
        return
    tradingInfo.update({
        'structure': retPriceStruMp['structure'],
        'slope': retPriceStruMp['slope'],
        'mon1Price': retPriceStruMp['mon1Price']
    })

    retSpreadMp = dataCleanUtilsDf.cleanToSpreadDaily(baseInfo, tradingDay, mon1, mon2, 'Y')
    if retSpreadMp is None:
        return
    tradingInfo.update({
        'mon1BasisStru': retSpreadMp['mon1BasisStru'],
        'cashPriceStru': retSpreadMp['cashPriceStru'],
        'mon1PriceStru': retSpreadMp['mon1PriceStru'],
        'spread': retSpreadMp['spread'],
        'spreadStru': retSpreadMp['spreadStru'],
        'mon1': retSpreadMp['mon1'],
        'mon2': retSpreadMp['mon2']
    })

    # 【仓单数据warehouse、指定日期区间的近月价格集合】
    retWarehouseMp = dataCleanUtilsDf.cleanToWarehouseDaily(baseInfo, tradingDay)
    if retWarehouseMp is not None:
        tradingInfo.update({
            'hisPercentile': retWarehouseMp['hisPercentile'],
            'warehouseStru': retWarehouseMp['warehouseStru']
        })
    else:
        tradingInfo.update({
            'hisPercentile': 100,
            'warehouseStru': 'UP'
        })

    # 汇总到每日交易信息
    print("m2汇总到每日交易信息:", tradingInfo['productCode'])
    dataCalUtilsDf.dailyTradingIinfoSave(tradingInfo)


'''
爬虫
'''


def run():
    tradingDayList = dateUtils.getTradingDayList('2023-11-22', 0)
    print('tradingDayList:', tradingDayList)

    mon1 = '01'
    mon2 = '03'
    shiftDays = 20
    # 从基本信息库中查询所有品种编码和名称
    retBaseList = baseMp.find_all_from_mongodb({})

    # 移除不活跃品种
    retBaseList = [base for base in retBaseList if base['productCode'] not in ['WH', 'RS', 'FB', 'WR', 'CY']]

    for tradingDay in tradingDayList:

        print('*****************************************************************')
        print('日期：', tradingDay)

        ######################################################################################################
        # 获取当日前15天的现货数据
        # 不存在，则重新查询
        # 存在，则跳过
        print(time.strftime('%H:%M:%S', time.localtime(time.time())), '获取【' + tradingDay + '】前15天现货数据')

        dayRange = dateUtils.getTradingDayList(tradingDay, shiftDays)

        if '2023-10-07' in dayRange or '2023-10-08' in dayRange:
            dayRange.remove('2023-10-07')
            dayRange.remove('2023-10-08')

        for day in dayRange:
            getTempUtilsDf.getCashPrice(day)

        ######################################################################################################
        # 数据暂存到TEMP表
        ######################################################################################################
        errorList = []
        print(time.strftime('%H:%M:%S', time.localtime(time.time())), '数据暂存到TEMP表')

        # # 开启多线程
        # 创建线程池
        pool1 = Pool(processes=maxPool)
        pool2 = Pool(processes=maxPool)

        for baseInfo in retBaseList:
            pool1.apply_async(m1, (baseInfo, tradingDay, errorList, mon1, mon2))

        pool1.close()
        pool1.join()

        ######################################################################################################
        # 数据清洗到目标表
        print(time.strftime('%H:%M:%S', time.localtime(time.time())), '数据清洗到目标表')

        for baseInfo in retBaseList:
            pool2.apply_async(m2, (baseInfo, tradingDay, errorList, mon1, mon2))

        pool2.close()
        pool2.join()

        #################
        # 交易池落库
        dataCalUtilsDf.dailyPoolSave(tradingDay, mon1, mon2)
        print(time.strftime('%H:%M:%S', time.localtime(time.time())), '落库END')

        #################
        # 邮件推送
        dataCalUtilsDf.sendEmail(mon1, mon2, tradingDay)


if __name__ == '__main__':
    run()
