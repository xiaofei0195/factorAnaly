# -*- coding:utf-8 -*-
import calendar
import datetime
import os
import sys
# 把当前文件所在文件夹的父文件夹路径加入到PYTHONPATH
import time

import chinese_calendar
import schedule

from utils.handle import dataCleanUtils, getTempUtilsDf, dataCalUtils, dateUtils

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mapper.clean import baseInfoMapper as baseMp
from multiprocessing.dummy import Pool as Pool

maxPool = 60


def m1(baseInfo, tradingDay, errorList, mon1, mon2):
    # print('当前品种：' + baseInfo['productName'])
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


def m2(baseInfo, tradingDay, errorList, mon1, mon2, dayRange):
    if baseInfo['productCode'] in errorList:
        print('品种' + baseInfo['productCode'] + '异常，跳过清洗')
        return

    tradingInfo = {}
    tradingInfo['productName'] = baseInfo['productName']
    tradingInfo['productCode'] = baseInfo['productCode']
    tradingInfo['tradingDay'] = tradingDay

    # 【期限结构structure、结构斜率slope、近月价格mon1Price】
    retPriceStruMp = dataCleanUtils.cleanToPriceStruDaily(baseInfo, tradingDay, mon1)
    if retPriceStruMp != None:
        tradingInfo['structure'] = retPriceStruMp['structure']  # 重点指标1---期限结构
        tradingInfo['slope'] = retPriceStruMp['slope']
        tradingInfo['mon1Price'] = retPriceStruMp['mon1Price']
    else:
        return

    # 【近月基差趋势mon1BasisStru、现货趋势cashPriceStru、近月价格趋势mon1PriceStru、月差趋势spreadStru】
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
        return

    # 【仓单数据warehouse、指定日期区间的近月价格集合】
    retWarehouseMp = dataCleanUtils.cleanToWarehouseDaily(baseInfo, tradingDay)
    if retWarehouseMp != None:
        tradingInfo['hisPercentile'] = retWarehouseMp['hisPercentile']
        tradingInfo['warehouseStru'] = retWarehouseMp['warehouseStru']
    else:
        tradingInfo['hisPercentile'] = 100
        tradingInfo['warehouseStru'] = 'UP'

    # 汇总到每日交易信息
    dataCalUtils.dailyTradingIinfoSave(tradingInfo)


def job():
    tradingDayList = []
    curTradingDay = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    tradingDayList.append(curTradingDay)

    print('tradingDayList:', tradingDayList)

    # 获取工作日
    today = datetime.datetime.today()
    year = today.year
    month = today.month
    max_day = calendar.monthrange(year, month)[1]
    work_days = chinese_calendar.get_workdays(datetime.datetime(year, month, 1),
                                              datetime.datetime(year, month, max_day))

    mon1 = '01'
    mon2 = '03'
    shiftDays = 10
    # 从基本信息库中，查询所有品种编码和名称
    baseCon = {}
    # baseCon['productCode'] = 'EB'
    retBaseList = baseMp.find_all_from_mongodb(baseCon)
    # 移除不活跃品种
    for base in retBaseList:
        productCode = base['productCode']
        if productCode in ['WH', 'RS', 'FB', 'WH', 'WR', 'CY', 'AG', 'AU', 'SC', 'B']:
            retBaseList.remove(base)

    for tradingDay in tradingDayList:
        # 当日为非交易日，跳过
        tradingDayDate = datetime.date(*map(int, tradingDay.split('-')))
        if tradingDayDate not in work_days:
            print(tradingDay + "为非交易日，跳过")
            continue

        print('*****************************************************************')
        print('日期：', tradingDay)

        ######################################################################################################
        # 获取当日前15天的现货数据
        # 不存在，则重新查询
        # 存在，则跳过
        print(time.strftime('%H:%M:%S', time.localtime(time.time())), '获取【' + tradingDay + '】前15天现货数据')

        dayRange = dateUtils.getTradingDayList(tradingDay, shiftDays)

        if '2023-10-07' in dayRange:
            dayRange.remove('2023-10-07')

        if '2023-10-08' in dayRange:
            dayRange.remove('2023-10-08')

        for day in dayRange:
            retCashList = getTempUtilsDf.getCashPrice(day)
            if retCashList == None:
                continue

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
            pool2.apply_async(m2, (baseInfo, tradingDay, errorList, mon1, mon2, dayRange))

        pool2.close()
        pool2.join()

        #################
        # 交易池落库
        dataCalUtils.dailyPoolSave(tradingDay, mon1, mon2)
        print(time.strftime('%H:%M:%S', time.localtime(time.time())), '落库END')

        #################
        # 邮件推送
        dataCalUtils.sendEmail(mon1, mon2, tradingDay)

        #################
        # 邮件推送
        #################
        dataCalUtils.sendEmail(mon1, mon2, tradingDay)


if __name__ == '__main__':

    setTime = "20:00"
    schedule.every().day.at(setTime).do(job)

    while True:
        print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())) + " schedule.run_pending:", setTime)
        schedule.run_pending()
        print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())) + " time.sleep:")
        time.sleep(1 * 60 * 10)
