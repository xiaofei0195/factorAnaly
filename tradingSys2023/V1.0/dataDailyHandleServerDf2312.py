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
import dataDailyHandleDf2312 as m

maxPool = 60

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

    mon1 = '12'
    mon2 = '02'
    shiftDays = 10
    # 从基本信息库中，查询所有品种编码和名称
    baseCon = {}
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
            pool1.apply_async(m.m1, (baseInfo, tradingDay, errorList, mon1, mon2))

        pool1.close()
        pool1.join()

        ######################################################################################################
        # 数据清洗到目标表
        print(time.strftime('%H:%M:%S', time.localtime(time.time())), '数据清洗到目标表')

        for baseInfo in retBaseList:
            pool2.apply_async(m.m2, (baseInfo, tradingDay, errorList, mon1, mon2, dayRange))

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
