# -*- coding:utf-8 -*-
import sys
import os
# 把当前文件所在文件夹的父文件夹路径加入到PYTHONPATH
from utils.handle import dataCleanUtils, getTempUtils, dataCalUtils, dateUtils

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mapper.clean import baseInfoMapper as baseMp
from mapper import tradingInfoDailyMapper as tradingInfoDailyMp


def run():
    tradingDayList = dateUtils.getTradingDayList('2023-11-13', 0)

    if '2023-04-23' in tradingDayList:
        tradingDayList.remove()
    print('tradingDayList:', tradingDayList)

    mon1 = '12'
    mon2 = '02'

    for tradingDay in tradingDayList:
        #################
        # 规则：
        # 1、先通过productCode mon1 mon2查询当前品种 是否在交易池中，
        # 2、不存在，并且满足交易条件，则直接插入
        # 【基差率>3%,盈亏比>1】
        # 3、存在，跟踪近月基差率
        # 【基差率>0%,盈亏比>0】则直接插入
        # 否则，则软删除
        dataCalUtils.dailyPoolSave(tradingDay, mon1, mon2)
        print('交易池落库END' + '\n')

        #################
        # 邮件推送
        #################
        dataCalUtils.sendEmail(mon1, mon2, tradingDay)


if __name__ == '__main__':
    run()

