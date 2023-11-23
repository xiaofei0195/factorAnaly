# -*- coding:utf-8 -*-
import os
import sys
import pandas as pd
from datetime import datetime

# 把当前文件所在文件夹的父文件夹路径加入到PYTHONPATH
from utils.other import fileUtils, emailUtils

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mapper.clean import cashPriceMapper as cashPriceMp
from mapper import tradingInfoDailyMapper as tradingInfoMp, tradingPoolDailyMapper as tradingPoolMp, \
    emailRecordDailyMapper as emailRecordMp


def dailyTradingIinfoSave(resultInfo):
    # 查询现货价格
    cashCon = {'productCode': resultInfo['productCode'], 'tradingDay': resultInfo['tradingDay']}
    cashPriceData = cashPriceMp.find_one_from_mongodb(cashCon)
    if cashPriceData is None:
        print(resultInfo['productCode'] + resultInfo['tradingDay'] + '查询现货价格异常')
        return

    cashPriceFloat = float(cashPriceData['cashPrice'])
    mon1PriceFloat = float(resultInfo['mon1Price'])
    spreadFloat = float(resultInfo['spread'])

    # 近月基差
    futureBasisFloat = cashPriceFloat - mon1PriceFloat
    # 计算近月基差率
    futureBasisRatioFloat = float(format((futureBasisFloat * 100) / cashPriceFloat, '.2f'))
    # 计算盈亏比
    ratioFloat = float(format(futureBasisFloat / spreadFloat, '.2f'))

    resultInfo['futureBasis'] = futureBasisFloat
    resultInfo['futureBasisRatio'] = futureBasisRatioFloat
    resultInfo['ratio'] = ratioFloat

    # 先删除，再新增
    delCon = {}
    delCon['tradingDay'] = resultInfo['tradingDay']
    delCon['productCode'] = resultInfo['productCode']
    delCon['mon1'] = resultInfo['mon1']
    delCon['mon2'] = resultInfo['mon2']
    tradingInfoMp.delete_one_from_mongodb(delCon)

    # 胜率计算
    # 基差率权重
    futureBasisRatioValue = resultInfo['futureBasisRatio']
    conditions = {5: futureBasisRatioValue >= 5,
                  4: 2 <= futureBasisRatioValue < 5,
                  3: 0 <= futureBasisRatioValue < 2,
                  2: -2 <= futureBasisRatioValue < 0,
                  1: futureBasisRatioValue < -2}
    pointValueFutureBasisRatio = next(key for key, condition in conditions.items() if condition)

    # 现货趋势
    cashPriceStruValue = resultInfo['cashPriceStru']
    pointValueCashPriceStru = 1 if cashPriceStruValue == 'UP' else 0

    # 基差趋势
    mon1BasisStruValue = resultInfo['mon1BasisStru']
    pointValueMon1BasisStru = 1 if mon1BasisStruValue == 'UP' else 0

    # 仓单历史百分位权重
    hisPercentileValue = resultInfo['hisPercentile']
    if hisPercentileValue >= 80:
        pointValueHisPercentile = 1
    elif hisPercentileValue >= 60:
        pointValueHisPercentile = 2
    elif hisPercentileValue >= 40:
        pointValueHisPercentile = 3
    elif hisPercentileValue >= 20:
        pointValueHisPercentile = 4
    else:
        pointValueHisPercentile = 5

    # 仓单趋势
    warehouseStruValue = resultInfo['warehouseStru']
    pointValueWarehouseStru = 0 if warehouseStruValue == 'UP' else 1

    # 胜率综合计算
    winProbability = float(
        pointValueFutureBasisRatio / 5) * 15 + pointValueCashPriceStru * 20 + pointValueMon1BasisStru * 20 + float(
        pointValueHisPercentile / 5) * 15 + pointValueWarehouseStru * 30
    resultInfo['winProbability'] = winProbability
    resultInfo['createTime'] = datetime.today()
    tradingInfoMp.insert_one_to_mongodb(resultInfo)


def dailyPoolSave(tradingDay, mon1, mon2):
    # 查询当日交易信息
    queryCon = {
        'tradingDay': tradingDay,
        'mon1': mon1,
        'mon2': mon2
    }
    retTradingDayList = tradingInfoMp.find_all_from_mongodb(queryCon)
    if retTradingDayList is None:
        return

    for resultInfo in retTradingDayList:
        productCode = resultInfo['productCode']
        productName = resultInfo['productName']
        mon1 = resultInfo['mon1']
        mon2 = resultInfo['mon2']
        tradingDay = resultInfo['tradingDay']
        structure = resultInfo['structure']
        slope = resultInfo['slope']
        futureBasisRatio = resultInfo['futureBasisRatio']
        ratio = resultInfo['ratio']

        # 1、先通过productCode mon1 mon2查询当前品种 是否在交易池中，
        preCondition = {
            'productCode': productCode,
            'mon1': mon1,
            'mon2': mon2,
            'isValid': 'Y'
        }
        retPreData = tradingPoolMp.find_one_from_mongodb(preCondition)

        orgCondition = {
            'productCode': productCode,
            'mon1': mon1,
            'mon2': mon2,
            'tradingDay': tradingDay,
            'isValid': 'Y'
        }
        retOrgData = tradingPoolMp.find_one_from_mongodb(orgCondition)

        try:
            # 2、不存在，并且满足交易条件，则直接插入
            if (retPreData is None):
                # 策略2：
                # 近月基差率>3%  盈亏比（月差/近月基差）>1
                if float(futureBasisRatio) > 3 and float(ratio) > 1:
                    print(productName + '不存在，满足交易条件' + '=====》【品种交易池】')
                    if (retOrgData == None):
                        resultInfo['isValid'] = 'Y'
                        tradingPoolMp.delete_one_from_mongodb(resultInfo)
                        tradingPoolMp.insert_one_to_mongodb(resultInfo)

            # 3、存在，跟踪近月基差率
            else:
                # 【基差率>0%,,盈亏比>0】则直接插入
                if float(futureBasisRatio) >= 0 and float(ratio) > 0:
                    print(productName + '存在，跟踪近月基差率和盈亏比，新增' + '=====》【品种交易池】')
                    if (retOrgData is None):
                        resultInfo['isValid'] = 'Y'
                        tradingPoolMp.delete_one_from_mongodb(resultInfo)
                        tradingPoolMp.insert_one_to_mongodb(resultInfo)
                # 否则软删除
                else:
                    print(productName + '存在，跟踪近月基差率和盈亏比，删除' + '《=====【品种交易池】')
                    updCondition = {
                        'productCode': productCode,
                        'mon1': mon1,
                        'mon2': mon2
                    }
                    data = {
                        'isValid': 'N'
                    }
                    tradingPoolMp.update_many_to_mongodb(updCondition, data)
        except Exception as e:
            errorMsg = '品种：' + productName + '，dailyPoolSave，异常！' + str(e)
            print(errorMsg)
            continue


def sendEmail(mon1, mon2, tradingDay):

    columns = ['productName', 'structure', 'slope', 'cashPriceStru', 'mon1BasisStru', 'futureBasisRatio', 'ratio']

    # 拼接交易池列表
    tradingPoolCon = {'tradingDay': tradingDay, 'mon1': mon1, 'mon2': mon2}
    tradingPoolList = tradingPoolMp.find_from_mongodb_with_condition(tradingPoolCon, 'createTime')
    if tradingPoolList is None:
        return
    tradingPoolData = pd.DataFrame(tradingPoolList, columns=columns)
    tradingPoolData['futureBasisRatio'] = tradingPoolData['futureBasisRatio'].map(lambda x: str(x) + '%')
    tradingPoolStr = tradingPoolData.to_string(index=False, header=True)


    # 拼接交易信息列表
    tradingInfoCon = {'tradingDay': tradingDay, 'mon1': mon1, 'mon2': mon2}
    tradingInfoList = tradingInfoMp.find_from_mongodb_with_condition(tradingInfoCon, 'createTime')
    if tradingInfoList is None:
        return
    tradingInfo_df = pd.DataFrame(tradingInfoList, columns=columns)
    tradingInfo_df['futureBasisRatio'] = tradingInfo_df['futureBasisRatio'].map(lambda x: str(x) + '%')
    tradingInfoStr = tradingInfo_df.to_string(index=False, header=True)

    # 打印筛选数据
    # 现货UP 基差UP
    df1 = tradingInfo_df[(tradingInfo_df['structure'] == 'BACK')
                                 & (tradingInfo_df['cashPriceStru'] == 'UP')
                                 & (tradingInfo_df['mon1BasisStru'] == 'UP')]
    df2 = tradingInfo_df[(tradingInfo_df['structure'] == 'OTHER')
                         & (tradingInfo_df['cashPriceStru'] == 'UP')
                         & (tradingInfo_df['mon1BasisStru'] == 'UP')]
    # 高基差 盈亏比负值
    tradingInfo_df['futureBasisRatio'] = tradingInfo_df['futureBasisRatio'].str.rstrip('%').astype(float).round(2)
    tradingInfo_df['ratio'] = tradingInfo_df['ratio'].astype(float).round(2)
    df3 = tradingInfo_df[(tradingInfo_df['cashPriceStru'] == 'UP')
                                & (tradingInfo_df['futureBasisRatio'] > 0)
                                & (tradingInfo_df['ratio'] < 0)]
    prt_df = pd.concat([df1,df2,df3],axis=0)
    tradingInfoSelStr = prt_df.to_string(index=False, header=True)

    # 拼接完整邮件信息
    emailStr = tradingPoolStr + '\n\n' + tradingInfoSelStr + '\n\n' + tradingInfoStr
    print(emailStr)

    # 查询当日当套利组合，是否已经发过邮件
    emailCondition = {'tradingDay': tradingDay, 'mon1': mon1, 'mon2': mon2}
    retEmailRecordData = emailRecordMp.find_one_from_mongodb(emailCondition)
    if retEmailRecordData is None:
        # 写文件
        log_file = open('utils/data.txt', 'w+')
        fileUtils.write(log_file, emailStr)
        log_file.close()

        # 邮件附件通知结果
        subject = '期货套利分析【' + tradingDay + '|' + mon1 + '|' + mon2 + '】'
        content = '请查看附件！'
        mail = emailUtils.makeAttrMail(subject, content)
        attachment = emailUtils.makeAttachment('utils/data.txt')
        mail.attach(attachment)
        emailUtils.sendMail(mail)
        print('发送邮件成功')
        emailRecordData = {'tradingDay': tradingDay, 'mon1': mon1, 'mon2': mon2, 'isSendFlag': 'Y'}
        emailRecordMp.insert_one_to_mongodb(emailRecordData)
    else:
        print('当日邮件已发送')
