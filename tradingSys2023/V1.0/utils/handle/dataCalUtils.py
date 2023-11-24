# -*- coding:utf-8 -*-
import os
import sys
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
    if futureBasisRatioValue >= 5:
        pointValueFutureBasisRatio = 5
    if futureBasisRatioValue >= 2 and futureBasisRatioValue < 5:
        pointValueFutureBasisRatio = 4
    if futureBasisRatioValue >= 0 and futureBasisRatioValue < 2:
        pointValueFutureBasisRatio = 3
    if futureBasisRatioValue >= -2 and futureBasisRatioValue < 0:
        pointValueFutureBasisRatio = 2
    if futureBasisRatioValue < -2:
        pointValueFutureBasisRatio = 1

    # 现货趋势
    cashPriceStruValue = resultInfo['cashPriceStru']
    if cashPriceStruValue == 'UP':
        pointValueCashPriceStru = 1
    if cashPriceStruValue == 'DOWN':
        pointValueCashPriceStru = 0

    # 基差趋势
    mon1BasisStruValue = resultInfo['mon1BasisStru']
    if mon1BasisStruValue == 'UP':
        pointValueMon1BasisStru = 1
    if mon1BasisStruValue == 'DOWN':
        pointValueMon1BasisStru = 0

    # 仓单历史百分位权重
    hisPercentileValue = resultInfo['hisPercentile']
    if hisPercentileValue >= 0 and hisPercentileValue < 20:
        pointValueHisPercentile = 5
    if hisPercentileValue >= 20 and hisPercentileValue < 40:
        pointValueHisPercentile = 4
    if hisPercentileValue >= 40 and hisPercentileValue < 60:
        pointValueHisPercentile = 3
    if hisPercentileValue >= 60 and hisPercentileValue < 80:
        pointValueHisPercentile = 2
    if hisPercentileValue >= 80:
        pointValueHisPercentile = 1

    # 仓单趋势
    warehouseStruValue = resultInfo['warehouseStru']
    if warehouseStruValue == 'UP':
        pointValueWarehouseStru = 0
    if warehouseStruValue == 'DOWN':
        pointValueWarehouseStru = 1

    # 胜率综合计算
    winProbability = float(
        pointValueFutureBasisRatio / 5) * 15 + pointValueCashPriceStru * 20 + pointValueMon1BasisStru * 20 + float(
        pointValueHisPercentile / 5) * 15 + pointValueWarehouseStru * 30
    resultInfo['winProbability'] = winProbability
    resultInfo['createTime'] = datetime.today()
    tradingInfoMp.insert_one_to_mongodb(resultInfo)


def dailyPoolSave(tradingDay, mon1, mon2):
    # 查询当日交易信息
    queryCon = {}
    queryCon['tradingDay'] = tradingDay
    queryCon['mon1'] = mon1
    queryCon['mon2'] = mon2
    retTradingDayList = tradingInfoMp.find_all_from_mongodb(queryCon)
    if retTradingDayList == None:
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
        preCondition = {}
        preCondition['productCode'] = productCode
        preCondition['mon1'] = mon1
        preCondition['mon2'] = mon2
        preCondition['isValid'] = 'Y'
        retPreData = tradingPoolMp.find_one_from_mongodb(preCondition)

        orgCondition = {}
        orgCondition['productCode'] = productCode
        orgCondition['mon1'] = mon1
        orgCondition['mon2'] = mon2
        orgCondition['tradingDay'] = tradingDay
        orgCondition['isValid'] = 'Y'
        retOrgData = tradingPoolMp.find_one_from_mongodb(orgCondition)

        try:
            # 2、不存在，并且满足交易条件，则直接插入
            if (retPreData == None):
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
                    if (retOrgData == None):
                        resultInfo['isValid'] = 'Y'
                        tradingPoolMp.delete_one_from_mongodb(resultInfo)
                        tradingPoolMp.insert_one_to_mongodb(resultInfo)
                # 否则软删除
                else:
                    print(productName + '存在，跟踪近月基差率和盈亏比，删除' + '《=====【品种交易池】')
                    updCondition = {}
                    updCondition['productCode'] = productCode
                    updCondition['mon1'] = mon1
                    updCondition['mon2'] = mon2
                    data = {}
                    data['isValid'] = 'N'
                    tradingPoolMp.update_many_to_mongodb(updCondition, data)
        except Exception as e:
            errorMsg = '品种：' + productName + '，dailyPoolSave，异常！' + str(e)
            print(errorMsg)
            continue


def sendEmail(mon1, mon2, tradingDay):
    # 1、先通过日期从库里查，查不到，再【重新通过指定日期，获取所有品种现货价格】
    tradingPoolCon = {}
    tradingPoolCon['tradingDay'] = tradingDay
    tradingPoolCon['mon1'] = mon1
    tradingPoolCon['mon2'] = mon2
    tradingPoolList = tradingPoolMp.find_from_mongodb_with_condition(tradingPoolCon, 'createTime')
    if tradingPoolList == None:
        return

    print('-----------------------------------------------------------------')
    print('品种' + '\t\t期限结构' + '\t\t斜率' + '\t\t现货趋势' + '\t\t基差趋势' + '\t\t基差率(%)' + '\t\t盈亏比')
    print('-----------------------------------------------------------------')

    # 打印邮件头【交易池信息】
    emailStr = ""
    emailTradingPool = '品种' + '\t\t期限结构' + '\t\t斜率' + '\t\t现货趋势' + '\t\t基差趋势' + '\t\t基差率(%)' + '\t盈亏比' + '\n'

    for tradingPoolData in tradingPoolList:
        tradingPoolStr = tradingPoolData['productName'] + '\t\t' + \
                         tradingPoolData['structure'] + '\t\t' + \
                         str(tradingPoolData['slope']) + '\t\t' + \
                         str(tradingPoolData['cashPriceStru']) + '\t\t' + \
                         str(tradingPoolData['mon1BasisStru']) + '\t\t' + \
                         str(tradingPoolData['futureBasisRatio']) + '% \t\t' + \
                         str(tradingPoolData['ratio'])
        # print(tradingPoolStr)
        emailTradingPool = emailTradingPool + tradingPoolStr + '\n'

    tradingInfoCon = {}
    tradingInfoCon['tradingDay'] = tradingDay
    tradingInfoCon['mon1'] = mon1
    tradingInfoCon['mon2'] = mon2
    tradingInfoList = tradingInfoMp.find_from_mongodb_with_condition(tradingInfoCon, 'createTime')
    # 打印邮件头【】
    emailTradingInfo = '品种' + '\t\t期限结构' + '\t\t斜率' + '\t\t现货趋势' + '\t\t基差趋势' + '\t\t基差率(%)' + '\t盈亏比' + '\n'

    for tradingInfoData in tradingInfoList:
        tradingInfoStr = tradingInfoData['productName'] + '\t\t' + \
                         tradingInfoData['structure'] + '\t\t' + \
                         str(tradingInfoData['slope']) + '\t\t' + \
                         str(tradingInfoData['cashPriceStru']) + '\t\t' + \
                         str(tradingInfoData['mon1BasisStru']) + '\t\t' + \
                         str(tradingInfoData['futureBasisRatio']) + '% \t\t' + \
                         str(tradingInfoData['ratio'])
        # print(tradingInfoStr)
        emailTradingInfo = emailTradingInfo + tradingInfoStr + '\n'

    emailStr = emailTradingPool + '\n' + emailTradingInfo
    print(emailStr)

    # 查询当日当套利组合，是否已经发过邮件
    emailCondition = {}
    emailCondition['tradingDay'] = tradingDay
    emailCondition['mon1'] = mon1
    emailCondition['mon2'] = mon2
    retEmailRecordData = emailRecordMp.find_one_from_mongodb(emailCondition)
    if (retEmailRecordData == None):
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
        emailRecordData = {}
        emailRecordData['tradingDay'] = tradingDay
        emailRecordData['mon1'] = mon1
        emailRecordData['mon2'] = mon2
        emailRecordData['isSendFlag'] = 'Y'
        emailRecordMp.insert_one_to_mongodb(emailRecordData)
    else:
        print('当日邮件已发送')
