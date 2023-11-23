# -*- coding:utf-8 -*-
import os
import sys

import pandas as pd
from pandas.tseries.offsets import BDay

from utils.other import comUtils

try:
    import talib
except:
    print('请安装TA-Lib库')
    # 安装talib请看文档https://www.myquant.cn/docs/gm3_faq/154?
    sys.exit(-1)

# 把当前文件所在文件夹的父文件夹路径加入到PYTHONPATH

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mapper.temp import priceStruDailyTempMapper as priceStruTempMp, warehouseDailyTempMapper as warehouseDailyTempMp, \
    spreadDailyTempMapper as spreadDailyTempMp
from mapper.clean import priceStruDailyMapper as priceStruMp, warehouseDailyMapper as warehouseDailyMp, \
    spreadDailyMapper as spreadDailyMp, cashPriceMapper as cashPriceMp


def cleanToPriceStruDaily(baseInfo, tradingDay, mon1):
    productCode = baseInfo['productCode']
    productName = baseInfo['productName']

    queryCon = {'tradingDay': tradingDay, 'productCode': productCode}
    result = priceStruTempMp.find_one_from_mongodb(queryCon)
    try:
        if result != None:
            data = result['data']

            round_slop = 1
            priceSet = data['todayData']
            df = pd.DataFrame(priceSet)

            # 获取近月价格
            mon1Price = df.loc[df['instrument'].str.contains(str(mon1)), 'closePrice'].values
            if len(mon1Price) > 0:
                mon1Price = float(mon1Price[0])
            else:
                mon1Price = None

            # 计算期限结构、基差动量
            if len(df) > 3:
                structure = comUtils.calculate_term_structure(df, 'closePrice')
                slope = comUtils.calculate_price_slop(df, 'closePrice')
                round_slop = round(slope, 2)
            else:
                structure = 'NG'



            # 先删除，再新增
            delCon = {'tradingDay': tradingDay, 'productCode': productCode}
            priceStruMp.delete_one_from_mongodb(delCon)

            bean = {'productName': productName, 'productCode': productCode, 'structure': structure, 'slope': round_slop,
                    'tradingDay': tradingDay, 'mon1Price': mon1Price}
            priceStruMp.insert_one_to_mongodb(bean)

            return bean
        else:
            '品种：' + productName + '，getTempPriceStruDaily，异常！'
    except Exception as e:
        errorMsg = '品种：' + productName + '，cleanToPriceStruDaily，异常！' + str(e)
        print(errorMsg)
        return None


'''
#######################################################
# 查询近月远月价差、指定日期区间的近月价格集合
# 1、初始化前15日数据 updFlag=N
# 2、已初始化清洗数据 updFlag=Y
#######################################################
'''


def cleanToSpreadDaily(baseInfo, tradingDay, mon1, mon2, updFlag):
    try:
        productCode = baseInfo['productCode']
        productName = baseInfo['productName']

        tempCon = {
            'productCode': baseInfo['productCode'],
            'tradingDay': tradingDay,
            'mon1': mon1,
            'mon2': mon2
        }
        result = spreadDailyTempMp.find_one_from_mongodb(tempCon)

        if result is not None:
            # 获取日期集合
            data = result['data']
            category = data['category']
            value = data['value']
            recentPrice = data['recentPrice']

            category_df = pd.DataFrame(category)
            value_df = pd.DataFrame(value)
            recentPrice_df = pd.DataFrame(recentPrice)

            if category_df.size == value_df.size and category_df.size == recentPrice_df.size and category_df.size > 0:
                spread_mon1price_df = pd.concat([category_df, value_df, recentPrice_df], axis=1)
                spread_mon1price_df['productCode'] = productCode
                spread_mon1price_df.set_axis(['tradingDay', 'spread', 'mon1Price', 'productCode'], axis=1, inplace=True)
                spread_mon1price_df['tradingDay'] = pd.to_datetime(spread_mon1price_df['tradingDay'])
            else:
                return None

            '''
            # 通过日期下标，获取价差
            '''
            spreads = spread_mon1price_df.loc[spread_mon1price_df['tradingDay'] == tradingDay, 'spread'].values
            if len(spreads) == 0:  # 如果找不到指定日期的价差
                # 找到当前日期往前10个工作日的最后一个日期列表
                business_dates = pd.date_range(end=tradingDay, periods=3, freq=BDay())

                # 筛选最后一个日期的价差
                spread = spread_mon1price_df.loc[spread_mon1price_df['date'].isin(business_dates), 'spread'].values[-1]
                print(spread)
            else:
                spread = spreads[0]

            '''
            # 计算跨期价差趋势
            '''
            spreadMa2 = comUtils.calAverage(spread_mon1price_df, "spread", tradingDay, 2)
            spreadMa10 = comUtils.calAverage(spread_mon1price_df, "spread", tradingDay, 10)
            spreadStru = 'UP' if spreadMa2 >= spreadMa10 else 'DOWN'

            '''
            # 计算近月价格趋势
            '''
            mon1Ma2 = comUtils.calAverage(spread_mon1price_df, "mon1Price", tradingDay, 2)
            mon1Ma10 = comUtils.calAverage(spread_mon1price_df, "mon1Price", tradingDay, 10)
            mon1PriceStru = 'UP' if mon1Ma2 >= mon1Ma10 else 'DOWN'

            '''
            # 计算现货价格趋势
            '''
            queryCashPriceCon = {'productCode': productCode}
            retCashPriceList = cashPriceMp.find_all_from_mongodb(queryCashPriceCon)
            cashPrice_df = pd.DataFrame(retCashPriceList)
            cashPrice_df['tradingDay'] = pd.to_datetime(cashPrice_df['tradingDay'])
            cashPrice_df.sort_values('tradingDay')

            cashPriceMa2 = comUtils.calAverage(cashPrice_df, "cashPrice", tradingDay, 2)
            cashPriceMa10 = comUtils.calAverage(cashPrice_df, "cashPrice", tradingDay, 10)
            cashPriceStru = 'UP' if cashPriceMa2 >= cashPriceMa10 else 'DOWN'

            '''
            # 计算近月基差趋势
            '''
            spread_price_df = pd.merge(spread_mon1price_df, cashPrice_df, on=['tradingDay', 'productCode'])
            # 计算近月基差 = 现货 - 近月价格
            spread_price_df['mon1Basis'] = spread_price_df['cashPrice'] - spread_price_df['mon1Price']

            mon1BasisStru = 'NG'
            mon1BasisMa2 = comUtils.calAverage(spread_price_df, "mon1Basis", tradingDay, 2)
            mon1BasisMa10 = comUtils.calAverage(spread_price_df, "mon1Basis", tradingDay, 10)
            mon1BasisStru = 'UP' if mon1BasisMa2 >= mon1BasisMa10 else 'DOWN'

            retDic = {
                'spread': spread,
                'mon1BasisStru': mon1BasisStru,
                'cashPriceStru': cashPriceStru,
                'mon1PriceStru': mon1PriceStru,
                'productCode': productCode,
                'productName': productName,
                'tradingDay': tradingDay,
                'mon1': mon1,
                'mon2': mon2
            }

            # 先删除，再插入
            delCon = {
                'productCode': productCode,
                'tradingDay': tradingDay,
                'mon1': mon1,
                'mon2': mon2
            }
            spreadDailyMp.delete_one_from_mongodb(delCon)
            spreadDailyMp.insert_one_to_mongodb(retDic)

            # 已清洗过 则更新月差结构入库
            if updFlag == 'Y':
                updCon = {
                    'productCode': productCode,
                    'tradingDay': tradingDay,
                    'mon1': mon1,
                    'mon2': mon2
                }
                updData = {'spreadStru': spreadStru}
                spreadDailyMp.update_to_mongodb(updCon, updData)

    except Exception as e:
        errorMsg = f'品种：{productName}，cleanToSpreadDaily，total异常！{e}'
        print(errorMsg)
        return None

    dataCon = {}
    dataCon['productCode'] = productCode
    dataCon['tradingDay'] = tradingDay
    dataCon['mon1'] = mon1
    dataCon['mon2'] = mon2
    dataResult = spreadDailyMp.find_one_from_mongodb(dataCon)
    return dataResult


# 定义函数来处理分组后的数据
def get_value(group, df, tradingDay):
    # 如果当前年份的数值不存在，取9月11日往前两个工作日的数值的平均值
    if len(group) == 0:
        prev_dates = pd.bdate_range(end=tradingDay, periods=2, closed='left')
        prev_values = df[df['tradingDay'].isin(prev_dates)]['value']
        return prev_values.mean()
    else:
        return group['value']


##仓单分析，数据处理
def cleanToWarehouseDaily(baseInfo, tradingDay):
    try:
        productCode = baseInfo['productCode']
        productName = baseInfo['productName']
        tempCon = {'tradingDay': tradingDay, 'productCode': productCode}
        result = warehouseDailyTempMp.find_one_from_mongodb(tempCon)

        if result is None:
            return

        data = result['data']

        category = data['category']
        value = data['value']
        category_df = pd.DataFrame(category)
        value_df = pd.DataFrame(value)

        if category_df.size == value_df.size and category_df.size > 0:
            warehouse_df = pd.concat([category_df, value_df], axis=1)
            warehouse_df['productCode'] = productCode
            warehouse_df.set_axis(['tradingDay', 'value', 'productCode'], axis=1, inplace=True)
            warehouse_df['tradingDay'] = pd.to_datetime(warehouse_df['tradingDay'])
        else:
            return None

        # 获取各年份当日历史仓单数值
        df = warehouse_df
        # 按年份分组
        df_grouped = df.groupby(df['tradingDay'].dt.year)
        # 遍历每个分组，生成不同的DataFrame
        dfs = []
        for group_name, group_df in df_grouped:
            dfs.append(group_df)

        # 输出每个年份对应的DataFrame
        month_day = tradingDay[5:]
        wareList = []
        for i, df_year in enumerate(dfs):
            year = str(df_year['tradingDay'].dt.year.unique()[0])
            curDay = year + "-" + month_day

            if curDay in df_year['tradingDay'].dt.strftime('%Y-%m-%d').tolist():
                value = df_year.loc[df_year['tradingDay'] == curDay, 'value'].values[0]
            else:
                value = comUtils.calAverage(df_year, "value", curDay, 2)

            dic = {'tradingDay': curDay, "value": value}
            wareList.append(dic)

        ware_df = pd.DataFrame(wareList)
        ware_df.set_axis(['tradingDay', 'value'], axis=1, inplace=True)
        ware_df.sort_values('tradingDay')
        last_price = ware_df['value'].iloc[-1]
        max_price = ware_df['value'].max()
        min_price = ware_df['value'].min()

        # 计算仓单历史百分位
        # 目前值11.77举例：历史估值水平=（11.77-历史最小）/（历史最高-历史最小）
        if max_price == min_price:
            hisPercentile = 0
        else:
            hisPercentile = (last_price - min_price) * 100 / (max_price - min_price)
        hisPercentile = round(hisPercentile, 2)

        '''
        # 计算仓单趋势
        '''
        warehouseMa2 = comUtils.calAverage(warehouse_df, "value", tradingDay, 2)
        warehouseMa10 = comUtils.calAverage(warehouse_df, "value", tradingDay, 10)
        warehouseStru = 'UP' if warehouseMa2 >= warehouseMa10 else 'DOWN'

        delCon = {'tradingDay': tradingDay, 'productCode': productCode}
        warehouseDailyMp.delete_one_from_mongodb(delCon)

        bean = {'tradingDay': tradingDay, 'productName': productName, 'productCode': productCode,
                'hisPercentile': hisPercentile, 'curValue': last_price, 'wareMa2': warehouseMa2,
                'wareMa10': warehouseMa10, 'warehouseStru': warehouseStru}
        warehouseDailyMp.insert_one_to_mongodb(bean)
        return bean

    except Exception as e:
        errorMsg = '品种：' + productName + '，cleanToWarehouseDaily，异常！【可忽略】' + str(e)
        print(errorMsg)
        return None
