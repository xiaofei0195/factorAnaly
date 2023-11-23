import numpy as np
import pandas as pd
from gm.api import *
from numpy import nan
from pandas import (DataFrame, date_range)

# 掘金终端需要打开，接口取数是通过网络请求的方式
# 设置token，可在用户-密钥管理里查看获取已有token ID
set_token('db3fefbe888684f75e4457e9d40ed84e968bfcb4')


# 因子：展期收益率
# 贴水结构表明当前商品供需偏紧或不足，买方愿意为当下购买该商品支付更高的溢价。
# 升水结构表明商品供需过剩，过剩库存需要在未来卖出，这期间持仓成本，例如仓储费、资金成本等。
# 其中，P^c,d：主力合约价格ok
# P^c,n：次主力合约价格ok
# t_c,d：主力合约剩余天数
# t_c,n：次主力合约剩余天数


# 获取主力合约基本信息
# sec_type1证券大类
# 1010: 股票， 1020: 基金， 1030: 债券 ， 1040: 期货， 1050: 期权， 1060: 指数

# sec_type2大类细分
# 股票 101001:A 股，101002:B 股，101003:存托凭证 - 基金 102001:ETF，102002:LOF，102005:FOF - 债券 103001:可转债，103008:回购 - 期货 104001:股指期货，104003:商品期货，104006:国债期货 - 期权 105001:股票期权，105002:指数期权，105003:商品期权 - 指数 106001:股票指数，106002:基金指数，106003:债券指数，106004:期货指数

# exchanges易所代码
# SHSE:上海证券交易所，SZSE:深圳证券交易所 ， CFFEX:中金所，SHFE:上期所，DCE:大商所， CZCE:郑商所，INE:能源中心


sec_type1 = 1040
sec_type2 = 104003
exchanges = ['SHFE','DCE', 'CZCE','INE']

##获取合约基本信息,包含法
# 入参：str 描述key
# 返回：list symbol合约集合
def getSymbolsBaseInfoInc(incDesc):
    codeList = get_symbol_infos(sec_type1, sec_type2=sec_type2, exchanges=exchanges, symbols=None, df=False)

    symbolCodes = []
    symbolInfos = []
    for d in codeList:
        sec_name = d['sec_name']
        if str(sec_name).__contains__(incDesc):
            symbolCodes.append(d['symbol'])
            symbolInfos.append(d)

    return symbolCodes, symbolInfos


##获取合约基本信息，排除法
def getSymbolsBaseInfoExc(excDesc1, excDesc2):
    codeList = get_symbol_infos(sec_type1, sec_type2=sec_type2, exchanges=exchanges, symbols=None, df=False)

    symbolCodes = []
    symbolInfos = []
    for d in codeList:
        sec_name = d['sec_name']
        if not str(sec_name).__contains__(excDesc1) and not str(sec_name).__contains__(excDesc2):
            symbolCodes.append(d['symbol'])
            symbolInfos.append(d)

    return symbolCodes, symbolInfos


# 获取合约价格，包含法
def getSymbolsDailyInc(trade_date, desc):
    symbolCodes, symbolInfos = getSymbolsBaseInfoInc(desc)
    symbols = get_symbols(sec_type1, sec_type2=sec_type2, exchanges=None, symbols=symbolCodes, skip_suspended=True,
                          skip_st=True, trade_date=trade_date, df=True)

    symbolInfosDf = pd.DataFrame(symbolInfos)
    symbolsMerge = pd.merge(symbolInfosDf, symbols, on='symbol')
    return symbolsMerge


# 获取合约价格，排除法
def getSymbolsDailyExc(trade_date, excDesc1, excDesc2):
    symbolCodes, symbolInfos = getSymbolsBaseInfoExc(excDesc1, excDesc2)
    symbols = get_symbols(sec_type1, sec_type2=sec_type2, exchanges=None, symbols=symbolCodes, skip_suspended=True,
                          skip_st=True, trade_date=trade_date, df=True)
    if len(symbols) == 0:
        return pd.DataFrame([])

    symbolInfosDf = pd.DataFrame(symbolInfos)
    symbolsMerge = pd.merge(symbolInfosDf, symbols, on='symbol')
    return symbolsMerge


if __name__ == '__main__':

    start = '2019-01-01'
    end = '2019-12-31'

    dateRange = date_range(start=start, end=end)
    dateList = [x.strftime('%F') for x in dateRange]

    # 某交易日, 所有品种的主力和次主力合约,展期收益率
    print("start!!!")
    df = pd.DataFrame()
    for trade_date in dateList:
        print(trade_date)

        # 1、获取实际主力合约信息，排除法,并拼接价格和合约到期日数据
        # trade_date = '2023-10-23'
        mainIinfos = getSymbolsDailyExc(trade_date, '主', '连')
        if len(mainIinfos) == 0:
            print(trade_date,"无数据")
            continue
        # 1.1、品种持仓升序排列
        # 1.2、按品种分组
        # 1.3、筛选每组最后一条记录，作为实际主力合约
        # 1.4、筛选每组倒数第二条记录，作为实际次主力合约
        val_sorted = mainIinfos.sort_values(by='position', ascending=True)
        mainRel = val_sorted.groupby('underlying_symbol_x').nth(-1)
        secMainRel = val_sorted.groupby('underlying_symbol_x').nth(-2)

        # 修改主力、次主力指定列索引名称
        mainRel.rename(columns={"settle_price": "settle_price_m"}, inplace=True)
        secMainRel.rename(columns={"settle_price": "settle_price_sm"}, inplace=True)

        # 摘取数据
        # 品种代码 underlying_symbol_y
        # 到期日期 delisted_date_x
        # 到期日期 delisted_date_y
        # 主力结算价 settle_price_m
        # 次主力结算价 settle_price_sm
        # 指定要拼接的数据列
        columns_to_concat1 = ['underlying_symbol_y', 'settle_price_m', 'delisted_date_x']
        columns_to_concat2 = ['settle_price_sm', 'delisted_date_y']
        raw_data = pd.concat([mainRel[columns_to_concat1], secMainRel[columns_to_concat2]], axis=1)
        raw_data['date'] = trade_date

        # 2、计算T10的展期收益率
        # log(主力合约价格/次主力合约价格)*360/(次主力合约剩余天数-主力合约剩余天数)
        raw_data['price_div_log'] = (raw_data['settle_price_m'] / raw_data['settle_price_sm']).apply(np.log)
        raw_data['date_minus'] = abs((raw_data['delisted_date_y'] - raw_data['delisted_date_x']).dt.days)
        raw_data['factor'] = (raw_data['price_div_log']*365)/raw_data['date_minus']

        # print(raw_data)
        df = pd.concat([df,raw_data],axis=0)

    df.to_csv(start + " TO " + end + "全品种T10展期收益率.csv")