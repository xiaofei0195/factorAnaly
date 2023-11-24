import akshare as ak
import pandas as pd

from 单因子分析.DB.工具 import constant

'''
####################################################
格式化保存 AKSHARE 南华商品价格指数接口
url:https://www.akshare.xyz/data/futures/futures.html#id58

指定品种 INVTOTAL
指定日期范围2010-2023
增加行业代码ind_code,品种代码
####################################################
'''


def getMainConInfo(start_date, end_date):
    df = pd.DataFrame()
    for code in constant.CODE_LIST:
        print(code)

        singleDataRaw = ak.futures_price_index_nh(symbol=code)
        if singleDataRaw is None:
            print("无数据")
            continue

        singleDataRaw.rename(columns={"value": "close"}, inplace=True)
        singleDataRaw['date'] = pd.to_datetime(singleDataRaw['date'])

        # 筛选日期在指定范围内的数据
        singleData = singleDataRaw[
            (singleDataRaw['date'] >= constant.startStr) & (singleDataRaw['date'] <= constant.endStr)]

        singleData['code'] = code

        if code in constant.HS:
            singleData['ind_code'] = 0
        elif code in constant.NCP:
            singleData['ind_code'] = 1
        elif code in constant.NH:
            singleData['ind_code'] = 2
        elif code in constant.YS:
            singleData['ind_code'] = 3

        df = pd.concat([df, singleData], axis=0)
    return df


if __name__ == '__main__':
    print("start!!!")
    df = getMainConInfo("", "")
    df.to_csv("全品种指数价格信息.csv")
    print("done!!!")
