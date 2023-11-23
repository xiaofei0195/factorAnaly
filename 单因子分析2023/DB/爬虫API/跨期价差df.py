import pandas as pd
import requests

from 单因子分析.DB.工具 import constant


def getSpreadDailyTemp(productCode, tradingDay, mon1, mon2):
    url = 'http://www.jiaoyifamen.com/tools/api/future/spread/free?t=1699934925753&type1='+productCode+'&code1='+mon1+'&type2='+productCode+'&code2='+mon2
    try:
        r = requests.get(url)
        # r = requests.post(url, data={'type1': productCode, 'code1': mon1, 'type2': productCode, 'code2': mon2})
    except Exception as e:
        errorMsg = '品种：' + productCode + '，查询近月远月价差（交易法门），接口异常！' + str(e)
        print(errorMsg)
        return None

    retData = r.json()
    data = retData['data']
    if data != None:
        category = data['category']
        value = data['value']
        category_df = pd.DataFrame(category)
        value_df = pd.DataFrame(value)
        print(category_df.size)
        print(value_df.size)

        if category_df.size == value_df.size and category_df.size > 0:
            spread_df = pd.concat([category_df, value_df], axis=1)
            spread_df.set_axis(['date', 'spread'], axis=1, inplace=True)
            return spread_df

    return pd.DataFrame()


df = pd.DataFrame()
for code in constant.CODE_LIST:
    print(code)

    singleData = getSpreadDailyTemp(code, "", constant.mon07["mon1"], constant.mon07["mon2"])

    if len(singleData) > 0:

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

df.to_csv("../CSV导出/跨期价差/MONGO_" + str(constant.mon07["mon1"]) + "_跨期价差.csv")
