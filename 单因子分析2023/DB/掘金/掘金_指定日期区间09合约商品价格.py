import pandas as pd
from gm.api import *
# 掘金终端需要打开，接口取数是通过网络请求的方式
# 设置token，可在用户-密钥管理里查看获取已有token ID
from gm.api import set_token

from 单因子分析.DB.工具 import constant, utils

set_token('db3fefbe888684f75e4457e9d40ed84e968bfcb4')

'''
####################################################
格式化保存 掘金 商品加权价格指数
url:https://www.myquant.cn/docs2/sdk/python/API%E4%BB%8B%E7%BB%8D.html#get-history-symbol-%E6%9F%A5%E8%AF%A2%E6%8C%87%E5%AE%9A%E6%A0%87%E7%9A%84%E5%A4%9A%E6%97%A5%E4%BA%A4%E6%98%93%E4%BF%A1%E6%81%AF

指定品种 INVTOTAL
指定日期范围2010-2023
增加行业代码ind_code,品种代码
####################################################
'''

startStr = '2023-01-01'
endStr = '2023-09-05'

'''
主函数
'''
if __name__ == '__main__':
    df = pd.DataFrame()
    # 指定品种
    for code in constant.CODE_LIST:
        print(code)

        # 获取品种指数合约代码 【08合约】
        selectedSymbol = utils.getSelectedSymbol(code, "09")

        # 指定日期区间
        singleDataRaw = get_history_symbol(symbol=selectedSymbol, start_date=startStr,
                                           end_date=endStr, df=True)
        # 格式化存储
        if singleDataRaw is None:
            print("无数据 is None")
            continue
        if len(singleDataRaw) == 0:
            print("无数据 len = 0")
            continue

        # 将数据列整体上移一行,计算当日收盘价close
        singleDataRaw['close'] = singleDataRaw['pre_close'].shift(-1)

        singleDataRaw.rename(columns={"trade_date": "date"}, inplace=True)
        singleData = singleDataRaw.loc[:, ['date', 'close']]
        # 将日期列转换为datetime类型
        singleData['date'] = pd.to_datetime(singleData['date']).dt.date


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

    df.to_csv("../CSV导出/" + "掘金_09合约价格" + startStr + "TO" + endStr + ".csv")
