import akshare as ak
from pandas import (DataFrame, date_range)
import pandas as pd

from multiprocessing.dummy import Pool as Pool

start = '2023-01-01'
end = '2023-05-31'

dateRange = date_range(start=start, end=end)
dateList = [x.strftime('%F') for x in dateRange]

def m1(date):
    dateStr = str(date).replace('-', '')
    try:
        singleData = ak.get_roll_yield_bar(type_method="var", date=dateStr)
        return singleData

    except Exception as e:
        print(date, 'except: %s' % e)
        return


if __name__ == '__main__':

    # 某交易日, 所有品种的主力次主力合约展期收益率的组合
    print("start!!!")
    df = pd.DataFrame()
    # 开启多线程
    for date in dateList:
        singleData = m1(date)
        df = pd.concat([df, singleData], axis=0)

    df.to_csv(start + " TO " + end + "全品种展期收益率.csv")
    print("done!!!")


