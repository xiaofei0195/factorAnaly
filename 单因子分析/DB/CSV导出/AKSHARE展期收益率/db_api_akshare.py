import akshare as ak
from pandas import (DataFrame, date_range)
import pandas as pd

from multiprocessing.dummy import Pool as Pool

maxPool = 50

start = '2010-1-01'
end = '2010-12-31'
dateRange = date_range(start=start, end=end)
dateList = [x.strftime('%F') for x in dateRange]


def m1(date, test):
    dateStr = str(date).replace('-', '')
    try:
        singleData = ak.get_roll_yield_bar(type_method="var", date=dateStr)
        return singleData

    except Exception as e:
        print(date, 'except: %s' % e)
        return


if __name__ == '__main__':

    # 某交易日, 所有品种的主力次主力合约展期收益率的组合

    df = pd.DataFrame()
    # 开启多线程
    for date in dateList:
        pool1 = Pool(processes=maxPool)
        singleData = pool1.apply_async(m1, (date, "test")).get()
        df = pd.concat([df, singleData], axis=0)

    df.to_csv(start + " To +" + end + "全品种展期收益率.csv")

    pool1.close()
    pool1.join()
