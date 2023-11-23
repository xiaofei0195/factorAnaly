import math

import pandas as pd

# 指定日期范围
startStr = '2023-01-01'
endStr = '2023-11-05'
start_date = pd.to_datetime(startStr)
end_date = pd.to_datetime(endStr)

# 指定回测品种
codeList = ["V", "P", "B", "M", "I", "JD", "L", "PP", "Y", "C", "A", "J", "JM", "CS", "EG", "RR", "EB", "TA",
            "RM", "JR", "SR", "CF", "FG", "SF", "SM", "CY", "AP", "CJ", "UR", "SA", "FU", "SC", "AL", "RU", "ZN",
            "CU", "AU", "RB", "WR", "PB", "AG", "BU", "HC", "SN", "NI", "SP", "NR", "SS"]

# 读出指定品种，指定年份的指数价格信息
raw_price = pd.read_csv('../DB/CSV导出/商品指数/AKSHARE全品种指数价格信息.csv')
raw_price = raw_price[raw_price['date'].str.contains('2020|2021|2022|2023')]
raw_price = raw_price[raw_price['code'].isin(codeList)]
raw_price['date'] = pd.to_datetime(raw_price['date'])
raw_price.rename({"value": "close", "code": "asset"}, axis=1, inplace=True)
raw_price['daily_return'] = raw_price.groupby('asset')['close'].pct_change()

# 计算稳健动量因子
Nt = len(codeList)
K = 10
# 使用rank()函数计算排名，设置ascending=False表示降序排列
raw_price['rank'] = raw_price.groupby('date')['daily_return'].rank()
# 滚动计算过去10个交易日排名数值的标准化得分
raw_price['标准化得分'] = raw_price.groupby('asset')['rank'].apply(
    lambda x: (x - (Nt + 1) / 2) / math.sqrt((Nt + 1) * (Nt - 1) / 12))
# 计算某列下某行数据往前推10行数据的均值
raw_price['roll_mean_val'] = raw_price['标准化得分'].rolling(window=10, min_periods=1).mean()

raw_price.sort_values(['date', 'asset'], inplace=True)
price_df = raw_price[['date', 'asset', 'roll_mean_val']]
price_df.to_csv(startStr + " TO " + endStr + "稳健动量因子.csv")
print(price_df)
