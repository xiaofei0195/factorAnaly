import pandas as pd
from alphalens.tears import (create_full_tear_sheet,
                             create_event_returns_tear_sheet)
from alphalens.utils import get_clean_factor_and_forward_returns

#
# build price
#
raw_price = pd.read_csv('../../代码与数据/db/akshare/连续合约信息/20200101 TO 20200531全品种连续合约信息 - 副本.csv', encoding='gb2312')
raw_price['日期'] = pd.to_datetime(raw_price['日期'])
raw_price.rename({"日期": "date", "收盘价": "close"}, axis=1, inplace=True)
raw_price.sort_values(['date', 'code'], inplace=True)
price_df = raw_price[['date', 'code', 'close']].pivot(index='date', columns='code', values='close')
print(price_df.head(100))

price_df = price_df.asfreq('C')
print(price_df.index.freq)



#
# build factor
#
# 查询原始因子数据
raw_factor = pd.read_csv('../../代码与数据/db/akshare/展期收益率/2020-01-01 To +2020-05-31全品种展期收益率.csv')
# 日期格式化
raw_factor.date = pd.to_datetime(raw_factor.date)
# 获取numpy值数据
size = raw_factor.index.size
vals = raw_factor.values
v1 = vals[0:size, 0].tolist()
v2 = vals[0:size, 1].tolist()
v3 = vals[0:size, 4].tolist()
data = {'code': v1, 'ratio': v2, 'date': v3}
df = pd.DataFrame(data)
# 设置多重索引
df = df.sort_values(['date', 'code'])
factor_df = df.set_index(['date', 'code'])
print(factor_df)



# 设置分组
group_df = raw_price[['date', 'code', 'ind_code']].sort_values(['date', 'code']).set_index(['date', 'code'])
print(group_df)

sector_names = {
    0: "化工",
    1: "黑色",
    2: "农产品"
}

factor_data = get_clean_factor_and_forward_returns(
    factor_df,
    price_df,
    groupby=group_df['ind_code'],
    quantiles=4,
    periods=(1, 3),
    filter_zscore=None,
    groupby_labels=sector_names)
print(factor_data.head(10))

create_full_tear_sheet(factor_data, long_short=False, group_neutral=False, by_group=False)
create_event_returns_tear_sheet(factor_df, price_df, avgretplot=(3, 11),
                                long_short=False, group_neutral=False, by_group=False)
