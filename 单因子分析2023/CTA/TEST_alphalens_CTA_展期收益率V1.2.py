import alphalens
import numpy as np
import pandas as pd
from alphalens.utils import get_clean_factor_and_forward_returns


# V1.1计算当日累计收益
# V1.2回测全年数据

def cal(x):
    data = x['shift'] / x['close']
    return data


def winsor(ser):
    max_value, min_value = ser.mean() + 3 * ser.std(), ser.mean() - 3 * ser.std()
    return pd.Series([max_value if item > max_value else (min_value if item < min_value else item) for item in ser])


def standize(ser):
    return (ser - ser.mean()) / ser.std()


# def industry_neutralization(factor_df, factor_name):
#     result = sm.OLS(factor_df[factor_name], factor_df[list(factor_df.ind_code.unique())], hasconst=True).fit()
#     return result.resid


'''
# build price
# 2020全年
'''
raw_price = pd.read_csv('../../代码与数据/db/akshare/20200101 TO 20201231全品种连续合约信息.csv')
raw_price['日期'] = pd.to_datetime(raw_price['日期'])
raw_price.rename({"日期": "date", "收盘价": "close"}, axis=1, inplace=True)

# 遍历df计算单品种每日涨幅
raw_price['shift'] = raw_price['close'] - raw_price['close'].shift(1)
raw_price.sort_values(['code', 'date'], inplace=True)
# shift第一个数值赋值为0
column_to_search = 'date'
value_to_search = '2020-01-02 00:00:00'
column_to_update = 'shift'
new_value = 0
raw_price.loc[raw_price[column_to_search] == value_to_search, column_to_update] = new_value

win_factor_value = raw_price.groupby('code').apply(lambda x: cal(x))
raw_price['daily_return'] = win_factor_value.values

raw_price.sort_values(['date', 'code'], inplace=True)
# raw_price['daily_return'] = raw_price['shift']/raw_price['close']
raw_price['total_return'] = raw_price.groupby('code')['daily_return'].expanding().apply(lambda x: np.prod(x + 1)).values

# 筛选需要的数据
raw_price.sort_values(['date', 'code'], inplace=True)
price_df = raw_price[['date', 'code', 'total_return']].pivot(index='date', columns='code', values='total_return')

price_df = price_df.asfreq('C')
# print(price_df.index.freq)
print("price_df:")
print(price_df)

'''
# build factor
# 2020全年
'''
# 查询原始因子数据
raw_factor1 = pd.read_csv('../../代码与数据/db/akshare/展期收益率/2020-01-01 To +2020-05-31全品种展期收益率.csv')
raw_factor2 = pd.read_csv('../../代码与数据/db/akshare/展期收益率/2020-06-01 To +2020-12-31全品种展期收益率.csv')
raw_factor = pd.concat([raw_factor1, raw_factor2], axis=0)
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
# factor_df = df.set_index(['date', 'code'])
# print(factor_df)
factor_df_cp = df.copy()

# #因子去极值
column_name = 'ratio'
win_factor_value = factor_df_cp.groupby('date').apply(lambda x: winsor(x[column_name]))
factor_df_cp['factor_wined'] = win_factor_value.values

# 因子标准化
factor_df_cp['factor_std'] = factor_df_cp.groupby('date').apply(lambda x: standize(x['factor_wined'])).values

# 不需要中性化的因子
factor_df_noneg = factor_df_cp[['code', 'date', 'factor_std']]
factor_flag = 1
factor_df = factor_df_noneg.set_index(['date', 'code'])
factor_df = factor_flag * factor_df
print("factor_df:")
print(factor_df)

# 设置分组
group_df = raw_price[['date', 'code', 'ind_code']].sort_values(['date', 'code']).set_index(['date', 'code'])
print("group_df:")
print(group_df)

sector_names = {
    0: "黑色",
    1: "农产品",
    2: "能化",
    3: "有色",
    9: "其他",
}

# 数据预处理
factor_data = get_clean_factor_and_forward_returns(
    factor_df,
    price_df,
    groupby=group_df['ind_code'],
    quantiles=5,
    groupby_labels=sector_names)

print(factor_data.head(100))

# spearman_ic = factor_data.reset_index().groupby('date').apply(lambda x: x['factor'].corr(x['5D'], method='spearman'))
# 因子收益分析
#, long_short=True, group_neutral=False, by_group=True
alphalens.tears.create_returns_tear_sheet(factor_data)
##因子IC分析
# alphalens.tears.create_information_tear_sheet(factor_data, group_neutral=False, by_group=False)
# 换手率分析
# alphalens.tears.create_turnover_tear_sheet(factor_data)
