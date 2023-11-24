import alphalens
import matplotlib as plt
import pandas as pd
from alphalens.utils import get_clean_factor_and_forward_returns
import numpy as np

# V1.1计算当日累计收益
# V1.2回测全年数据
# V1.3修改算法，计算每日涨幅和累计涨幅
# V1.4从掘金获取T10展期收益率
# V1.5修改格式化数据索引

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
# 2020-2021年
'''
# raw_price = pd.read_csv('../db/akshare/20200101 TO 20201231全品种连续合约信息.csv')
raw_price2020 = pd.read_csv('../../代码与数据/db/akshare/20200101 TO 20201231全品种连续合约信息.csv')
raw_price2021 = pd.read_csv('../../代码与数据/db/akshare/20210101 TO 20211231全品种连续合约信息.csv')
raw_price = pd.concat([raw_price2020, raw_price2021], axis=0)
raw_price['日期'] = pd.to_datetime(raw_price['日期'])
raw_price.rename({"日期": "date", "收盘价": "close", "code": "asset"}, axis=1, inplace=True)

raw_price['daily_return'] = raw_price.groupby('asset')['close'].pct_change()
raw_price['cumulative_return'] = raw_price.groupby('asset')['daily_return'].cumsum()

# 筛选需要的数据
raw_price.sort_values(['date', 'asset'], inplace=True)
price_df = raw_price[['date', 'asset', 'cumulative_return']].pivot(index='date', columns='asset',
                                                                  values='cumulative_return')
price_df = price_df.asfreq('C')
# print(price_df.index.freq)
print("价格数据:")
print(price_df)

'''
# build factor
# 2020-2021全年
'''
# 查询原始因子数据
raw_factor2020 = pd.read_csv('../../代码与数据/db/gm/2020-01-01 TO 2020-12-31全品种T10展期收益率.csv')
raw_factor2021 = pd.read_csv('../../代码与数据/db/gm/2021-01-01 TO 2021-12-31全品种T10展期收益率.csv')
raw_factor = pd.concat([raw_factor2020, raw_factor2021], axis=0)
# 日期格式化
raw_factor.date = pd.to_datetime(raw_factor.date)
# 获取numpy值数据
size = raw_factor.index.size
vals = raw_factor.values
v1 = vals[0:size, 1].tolist()
v2 = vals[0:size, 9].tolist()
v3 = vals[0:size, 6].tolist()
data = {'asset': v1, 'ratio': v2, 'date': v3}
df = pd.DataFrame(data)
# 格式化code列
df['asset'] = df['asset'].str.split('.').str[1]

# 设置多重索引
df = df.sort_values(['date', 'asset'])
# factor_df = df.set_index(['date', 'asset'])
# print(factor_df)
factor_df_cp = df.copy()

# #因子去极值
column_name = 'ratio'
win_factor_value = factor_df_cp.groupby('date').apply(lambda x: winsor(x[column_name]))
factor_df_cp['factor_wined'] = win_factor_value.values

# 因子标准化
factor_df_cp['factor_std'] = factor_df_cp.groupby('date').apply(lambda x: standize(x['factor_wined'])).values

# 不需要中性化的因子
factor_df_noneg = factor_df_cp[['asset', 'date', 'factor_std']]
factor_flag = 1
factor_df = factor_df_noneg.set_index(['date', 'asset'])
factor_df = factor_flag * factor_df
print("因子数据:")
print(factor_df)

# 设置分组
group_df = raw_price[['date', 'asset', 'ind_code']].sort_values(['date', 'asset']).set_index(['date', 'asset'])
print("分组数据:")
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
    groupby_labels=sector_names,
    max_loss=0.15,
    periods=(1, 5, 10))

print(factor_data.head(100))

# spearman_ic = factor_data.reset_index().groupby('date').apply(lambda x: x['factor'].corr(x['5D'], method='spearman'))

# 因子收益分析
# factor_data.replace([np.inf,-np.inf],0)
# alphalens.tears.create_returns_tear_sheet(factor_data)


# # 换手率分析
# alphalens.tears.create_turnover_tear_sheet(factor_data)

# 信息量分析
res_ic = alphalens.performance.factor_information_coefficient(factor_data)
print("因子IC:")
print(res_ic)
alphalens.plotting.plot_ic_ts(res_ic)
alphalens.plotting.plot_ic_hist(res_ic)
alphalens.plotting.plot_ic_qq(res_ic)
plt.pyplot.show()

# 分组分析
# ic_by_sector = alphalens.performance.mean_information_coefficient(factor_data, by_group=True)
# ic_by_sector.head()
# alphalens.plotting.plot_ic_by_group(ic_by_sector)
# plt.pyplot.show()
