import alphalens
import matplotlib as plt
import pandas as pd
import numpy as np
from alphalens.utils import get_clean_factor_and_forward_returns

# V1.1计算当日累计收益
# V1.2回测全年数据
# V1.3修改算法，计算每日涨幅和累计涨幅
# V1.4从掘金获取T10展期收益率
# V1.5修改格式化数据索引
# V1.5.1掘金数据2023年
# V1.5.2价格数据源替换为指数价格

# 指定回测品种
codeList = ["V", "P", "B", "M", "I", "JD", "L", "PP", "Y", "C", "A", "J", "JM", "CS", "EG", "RR", "EB", "TA",
            "RM", "JR", "SR", "CF", "FG", "SF", "SM", "CY", "AP", "CJ", "UR", "SA", "FU", "SC", "AL", "RU", "ZN",
            "CU", "AU", "RB", "WR", "PB", "AG", "BU", "HC", "SN", "NI", "SP", "NR", "SS"]


def winsor(ser):
    max_value, min_value = ser.mean() + 3 * ser.std(), ser.mean() - 3 * ser.std()
    return pd.Series([max_value if item > max_value else (min_value if item < min_value else item) for item in ser])


def standize(ser):
    return (ser - ser.mean()) / ser.std()


# def industry_neutralization(factor_df, factor_name):
#     result = sm.OLS(factor_df[factor_name], factor_df[list(factor_df.ind_code.unique())], hasconst=True).fit()
#     return result.resid


# 指定日期范围
start_date = pd.to_datetime('2023-01-01')
end_date = pd.to_datetime('2023-10-01')

'''
########################################################
# build 价格
# 2020-2021年
# 2023年
########################################################
'''
# 提取2023年数据，指定品种指数价格
raw_price_00 = pd.read_csv('../../代码与数据/db/akshare/全品种指数价格信息.csv')
raw_price_2023 = raw_price_00[raw_price_00['date'].str.contains('2023')]
raw_price = raw_price_2023[raw_price_2023['code'].isin(codeList)]

raw_price['date'] = pd.to_datetime(raw_price['date'])
# raw_price2020 = pd.read_csv('../db/akshare/20230101 TO 20201231全品种连续合约信息.csv')
# raw_price2021 = pd.read_csv('../db/akshare/20230101 TO 20211231全品种连续合约信息.csv')
# raw_price = pd.concat([raw_price2020, raw_price2021], axis=0)

raw_price.rename({"value": "close", "code": "asset"}, axis=1, inplace=True)

raw_price['daily_return'] = raw_price.groupby('asset')['close'].pct_change()
# raw_price['cumulative_return'] = raw_price.groupby('asset')['daily_return'].cumsum()
raw_price['cumulative_return'] = raw_price.groupby('asset')['daily_return'].expanding().apply(lambda x: np.prod(x + 1)).values

# # 筛选需要的数据
# selected_columns = ['date', 'asset', 'cumulative_return']
# raw_price = raw_price.loc[:, selected_columns]
# print(raw_price)
# raw_price.set_index(['date','asset'],inplace=True)
# print(raw_price)

# 筛选日期在指定范围内的数据
filtered_price_df = raw_price[(raw_price['date'] >= start_date) & (raw_price['date'] <= end_date)]
filtered_price_df.sort_values(['date', 'asset'], inplace=True)
price_df = filtered_price_df[['date', 'asset', 'cumulative_return']].pivot_table(index='date', columns='asset',
                                                                   values='cumulative_return',aggfunc='mean')
price_df = price_df.asfreq('C')
# print(price_df.index.freq)

print("价格数据:")
print(price_df)


'''
########################################################
# build 因子
# 2020-2021全年
# 2023全年
########################################################
'''
# 查询原始因子数据
raw_factor = pd.read_csv('../../代码与数据/db/gm/2023-01-01 TO 2023-12-31全品种T10展期收益率.csv')
# raw_factor2020 = pd.read_csv('../db/gm/2020-01-01 TO 2020-12-31全品种T10展期收益率.csv')
# raw_factor2021 = pd.read_csv('../db/gm/2021-01-01 TO 2021-12-31全品种T10展期收益率.csv')
# raw_factor = pd.concat([raw_factor2020, raw_factor2021], axis=0)
# 日期格式化
raw_factor.date = pd.to_datetime(raw_factor.date)
# 筛选日期在指定范围内的数据
raw_factor = raw_factor[(raw_factor['date'] >= start_date) & (raw_factor['date'] <= end_date)]


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
#因子品种在某个列表内
df = df[df['asset'].isin(codeList)]

filtered_price_df.set_index(['date', 'asset'], inplace=True)
df.set_index(['date', 'asset'], inplace=True,drop=False)
print("匹配前价格索引：",filtered_price_df.index)
print("匹配前因子索引：",df.index)
df = df.loc[filtered_price_df.index]
print("最新因子索引：",df.index)

dfnew = df['ratio'].copy()
df = dfnew.reset_index()
# 设置多重索引
df = df.sort_values(['date', 'asset'])
# factor_df = df.set_index(['date', 'asset'])
# print(factor_df)
factor_df_cp = df.copy()


'''
########################################################
# #因子去极值
'''
column_name = 'ratio'
win_factor_value = factor_df_cp.groupby('date').apply(lambda x: winsor(x[column_name]))
factor_df_cp['factor_wined'] = win_factor_value.values

# 因子标准化
factor_df_cp['factor_std'] = factor_df_cp.groupby('date').apply(lambda x: standize(x['factor_wined'])).values

# 不需要中性化的因子
factor_df_cp.rename({"factor_std": "factor"}, axis=1, inplace=True)
factor_df_noneg = factor_df_cp[['asset', 'date', 'factor']]
factor_flag = 1
factor_df = factor_df_noneg.set_index(['date', 'asset'])
factor_df = factor_flag * factor_df
print("因子数据:")
print(factor_df)
print("因子索引：",factor_df.index)


'''
########################################################
# #设置分组
'''
filtered_price_df = filtered_price_df.reset_index()
group_df = filtered_price_df[['date', 'asset', 'ind_code']].sort_values(['date', 'asset']).set_index(['date', 'asset'])
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
    group_df['ind_code'],
    quantiles=5,
    periods=(1,5,10),
    groupby_labels=sector_names
    )
print(factor_data)


#评估因子的信息系数（IC）和分析因子在不同分组间的表现
alphalens.tears.create_returns_tear_sheet(factor_data, long_short=True, group_neutral=False, by_group=True)

#
# alphalens.tears.create_turnover_tear_sheet(factor_data)

# 分组分析
# ic_by_sector = alphalens.performance.mean_information_coefficient(factor_data, by_group=True)
# ic_by_sector.head()
# alphalens.plotting.plot_ic_by_group(ic_by_sector)
# plt.pyplot.show()
