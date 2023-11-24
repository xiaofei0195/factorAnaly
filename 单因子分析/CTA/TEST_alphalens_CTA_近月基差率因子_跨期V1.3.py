import alphalens
import numpy as np
import pandas as pd
from alphalens.utils import get_clean_factor_and_forward_returns

# 指定日期范围
from 单因子分析.DB.工具 import constant

# V1.0计算多07近月+空09远月，因子收益
# V1.1因子去极值和标准化，增加农产品因子分析
# V1.2价差收益修改为相对单边现货价格的收益
# V1.2.1增加回测月份为2-7月

startStr = '2023-03-01'
endStr = '2023-08-01'

start_date = pd.to_datetime(startStr)
end_date = pd.to_datetime(endStr)

# 指定回测品种
# codeList = ["V", "P", "B", "M", "I", "JD", "L", "PP", "Y", "C", "A", "J", "JM", "CS", "EG", "RR", "EB", "TA",
#             "RM", "JR", "SR", "CF", "FG", "SF", "SM", "CY", "AP", "CJ", "UR", "SA", "FU", "SC", "AL", "RU", "ZN",
#             "CU", "AU", "RB", "WR", "PB", "AG", "BU", "HC", "SN", "NI", "SP", "NR", "SS"]

codeList = ["CU","NI","SN","PB","AG","AL","ZN",
            "I","SS","RB","HC","SM","SF","JM","J",
            "A","P","Y","OI","M","RM","C","CS","CF","JD","LH",
            "PP","EG","PF","BU","L","SC","FU","LU","PG","RU","NR","FG","V","SA","MA","TA","UR","SP","EB",
            "B","SR","PX","BR","AO","SI","LC"]



def winsor(ser):
    max_value, min_value = ser.mean() + 3 * ser.std(), ser.mean() - 3 * ser.std()
    return pd.Series([max_value if item > max_value else (min_value if item < min_value else item) for item in ser])


def standize(ser):
    return (ser - ser.mean()) / ser.std()

# 定义函数用于处理每个分组
def set_shift_row_to_zero(group):
    group['shift'].iloc[0] = 0
    return group

# 定义函数用于处理每个分组
def set_daily_return_to_zero(group):
    group['daily_return'].iloc[0] = 0
    return group


if __name__ == '__main__':
    '''
    ########################################################
    # build 价格 跨期价差
    # 限定品种【黑色】、日期【0401-0630】
    ########################################################
    '''
    raw_price = pd.read_csv('../DB/CSV导出/跨期价差/MONGO_08_跨期价差.csv')
    raw_price['date'] = pd.to_datetime(raw_price['date'])
    raw_price.rename({"code": "asset"}, axis=1, inplace=True)
    # 数据筛选 品种+日期
    raw_price = raw_price[raw_price['asset'].isin(codeList)]
    raw_price = raw_price[(raw_price['date'] >= startStr) & (raw_price['date'] <= endStr)]

    # 指定近月合约价格
    # 读取指定日期区间、指定品种
    monPriceDf = pd.read_csv('../DB/CSV导出/指定合约价格/掘金_08合约价格2023-01-01TO2023-08-05.csv')
    monPriceDf['date'] = pd.to_datetime(monPriceDf['date'])
    monPriceDf.rename({"code": "asset"}, axis=1, inplace=True)
    monPriceDf = monPriceDf[monPriceDf['asset'].isin(codeList)]
    filteredMonPriceDf = monPriceDf[(monPriceDf['date'] >= startStr) & (monPriceDf['date'] <= endStr)]

    # 按照日期和品种合并两个DataFrame
    merged_df = pd.merge(raw_price, filteredMonPriceDf, on=['date', 'asset'])
    merged_df.rename(columns={"ind_code_x": "ind_code"}, inplace=True)
    raw_price = merged_df.loc[:, ["date", "spread", "asset", "ind_code", "close"]]
    # raw_price['spread_raito'] = raw_price['spread'] / raw_price['close']

    # 价格去极值
    # 计算每个品种的平均值
    mean_values = raw_price.groupby('asset')['spread'].transform('mean')
    # 计算每个品种的标准差
    std_values = raw_price.groupby('asset')['spread'].transform('std')
    # 设置阈值，如果数值与平均值的差异超过两倍标准差，则置为平均值
    threshold = 2 * std_values
    # 找到需要置为平均值的行索引
    outliers = np.abs(raw_price['spread'] - mean_values) > threshold
    # 将数值显著偏离平均值的行置为平均值
    raw_price.loc[outliers, 'spread'] = mean_values

    # 计算累计收益
    raw_price['shift'] = raw_price.groupby('asset')['spread'].diff()  # 当日价差减去上一日期价差数据
    raw_price['daily_return'] = raw_price['shift'] / raw_price['close'].shift(1)  # 结果除以上一日期的价格列数据

    # 按照日期列和品种列对DataFrame进行分组，并应用自定义函数
    raw_price = raw_price.groupby(['asset']).apply(set_shift_row_to_zero)
    raw_price = raw_price.groupby(['asset']).apply(set_daily_return_to_zero)


    # 收益去极值
    # 计算每个品种的平均值
    mean_values_return = raw_price.groupby('asset')['daily_return'].transform('mean')
    # 计算每个品种的标准差
    std_values_return = raw_price.groupby('asset')['daily_return'].transform('std')
    # 设置阈值，如果数值与平均值的差异超过两倍标准差，则置为平均值
    threshold_return = 2 * std_values_return
    # 找到需要置为平均值的行索引
    outliers_return = np.abs(raw_price['daily_return'] - mean_values_return) > threshold_return
    # 将数值显著偏离平均值的行置为平均值
    raw_price.loc[outliers_return, 'daily_return'] = mean_values_return

    # raw_price['daily_return'] = raw_price.groupby('asset')['spread'].pct_change()
    raw_price['cumulative_return'] = raw_price.groupby('asset')['daily_return'].expanding().apply(
        lambda x: np.prod(x + 1)).values

    # 构建价格数据【alphalens】
    raw_price.sort_values(['date', 'asset'], inplace=True)
    price_df = raw_price[['date', 'asset', 'cumulative_return']].pivot(index='date', columns='asset',
                                                                       values='cumulative_return')


    price_df = price_df.asfreq('C')
    print("价格数据:")
    print(price_df)



    '''
    ########################################################
    # build 因子
    ########################################################
    '''
    # 优化读取和合并csv文件
    file_paths = [
        '../FACTOR/2023-03-01 TO 2023-07-30近月基差率因子08_ALL.csv'
    ]
    factor_raw = pd.concat([pd.read_csv(file_path) for file_path in file_paths], axis=0)
    factor_raw['date'] = pd.to_datetime(factor_raw['date'])
    factor_raw.rename({"code": "asset"}, axis=1, inplace=True)
    factor_raw.sort_values(['date', 'asset'], inplace=True)
    factor_df = factor_raw[['date', 'asset', 'basisRatio']]

    '''
    ########################################################
    # 因子去极值、标准化
    ########################################################
    '''
    column_name = 'basisRatio'
    factor_df['factor_wined'] = factor_df.groupby('date')[column_name].transform(winsor)

    # 因子标准化
    factor_df['factor_std'] = factor_df.groupby('date').apply(lambda x: standize(x['factor_wined'])).values
    # 不需要中性化的因子
    factor_df.rename({"factor_std": "factor"}, axis=1, inplace=True)
    factor_df_noneg = factor_df[['asset', 'date', 'factor']]
    factor_flag = 1
    factor_df = factor_df_noneg.set_index(['date', 'asset'])
    factor_df = factor_flag * factor_df

    print("因子数据:")
    print(factor_df)
    print("因子索引：", factor_df.index)

    '''
    ########################################################
    # nan值处理
    ########################################################
    '''
    # 因子nan或inf处理
    for i in range(0, len(factor_df)):
        if pd.isna(factor_df.iloc[i, 0]) or np.isinf(factor_df.iloc[i, 0]):
            factor_df.iloc[i, 0] = factor_df.iloc[i - 10:i, 0].mean()

    # 价格nan或inf处理
    # 遍历所有列，将nan和inf替换为对应行的前10行平均值
    for j, col_index in enumerate(price_df.columns):
        for i in range(0, len(price_df)):
            if pd.isna(price_df.iloc[i, j]) or np.isinf(price_df.iloc[i, j]):
                price_df.iloc[i, j] = price_df.iloc[i - 10:i - 1, j].mean()

    '''
    ########################################################
    # #设置分组
    ########################################################
    '''
    filtered_price_df = raw_price.reset_index()
    group_df = filtered_price_df[['date', 'asset', 'ind_code']].sort_values(['date', 'asset']).set_index(
        ['date', 'asset'])
    print("分组数据:")
    print(group_df)

    '''
    ########################################################
    # 检查索引是否存在重复值
    ########################################################
    '''
    # if factor_df.index.duplicated().any():
    #     # 重置索引并添加唯一标识符
    #     factor_df = factor_df.reset_index(drop=True)
    #
    # # 执行重新索引
    # factor_df = factor_df.reindex(range(len(factor_df)))
    # factor_df = factor_df.set_index(['date', 'asset'])
    # print("因子最新:")
    # print(factor_df)

    '''
    ########################################################
    # 数据预处理
    ########################################################
    '''
    factor_data = get_clean_factor_and_forward_returns(
        factor_df,
        price_df,
        group_df['ind_code'],
        quantiles=6,
        periods=(1, 5, 10, 20),
        filter_zscore=20,
        zero_aware=True,
        max_loss= 0.5,
        groupby_labels=constant.sector_names
    )
    print(factor_data)

    for column in factor_data.columns[:3]:
        mean_value = factor_data[column].replace(np.inf, np.nan).mean()
        factor_data[column].replace(np.inf, mean_value, inplace=True)

    # 评估因子的信息系数（IC）和分析因子在不同分组间的表现
    # alphalens.tears.create_information_tear_sheet(factor_data, group_neutral=False, by_group=True)
    alphalens.tears.create_returns_tear_sheet(factor_data, long_short=False, group_neutral=False, by_group=True)