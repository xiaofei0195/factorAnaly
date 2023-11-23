import alphalens
import numpy as np
import pandas as pd
from alphalens.utils import get_clean_factor_and_forward_returns

# V1.1计算当日累计收益
# V1.2回测全年数据
# V1.3修改算法，计算每日涨幅和累计涨幅
# V1.4从掘金获取T10展期收益率
# V1.5修改格式化数据索引
# V1.5.1掘金数据2023年
# V1.5.2价格数据源替换为指数价格
# V1.5.3回测年限设为2020-2023年

# 指定日期范围
start_date = pd.to_datetime('2020-01-01')
end_date = pd.to_datetime('2023-11-05')

sector_names = {
    0: "hei se",
    1: "nong chan pin",
    2: "neng hua",
    3: "you se",
    9: "qi ta",
}

# 指定回测品种
codeList = ["V", "P", "B", "M", "I", "JD", "L", "PP", "Y", "C", "A", "J", "JM", "CS", "EG", "RR", "EB", "TA",
            "RM", "JR", "SR", "CF", "FG", "SF", "SM", "CY", "AP", "CJ", "UR", "SA", "FU", "SC", "AL", "RU", "ZN",
            "CU", "AU", "RB", "WR", "PB", "AG", "BU", "HC", "SN", "NI", "SP", "NR", "SS"]


def winsor(ser):
    max_value, min_value = ser.mean() + 3 * ser.std(), ser.mean() - 3 * ser.std()
    return pd.Series([max_value if item > max_value else (min_value if item < min_value else item) for item in ser])


def standize(ser):
    return (ser - ser.mean()) / ser.std()


if __name__ == '__main__':
    '''
    ########################################################
    # build 价格
    ########################################################
    '''

    raw_price = pd.read_csv('../DB/CSV导出/商品指数/AKSHARE全品种指数价格信息.csv')
    raw_price = raw_price[raw_price['date'].str.contains('2020|2021|2022|2023')]
    raw_price = raw_price[raw_price['code'].isin(codeList)]
    raw_price['date'] = pd.to_datetime(raw_price['date'])
    raw_price.rename({"value": "close", "code": "asset"}, axis=1, inplace=True)
    raw_price['daily_return'] = raw_price.groupby('asset')['close'].pct_change()
    raw_price['cumulative_return'] = raw_price.groupby('asset')['daily_return'].expanding().apply(
        lambda x: np.prod(x + 1)).values
    # 筛选日期在指定范围内的数据
    filtered_price_df = raw_price[(raw_price['date'] >= start_date) & (raw_price['date'] <= end_date)]
    filtered_price_df.sort_values(['date', 'asset'], inplace=True)
    price_df = filtered_price_df[['date', 'asset', 'cumulative_return']].pivot_table(index='date', columns='asset',
                                                                                     values='cumulative_return',
                                                                                     aggfunc='mean')
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
        '../FACTOR/2020-01-01 TO 2023-11-05稳健动量因子.csv'
    ]
    factor_df = pd.concat([pd.read_csv(file_path) for file_path in file_paths], axis=0)
    factor_df['date'] = pd.to_datetime(factor_df['date'])

    '''
    ########################################################
    # 因子去极值、标准化
    ########################################################
    '''
    print(factor_df)
    column_name = 'roll_mean_val'
    win_factor_value = factor_df.groupby('date').apply(lambda x: winsor(x[column_name]))
    factor_df['factor_wined'] = win_factor_value.values
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
    # mean_value = factor_df['factor'].mean()
    # factor_df['factor'].fillna(mean_value, inplace=True)
    for i in range(0, len(factor_df)):
        if pd.isna(factor_df.iloc[i, 0]) or np.isinf(factor_df.iloc[i, 0]):
            factor_df.iloc[i, 0] = factor_df.iloc[i-10:i, 0].mean()


    # 价格nan或inf处理
    # for column in price_df.columns:
    #     mean_value = price_df[column].mean()
    #     price_df[column].fillna(mean_value, inplace=True)
    # 遍历所有列，将nan和inf替换为对应行的前10行平均值
    for j, col_index in enumerate(price_df.columns):
        for i in range(0, len(price_df)):
            if pd.isna(price_df.iloc[i, j]) or np.isinf(price_df.iloc[i, j]):
                price_df.iloc[i, j] = price_df.iloc[i-10:i-1, j].mean()


    '''
    ########################################################
    # #设置分组
    ########################################################
    '''
    filtered_price_df = filtered_price_df.reset_index()
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
        quantiles=5,
        periods=(1,5,10),
        filter_zscore=20,
        groupby_labels=sector_names
    )
    print(factor_data)

    for column in factor_data.columns[:3]:
        mean_value = factor_data[column].replace(np.inf, np.nan).mean()
        factor_data[column].replace(np.inf, mean_value, inplace=True)

    # 评估因子的信息系数（IC）和分析因子在不同分组间的表现
    # alphalens.tears.create_information_tear_sheet(factor_data,group_neutral=False, by_group=False)
    alphalens.tears.create_returns_tear_sheet(factor_data, long_short=True, group_neutral=True, by_group=True)
