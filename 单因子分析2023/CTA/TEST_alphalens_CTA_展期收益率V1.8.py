import alphalens
import numpy as np
import pandas as pd
from alphalens.utils import get_clean_factor_and_forward_returns

from 单因子分析.DB.工具 import constant, utils

'''
版本信息
# V1.1计算当日累计收益
# V1.2回测全年数据
# V1.3修改算法，计算每日涨幅和累计涨幅
# V1.4从掘金获取T10展期收益率
# V1.5修改格式化数据索引
# V1.5.1掘金数据2023年
# V1.5.2价格数据源替换为指数价格
# V1.5.3回测年限设为2020-2023年
# V1.6回测数据源为南华商品指数tushare
# V1.7回测数据源为商品加权指数掘金
# V1.8修改价格dataframe
'''

'''
数据源：
价格：掘金 商品加权指数日线行情
因子：掘金T10公式计算 全品种T10展期收益率

分析工具：alphalens
回测时间：2020-2023年
'''

start_date = '2020-01-01'
end_date = '2023-01-01'

if __name__ == '__main__':
    '''
    ########################################################
    # build 价格  掘金
    ########################################################
    '''
    # 格式化输入价格数据
    raw_price = pd.read_csv('../DB/CSV导出/商品指数/掘金_商品加权指数2010-01-01TO2023-10-01.csv')
    raw_price.rename({"code": "asset"}, axis=1, inplace=True)
    raw_price['date'] = pd.to_datetime(raw_price['date'])
    raw_price = raw_price[raw_price['asset'].isin(constant.CODE_LIST)]

    # 筛选日期在指定范围内的数据
    filtered_price_df = raw_price[(raw_price['date'] >= start_date) & (raw_price['date'] <= end_date)]
    filtered_price_df['date'] = pd.to_datetime(filtered_price_df['date'])
    filtered_price_df['date'] = filtered_price_df['date'].dt.tz_localize(None)

    filtered_price_df.sort_values(['date', 'asset'], inplace=True)
    price_df = filtered_price_df[['date', 'asset', 'close']].pivot_table(index='date', columns='asset',
                                                                                     values='close',
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
        '../DB/CSV导出/掘金展期收益率【主次计算】/2020-01-01 TO 2020-12-31全品种T10展期收益率.csv',
        '../DB/CSV导出/掘金展期收益率【主次计算】/2021-01-01 TO 2021-12-31全品种T10展期收益率.csv',
        '../DB/CSV导出/掘金展期收益率【主次计算】/2022-01-01 TO 2022-12-31全品种T10展期收益率.csv',
        '../DB/CSV导出/掘金展期收益率【主次计算】/2023-01-01 TO 2023-12-31全品种T10展期收益率.csv'
    ]
    raw_factor = pd.concat([pd.read_csv(file_path) for file_path in file_paths], axis=0)
    raw_factor.rename({"underlying_symbol_y": "asset", "factor": "ratio"}, axis=1, inplace=True)
    # 日期格式化和筛选日期范围
    raw_factor['date'] = pd.to_datetime(raw_factor['date'])
    raw_factor = raw_factor[(raw_factor['date'] >= start_date) & (raw_factor['date'] <= end_date)]
    # 获取所需数据并创建DT
    df = raw_factor[['asset', 'ratio', 'date']].copy()
    df['asset'] = df['asset'].str.split('.').str[1]
    df = df[df['asset'].isin(constant.CODE_LIST)]
    # 价格索引和因子索引匹配剔除无效数据
    filtered_price_df.set_index(['date', 'asset'], inplace=True)
    df.set_index(['date', 'asset'], inplace=True, drop=False)
    print("匹配前价格索引：", filtered_price_df.index)
    print("匹配前因子索引：", df.index)
    df = df.loc[filtered_price_df.index]
    # 输出结果
    print("最新因子索引：", df.index)
    dfnew = df['ratio'].copy()
    df = dfnew.reset_index()
    df = df.sort_values(['date', 'asset'])
    factor_df_cp = df.copy()

    '''
    ########################################################
    # 因子去极值、标准化
    ########################################################
    '''
    factor_df = factor_df_cp.set_index(['date', 'asset'])
    column_name = 'ratio'
    win_factor_value = factor_df_cp.groupby('date').apply(lambda x: utils.winsor(x[column_name]))
    factor_df_cp['factor_wined'] = win_factor_value.values
    # 因子标准化
    factor_df_cp['factor_std'] = factor_df_cp.groupby('date').apply(lambda x: utils.standize(x['factor_wined'])).values
    # 不需要中性化的因子
    factor_df_cp.rename({"factor_std": "factor"}, axis=1, inplace=True)
    factor_df_noneg = factor_df_cp[['asset', 'date', 'factor']]
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
            factor_df.iloc[i, 0] = factor_df.iloc[i - 10:i, 0].mean()

    # 价格nan或inf处理
    # for column in price_df.columns:
    #     mean_value = price_df[column].mean()
    #     price_df[column].fillna(mean_value, inplace=True)
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
    filtered_price_df = filtered_price_df.reset_index()
    group_df = filtered_price_df[['date', 'asset', 'ind_code']].sort_values(['date', 'asset']).set_index(
        ['date', 'asset'])
    print("分组数据:")
    print(group_df)

    '''
    ########################################################
    # 数据预处理
    ########################################################
    '''
    factor_data = get_clean_factor_and_forward_returns(
        factor_df,
        price_df,
        groupby=group_df['ind_code'],
        # binning_by_group=False,
        quantiles=10,
        bins=None,
        periods=(1, 5, 10),
        filter_zscore=20,
        groupby_labels=constant.sector_names,
        max_loss=0.35,
        zero_aware=True,
        cumulative_returns=True)

    print(factor_data)
    for column in factor_data.columns[:3]:
        mean_value = factor_data[column].replace(np.inf, np.nan).mean()
        factor_data[column].replace(np.inf, mean_value, inplace=True)

    # 评估因子的信息系数（IC）和分析因子在不同分组间的表现
    alphalens.tears.create_information_tear_sheet(factor_data,group_neutral=True, by_group=True)
    alphalens.tears.create_returns_tear_sheet(factor_data, long_short=True, group_neutral=True, by_group=True)
