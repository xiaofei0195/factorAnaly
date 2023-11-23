import pandas as pd

# from pandas.tseries.offsets import BDay
#
# # 创建日期序列
# dates = pd.date_range(start='2022-01-01', end='2022-01-10', freq='D')
#
# # 创建价差序列
# spreads = [1.2, 2.5, 0.8, 1.3, 0.5, 1.0, 2.2, 1.9, 0.7, 1.5]
#
# # 构造DataFrame
# df = pd.DataFrame({'date': dates, 'spread': spreads})
#
#
# # 指定日期
# date = '2022-01-11'  # 指定日期
# spread = df.loc[df['date'] == date, 'spread'].values
# if len(spread) == 0:  # 如果找不到指定日期的价差
#     # 找到当前日期往前10个工作日的最后一个日期列表
#     business_dates = pd.date_range(end=date, periods=3, freq=BDay())
#     # 筛选最后一个日期的价差
#     spread = df.loc[df['date'].isin(business_dates), 'spread'].values
#     print(spread)
from utils.other import comUtils

df = pd.read_csv("C:/Users/H3C/Desktop/warehouse_df.csv")
print(df)
df['tradingDay'] = pd.to_datetime(df['tradingDay'])



# 按年份分组
df_grouped = df.groupby(df['tradingDay'].dt.year)
# 遍历每个分组，生成不同的DataFrame
dfs = []
for group_name, group_df in df_grouped:
    dfs.append(group_df)


# 输出每个年份对应的DataFrame
tradingDay = '2023-11-14'
month_day = tradingDay[5:]
wareList = []

for i, df_year in enumerate(dfs):
    year = str(df_year['tradingDay'].dt.year.unique()[0])
    curDay = year + "-" + month_day
    value = 0

    if curDay in df_year['tradingDay'].dt.strftime('%Y-%m-%d').tolist():
        value = df_year.loc[df_year['tradingDay'] == curDay, 'value'].values[0]
    else:
        value = comUtils.calAverage(df_year, "value", curDay, 2)

    dic = {curDay:value}
    wareList.append(dic)

print(wareList)