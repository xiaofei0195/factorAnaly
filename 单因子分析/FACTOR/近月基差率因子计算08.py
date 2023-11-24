import pandas as pd

# 指定日期范围
from 单因子分析.DB.工具 import constant

startStr = '2023-03-01'
endStr = '2023-07-30'

# start_date = pd.to_datetime(startStr)
# end_date = pd.to_datetime(endStr)

# 指定回测品种
# codeList = ["V", "P", "B", "M", "I", "JD", "L", "PP", "Y", "C", "A", "J", "JM", "CS", "EG", "RR", "EB", "TA",
#             "RM", "JR", "SR", "CF", "FG", "SF", "SM", "CY", "AP", "CJ", "UR", "SA", "FU", "SC", "AL", "RU", "ZN",
#             "CU", "AU", "RB", "WR", "PB", "AG", "BU", "HC", "SN", "NI", "SP", "NR", "SS"]
codeList = constant.NH + constant.NCP + constant.NH + constant.YS

'''
举例:
当前2023年4月1日
因子：
计算07合约近月基差率 = （07合约现货折盘面价格 - 07合约期货价格）/ 07合约期货价格
收益：07-09跨期组合价差
'''
# 现货折盘面价格
# 读取指定日期区间、指定品种
# filename = '../DB/CSV导出/现货折盘面/黑色/XX_现货折盘面价格.csv'
# filename = '../DB/CSV导出/现货折盘面/农产品/XX_现货折盘面价格.csv'
# filename = '../DB/CSV导出/现货折盘面/有色/XX_现货折盘面价格.csv'
# filename = '../DB/CSV导出/现货折盘面/能化/XX_现货折盘面价格.csv'
filename = '../DB/CSV导出/现货折盘面/YY/XX_现货折盘面价格.csv'
target = 'XX'
target2 = 'YY'

nameList = ["黑色","农产品","有色","能化"]

cashPriceDf = pd.DataFrame()
for name in nameList:
    filename2 = filename.replace(target2, name)
    if name == "黑色":
        codeList = constant.HS
    if name == "农产品":
        codeList = constant.NCP
    if name == "有色":
        codeList = constant.YS
    if name == "能化":
        codeList = constant.NH

    for item in codeList:
        modified_filename = filename2.replace(target, item)
        print(modified_filename)
        singleData = pd.read_csv(modified_filename)
        cashPriceDf = pd.concat([cashPriceDf, singleData], axis=0)

cashPriceDf['date'] = pd.to_datetime(cashPriceDf['date'])
cashPriceDf = cashPriceDf[cashPriceDf['code'].isin(constant.CODE_LIST)]
filteredCashPriceDf = cashPriceDf[(cashPriceDf['date'] >= startStr) & (cashPriceDf['date'] <= endStr)]

# 指定近月合约价格
# 读取指定日期区间、指定品种
monPriceDf = pd.read_csv('../DB/CSV导出/指定合约价格/掘金_08合约价格2023-01-01TO2023-08-05.csv')
monPriceDf['date'] = pd.to_datetime(monPriceDf['date'])
monPriceDf = monPriceDf[monPriceDf['code'].isin(constant.CODE_LIST)]
filteredMonPriceDf = monPriceDf[(monPriceDf['date'] >= startStr) & (monPriceDf['date'] <= endStr)]

# 按照日期和品种合并两个DataFrame
merged_df = pd.merge(filteredCashPriceDf, filteredMonPriceDf, on=['date', 'code'])

# 计算合约近月基差率
merged_df['basisRatio'] = (merged_df['cashPrice'] - merged_df['close'])/merged_df['cashPrice']
factor_df = merged_df.loc[:,['date','code','basisRatio']]
factor_df.sort_values(['date', 'code'], inplace=True)
# factor_df.to_csv(startStr + " TO " + endStr + "近月基差率因子_农产品.csv")
# factor_df.to_csv(startStr + " TO " + endStr + "近月基差率因子_有色.csv")
# factor_df.to_csv(startStr + " TO " + endStr + "近月基差率因子_能化.csv")
factor_df.to_csv(startStr + " TO " + endStr + "近月基差率因子08_ALL.csv")
print(factor_df)



