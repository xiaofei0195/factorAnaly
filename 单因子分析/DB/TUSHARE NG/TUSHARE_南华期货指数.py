import pandas as pd
import tushare as ts
ts.set_token('3a5d8df1d0f8bbbe5bd54664417eaf8b60f4bfea4a8551f2926f8019')
pro = ts.pro_api()

startStr = '20100101'
endStr = '20231101'

# 指定回测品种
codeList = ["V", "P", "B", "M", "I", "JD", "L", "PP", "Y", "C", "A", "J", "JM", "CS", "EG", "RR", "EB", "TA",
            "RM", "JR", "SR", "CF", "FG", "SF", "SM", "CY", "AP", "CJ", "UR", "SA", "FU", "SC", "AL", "RU", "ZN",
            "CU", "AU", "RB", "WR", "PB", "AG", "BU", "HC", "SN", "NI", "SP", "NR", "SS"]

HS = ['ZC', 'JM', 'J', 'I', 'RB', 'HC', 'SM', 'SF', 'WR']
NCP = ['A', 'B', 'PK', 'M', 'Y', 'P', 'RM', 'OI', 'SR', 'CF', 'CY', 'JD', 'LH', 'C', 'CS', 'AP', 'CJ', 'RR', 'RI', 'JR',
       'LR', 'WH', 'PM']
NH = ['EG', 'SC', 'BU', 'SP', 'SA', 'FG', 'L', 'MA', 'PP', 'EB', 'NR', 'UR', 'RU', 'FU', 'BB', 'V', 'PF', 'TA', 'LU',
      'PG', 'BR', 'PX', 'SH', 'EC']
YS = ['CU', 'AL', 'ZN', 'PB', 'NI', 'SN', 'SS', 'BC', 'AU', 'AG', 'AO', 'SI', 'LC']

TOTAL = HS + NCP + NH + YS

df = pd.DataFrame()
# 南华期货指数日线行情
for code in codeList:
    relCode = code+".NH"
    print(relCode)

    singleData = pro.index_daily(ts_code=relCode, start_date=startStr, end_date=endStr)
    singleData['code'] = code

    if code in HS:
        singleData['ind_code'] = 0
    if code in NCP:
        singleData['ind_code'] = 1
    if code in NH:
        singleData['ind_code'] = 2
    if code in YS:
        singleData['ind_code'] = 3
    if code not in TOTAL:
        singleData['ind_code'] = 9

    df = pd.concat([df, singleData], axis=0)

df.to_csv(startStr + " TO " + endStr + "南华期货指数日线行情.csv")
print("done!!!")