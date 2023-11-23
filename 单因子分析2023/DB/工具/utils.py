import pandas as pd

from 单因子分析.DB.工具 import constant


def getSymbol(code):
    symbol = ''
    if code in constant.SHFE_LIST:
        symbol = "SHFE." + code + "99"
    elif code in constant.CZCE_LIST:
        symbol = "CZCE." + code + "99"
    elif code in constant.DCE_LIST:
        symbol = "DCE." + code + "99"
    elif code in constant.INE_LIST:
        symbol = "INE." + code + "99"
    elif code in constant.GFEX_LIST:
        symbol = "GFEX." + code + "99"

    return symbol


def getSelectedSymbol(code, mon1):
    codeLower = code.lower()
    symbol = ''

    if code in constant.SHFE_LIST:
        symbol = "SHFE." + codeLower + '23' + mon1
    elif code in constant.CZCE_LIST:
        symbol = "CZCE." + code + '3' + mon1
    elif code in constant.DCE_LIST:
        symbol = "DCE." + codeLower + '23' + mon1
    elif code in constant.INE_LIST:
        symbol = "INE." + codeLower + '23' + mon1
    elif code in constant.GFEX_LIST:
        symbol = "GFEX." + codeLower + '23' + mon1

    return symbol


def winsor(ser):
    max_value, min_value = ser.mean() + 3 * ser.std(), ser.mean() - 3 * ser.std()
    return pd.Series([max_value if item > max_value else (min_value if item < min_value else item) for item in ser])


def standize(ser):
    return (ser - ser.mean()) / ser.std()

#
# if __name__ == '__main__':
#     for code in constant.CODE_LIST:
#         symbol = getSymbol(code)
#         print(code + ":" + symbol)


def winsor2(df, columStr):

    # 计算列的平均值
    mean_value = df[columStr].mean()

    # 计算列的标准差
    std_value = df[columStr].std()

    # 设置阈值，如果数值与平均值的差异超过两倍标准差，则置为平均值
    threshold = 2 * std_value

    # 找到需要置为平均值的行索引
    outliers = df[columStr].abs() - mean_value > threshold

    # 将数值显著偏离平均值的行置为平均值
    df.loc[outliers, columStr] = mean_value

def add_timezone(dt):
    return dt.replace(tzinfo=constant.tz)

def remove_timezone(dt):
    return dt.dt.tz_localize(None)

