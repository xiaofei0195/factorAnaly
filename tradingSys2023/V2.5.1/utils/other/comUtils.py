# -*- coding:utf-8 -*-
import os
import sys

from pandas.tseries.offsets import BDay

# 把当前文件所在文件夹的父文件夹路径加入到PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd
import datetime
import time
from bs4 import BeautifulSoup  # 一个解析库，用来解析网页结构
import requests


def fetch_data_from_api(url, product_code):
    try:
        r = requests.get(url + product_code)
        return r.json()['data']
    except Exception as e:
        return None


def fetch_data_from_api_post(url, data):
    try:
        r = requests.post(url, data)
        return r.json()['data']
    except Exception as e:
        return None


'''
计算期限结构
'''


def calculate_term_structure(df, columeName):
    prices = df[columeName]
    if len(prices) >= 4:
        if all(prices[i] > prices[i + 1] for i in range(3)):
            term_structure = 'BACK'
        elif all(prices[i] < prices[i + 1] for i in range(3)):
            term_structure = 'CONT'
        else:
            term_structure = 'OTHER'
    else:
        term_structure = 'OTHER'
    return term_structure


'''
计算基差动量
'''


def calculate_price_slop(df, columeName):
    prices = df[columeName]
    if len(prices) >= 2:
        diff = prices[0] - prices[1]
        ratio = diff / prices[1]
        return ratio
    else:
        return None
    return None


'''
计算指定日期前数据的平均值
'''


def calAverage(df, column, tradingDay, periods):
    business_dates = pd.date_range(end=tradingDay, periods=periods, freq=BDay())
    subset = df[df['tradingDay'].isin(business_dates)]
    average = subset[column].mean()
    return average


# 查询现货价格（通过生意社期货现表）
def getCashPriceBySYS(tradingDay, paramList):
    r = requests.get('http://www.100ppi.com/sf2/day-' + tradingDay + '.html')
    retData = r.text

    bs = BeautifulSoup(retData, "html.parser")  # 解析网页
    table = bs.find_all("tr")  # 定位信息

    productList = []  # [{品种名称，现货价格},{}]结果集
    for i in range(len(table)):
        productDic = {}  # {品种名称，现货价格}

        trStr = table[i].text.strip()
        if len(trStr) > 50 and not trStr.startswith('http'):
            trList = trStr.split('\n')
            trNewList = []
            for td in trList:
                tdNew = td.replace('\xa0', '')
                trNewList.append(tdNew)
            productName = trNewList[0]
            cashPrice = trNewList[1]

            if productName == '涤纶短纤':
                productName = '短纤'
            if productName == '低硫燃料油':
                productName = '低燃油'
            if productName == '液化石油气':
                productName = '液化气'
            if productName == '石油沥青':
                productName = '沥青'
            if productName == '燃料油':
                productName = '燃油'
            if productName == '天然橡胶':
                productName = '橡胶'
            if productName == '甲醇MA':
                productName = '甲醇'
            if productName == '聚氯乙烯':
                productName = 'PVC'
            if productName == '聚乙烯':
                productName = '塑料'

            if productName == '铜':
                productName = '沪铜'
            if productName == '锌':
                productName = '沪锌'
            if productName == '铜':
                productName = '沪铜'
            if productName == '铝':
                productName = '沪铝'
            if productName == '铅':
                productName = '沪铅'
            if productName == '镍':
                productName = '沪镍'
            if productName == '白银':
                productName = '沪银'
            if productName == '锡':
                productName = '沪锡'

            if productName == '棕榈油':
                productName = '棕榈'
            if productName == '玉米淀粉':
                productName = '淀粉'
            if productName == '菜籽粕':
                productName = '菜粕'
            if productName == '菜籽油OI':
                productName = '菜油'

            # if productName == '铁矿石':
            #     productName = '铁矿'
            if productName == '螺纹钢':
                productName = '螺纹'
            if productName == '热轧卷板':
                productName = '热卷'

            selectList = []
            if paramList != []:
                selectList = ['聚丙烯', '乙二醇', '短纤', '塑料', '豆一', '菜粕', '油菜籽', '鸡蛋', '生猪', '棉纱', '菜油']
            else:
                selectList = ['沪铜', '沪镍', '沪锡', '沪铅', '沪银', '沪铝', '沪锌', '铁矿石', '不锈钢', '螺纹', '热卷', '锰硅', '硅铁', '焦煤',
                              '焦炭',
                              '豆一', '棕榈', '豆油', '菜油', '豆粕', '菜粕', '玉米', '淀粉', '棉花', '油菜籽', '鸡蛋', '生猪', '棉纱', '聚丙烯',
                              '乙二醇', '短纤', '沥青', '塑料', '原油', '燃油', '低燃油', '液化气', '橡胶', '20号胶', '玻璃', 'PVC', '纯碱', '甲醇',
                              'PTA', '尿素', '纸浆', '苯乙烯']
            if productName in selectList:
                productDic['productName'] = productName
                if productName == '鸡蛋':
                    cashPrice = float(cashPrice) * 500
                if productName == '玻璃':
                    cashPrice = float(cashPrice) * 79.98
                productDic['cashPrice'] = cashPrice
                productList.append(productDic)
    # print('查询现货价格（通过生意社期货现表）：', productList)
    return productList


# 查询现货价格（通过交易法门基差日报）指定品种
def getCashPriceByJYFM(tradingDay):
    r = requests.post('https://www.jiaoyifamen.com/tools/api//future-basis/daily', data={'day': tradingDay})
    retData = r.json()

    # 获取现货价格
    data = retData['data']
    table_data = data['table_data']

    productList = []
    for item in table_data:

        productName = item['productName']
        if productName == '低硫燃料油':
            productName = '低燃油'
        if productName == '液化石油气':
            productName = '液化气'

        cashDic = {}
        cashDic['productName'] = productName
        cashDic['cashPrice'] = item['cashPrice']
        productList.append(cashDic)
    # print('查询现货价格（通过交易法门基差日报）：', productList)
    return productList


# 查询近月远月价差
def getDailyFutureSpread(tradingDay, type1, code1, type2, code2):
    r = requests.post('https://www.jiaoyifamen.com/tools/api//future/spread/free',
                      data={'type1': type1, 'code1': code1, 'type2': type2, 'code2': code2})
    retData = r.json()

    # 获取日期集合
    data = retData['data']
    dataCategory = data['dataCategory']
    codeNew = ''
    if (code1[0] == '0'):
        codeNew = code1.replace('0', '')
    else:
        codeNew = code1
        # print('codeNew: ',codeNew)

    # 通过当前日期，获取指定日期下标
    dayExlYear = tradingDay[-5:]
    dataIndex = dataCategory.index(dayExlYear)

    yearData = 0
    if int(codeNew) > 5:
        # 获取2022年价差集合
        # print('获取2022年价差集合')
        yearData = data['year2022']
    else:
        # 获取2023年价差集合
        yearData = data['year2023']

    retDic = {}

    # 通过日期下标，获取价差
    spread = yearData[dataIndex]
    # print('spread: ',spread)
    retDic['spread'] = spread  # 保存价差

    recentPrice = data['recentPrice']
    length = len(recentPrice)
    lastPrice = recentPrice[length - 1]
    # print('lastPrice: ',lastPrice)
    retDic['lastPrice'] = lastPrice  # 保存近月期货价格

    return retDic


# 通过name查找code
def getCodeByName(name):
    for product in productNameAndCodeList:
        if product['productName'] == name:
            code = product['productCode']
            # print('get 品种编码code：' , code , ' by name：',name)
            return code

    return ''


# 通过name查找code
def getNameByCode(code):
    for product in productNameAndCodeList:
        if product['productCode'] == code:
            name = product['productName']
            # print('get 品种编码code：' , code , ' by name：',name)
            return name

    return ''


# 通过name查找现货价格
def getCashPriceByName(name, cashList):
    for cashInfo in cashList:
        if cashInfo['productName'] == name:
            cashPrice = cashInfo['cashPrice']
            # print('get 现货价格cashPrice：' , cashPrice , ' by name：',name)
            return cashPrice
    return ''


# 查询期限结构（奇货可查）
def getStructure():
    url = 'https://www.qhkch.com/ajax/arbitrage_fut_spot_structure_all.php'
    headers = {
        'Cookie': 'remember=6b2e8d701dd87b593759e1b50326328b; Hm_lvt_c54eb5f0c700b7d446674a77b06c4d24=1666776704,1667036232,1667038635; Hm_lpvt_c54eb5f0c700b7d446674a77b06c4d24=1667038635; PHPSESSID=o7t4dao4tafpvjtlmdjlk5hc6t'}
    r = requests.post(url, headers=headers)
    retData = r.json()
    print("retData:", retData)

    nameAndCodeAndStruList = []

    # 获得品种编码code集合
    for productNameAndCode in productNameAndCodeList:
        nameAndCodeAndStru = {}

        productCode = productNameAndCode['productCode']
        productName = productNameAndCode['productName']
        nameAndCodeAndStru['productCode'] = productCode
        nameAndCodeAndStru['productName'] = productName

        # 分析期限结构，为back或contango
        spark_data = retData['spark_data']

        priceSet = spark_data[productCode]
        if len(priceSet) > 2:
            price1 = float(priceSet[0])
            price2 = float(priceSet[1])
            price3 = float(priceSet[2])
            if price1 > price2 and price2 > price3:
                structure = 'BACK'
            elif price1 < price2 and price2 < price3:
                structure = 'CONT'
            else:
                structure = 'OTHER'
        else:
            structure = 'DATA MISSING'
        nameAndCodeAndStru['structure'] = structure
        nameAndCodeAndStruList.append(nameAndCodeAndStru)

        condition = {}
        condition['productCode'] = productCode
        data = {}
        data['structure'] = structure
        dailyBaseInfo.update_to_mongodb(condition, data)

    return nameAndCodeAndStruList


# 查询期限结构（交易法门）
def getStructureByJYFM(type, day):
    tradingDay = day
    # 通过code查询name
    productCode = type
    productName = getNameByCode(productCode)

    url = 'https://www.jiaoyifamen.com/tools/api//future/basis/structure?t=1667631967944'
    try:
        r = requests.post(url, data={'type': type, 'day': day})
    except Exception as e:
        errorMsg = '品种：' + productName + '，查询期限结构（交易法门），接口异常！' + str(e)
        print(errorMsg)
        return

    retData = r.json()
    # print('retData: ',retData)

    # 分析期限结构，为back或contango
    data = retData['data']
    # print('data: ',data)

    # 转实时计算
    structure = ''
    slope = 1
    priceSet = data['todayData']
    if len(priceSet) > 3:
        price1 = float(priceSet[0]['closePrice'])
        price2 = float(priceSet[1]['closePrice'])
        price3 = float(priceSet[2]['closePrice'])
        price4 = float(priceSet[3]['closePrice'])
        # print(str(price1) + '/' + str(price2) + '/' + str(price3) + '/' + str(price4))
        if price1 > price2 and price2 > price3:  # and price3 > price4:
            structure = 'BACK'
        elif price1 < price2 and price2 < price3:  # and price3 < price4:
            structure = 'CONT'
        else:
            structure = 'OTHER'

        # 计算曲线倾斜度 （近月-远月）/近月
        slope = float(format((price1 - price3) / price1, '.2f'))

    else:
        structure = 'DATA MISSING'

    # 保存当日期限结构
    condition = {}
    condition['tradingDay'] = tradingDay
    condition['productCode'] = productCode
    retStrucData = dailyStruc.find_one_from_mongodb(condition)

    data = {}
    if (retStrucData == None):
        # print('保存品种: ' + productName + '\t\t' + productCode + '\t\t' + structure + '\t\t' + str(
        #     slope) + '\t\t' + tradingDay + ',到每日期限结构表')
        data['productName'] = productName
        data['productCode'] = productCode
        data['structure'] = structure
        data['slope'] = slope
        data['tradingDay'] = tradingDay
        dailyStruc.insert_one_to_mongodb(data)
        return data

    return data


# 查询基差原始数据（交易法门）
def getBasisByJYFM(type):
    # 通过code查询name
    productCode = type
    productName = getNameByCode(productCode)

    try:
        r = requests.get('https://www.jiaoyifamen.com/tools/api/future-basis/query?type=' + type)
    except Exception as e:
        errorMsg = '品种：' + productName + '，查询主力基差（交易法门），接口异常！' + str(e)
        print(errorMsg)
        return

    retData = r.json()
    # print('retData: ',retData)

    # 分析基差
    data = retData['data']
    # print('data: ',data)

    basisValue = data['basisValue'];
    category = data['category'];
    # print('basisValue:',basisValue)
    # print('category:',category)

    # 先删除，再新增
    dailyBasisCondition = {}
    dailyBasisCondition['productCode'] = productCode
    retDailyBasisData = dailyBasis.delete_one_from_mongodb(dailyBasisCondition)
    # if (retDailyBasisData == None):
    # print('保存品种: ' + productName + '=====》每日基差信息表')

    dailyBasisInfo = {}
    dailyBasisInfo['productName'] = productName
    dailyBasisInfo['productCode'] = productCode

    lengh = len(category)
    if lengh > 1:
        lastDay = category[lengh - 1]
        dailyBasisInfo['lastDay'] = lastDay

    # 计算最后5个交易日，基差变化情况
    if lengh > 5:
        basisStru = calBasisStruV2(basisValue, lengh - 1)
        dailyBasisInfo['basisStru'] = basisStru

    dailyBasisInfo['category'] = category
    dailyBasisInfo['basisValue'] = basisValue

    # print("dailyBasisInfo:", dailyBasisInfo)
    dailyBasis.insert_one_to_mongodb(dailyBasisInfo)
    # else:
    #     print("品种" + productName + "每日期货信息已存在")


## 保存基差原始数据（生意社）
# '菜粕','短纤','鸡蛋','聚丙烯','乙二醇','豆一','菜油'
def getBasisBySYS(tradingDay, selectList):
    # 日期累减1天
    in_date = tradingDay
    dt = datetime.datetime.strptime(in_date, "%Y-%m-%d")
    tradingDayM1 = (dt + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
    tradingDayM2 = (dt + datetime.timedelta(days=-2)).strftime("%Y-%m-%d")
    tradingDayM3 = (dt + datetime.timedelta(days=-3)).strftime("%Y-%m-%d")
    tradingDayM4 = (dt + datetime.timedelta(days=-4)).strftime("%Y-%m-%d")
    tradingDayM5 = (dt + datetime.timedelta(days=-5)).strftime("%Y-%m-%d")
    tradingDayM6 = (dt + datetime.timedelta(days=-6)).strftime("%Y-%m-%d")
    tradingDayM7 = (dt + datetime.timedelta(days=-7)).strftime("%Y-%m-%d")
    tradingDayM8 = (dt + datetime.timedelta(days=-8)).strftime("%Y-%m-%d")
    tradingDayM9 = (dt + datetime.timedelta(days=-9)).strftime("%Y-%m-%d")
    tradingDayM10 = (dt + datetime.timedelta(days=-10)).strftime("%Y-%m-%d")
    tradingDayM11 = (dt + datetime.timedelta(days=-11)).strftime("%Y-%m-%d")
    tradingDayM12 = (dt + datetime.timedelta(days=-12)).strftime("%Y-%m-%d")
    tradingDayM13 = (dt + datetime.timedelta(days=-13)).strftime("%Y-%m-%d")

    tradingDayList = [tradingDayM13, tradingDayM12, tradingDayM11, tradingDayM10, tradingDayM9, tradingDayM8,
                      tradingDayM7, tradingDayM6, tradingDayM5, tradingDayM4, tradingDayM3, tradingDayM2, tradingDayM1,
                      tradingDay]
    for curDay in tradingDayList:
        r = requests.get('http://www.100ppi.com/sf/day-' + curDay + '.html')
        retData = r.text

        bs = BeautifulSoup(retData, "html.parser")  # 解析网页
        table = bs.find_all("tr")  # 定位信息

        productList = []  # [{品种名称，现货价格},{}]结果集
        for i in range(len(table)):
            productDic = {}  # {品种名称，现货价格}

            trStr = table[i].text.strip()
            if len(trStr) > 50 and not trStr.startswith('http'):
                trList = trStr.split('\n')
                trNewList = []
                for td in trList:
                    tdNew = td.replace('\xa0', '')
                    trNewList.append(tdNew)
                productName = trNewList[0]
                if productName == '涤纶短纤':
                    productName = '短纤'
                if productName == '菜籽粕':
                    productName = '菜粕'
                if productName == '菜籽油OI':
                    productName = '菜油'

                productDic['productName'] = productName
                productCode = getCodeByName(productName)

                if productName in selectList:

                    # 查询当前品种的 category 和 basisValue 字段是否有值
                    categoryQueryCon = {}
                    categoryQueryCon['productCode'] = productCode
                    retCateData = dailyBasis.find_one_from_mongodb(categoryQueryCon)
                    if retCateData == None:
                        orgCategory = []
                        orgBasisValue = []
                    else:
                        orgCategory = retCateData['category']
                        orgBasisValue = retCateData['basisValue']

                    cashPrice = trNewList[1]
                    mainPrice = trNewList[3]
                    basisValue = float(cashPrice) - float(mainPrice)

                    productDic['productCode'] = productCode
                    productDic['lastDay'] = tradingDay

                    # categoryList = []
                    # categoryDic = {}
                    # categoryDic[tradingDay.replace("-","")] = tradingDay
                    # categoryList.append(categoryDic)
                    if curDay in orgCategory:
                        continue
                    orgCategory.append(curDay)

                    # basisValueList = []
                    # basisValueDic = {}
                    # basisValueDic[tradingDay.replace("-","")] = basisValue
                    # basisValueList.append(basisValueDic)
                    orgBasisValue.append(basisValue)

                    # 先删除，再新增
                    dailyBasisCondition = {}
                    dailyBasisCondition['productCode'] = productCode
                    dailyBasis.delete_one_from_mongodb(dailyBasisCondition)

                    data = {}
                    data['productName'] = productName
                    data['productCode'] = productCode
                    data['lastDay'] = tradingDay
                    data['category'] = orgCategory
                    data['basisValue'] = orgBasisValue
                    dailyBasis.insert_one_to_mongodb(data)


# selectList = ['聚丙烯', '乙二醇', '短纤', '豆一', '菜粕', '鸡蛋', '菜油']
# getBasisBySYS('2022-11-23', selectList)


# 最新交易日基差，上穿5日基差均值 UP
def calBasisStruV1(basisValue, index):
    lastBasis1 = basisValue[index]
    lastBasis2 = basisValue[index - 1]
    lastBasis3 = basisValue[index - 2]
    lastBasis4 = basisValue[index - 3]
    lastBasis5 = basisValue[index - 4]
    lastBasisList = [lastBasis1, lastBasis2, lastBasis3, lastBasis4, lastBasis5]
    maxBasisValue = max(lastBasisList)
    minBasisValue = min(lastBasisList)
    avgBasisValue = sum(lastBasisList) / len(lastBasisList)
    # print('lastBasisList' + str(lastBasisList))
    # print('maxBasisValue' + str(maxBasisValue))
    # print('minBasisValue' + str(minBasisValue))

    # print('lastBasis1:' + str(lastBasis1) + '|avgBasisValue:' + str(avgBasisValue))
    basisStru = 'OTHER'
    if lastBasis1 > avgBasisValue:
        basisStru = 'UP'
    if lastBasis1 < avgBasisValue:
        basisStru = 'DOWN'
    return basisStru


# 3日基差均值，上穿10日基差均值 UP
def calBasisStruV2(basisValue, index):
    subListMA2 = basisValue[index - 1:index + 1]
    subListMa10 = basisValue[index - 5:index + 1]
    # print("subListMA2:",subListMA2)
    # print("subListMa10:",subListMa10)

    avgBasisValueMA2 = sum(subListMA2) / len(subListMA2)
    avgBasisValueMa10 = sum(subListMa10) / len(subListMa10)
    # print("avgBasisValueMA2:",avgBasisValueMA2)
    # print("avgBasisValueMa10:",avgBasisValueMa10)

    basisStru = 'OTHER'
    if avgBasisValueMA2 > avgBasisValueMa10:
        basisStru = 'UP'
    if avgBasisValueMA2 < avgBasisValueMa10:
        basisStru = 'DOWN'
    return basisStru


# 获得历史基差（交易法门）
def getBasisByTypeAndTradingDay(type, tradingDay):
    try:
        productCode = type
        dailyBasisCondition = {}
        dailyBasisCondition['productCode'] = productCode
        retDailyBasisData = dailyBasis.find_one_from_mongodb(dailyBasisCondition)

        # 查询记录不为空，分情况
        # 1、为指定生意社基差数据 则往下执行，直接查询计算
        # 2、为普通品种，则判断交易日期是否大于系统之前保存的日期，大于，则重新查询
        if (retDailyBasisData != None and (type not in selectList)):
            reQueryFlag = False
            lastDay = retDailyBasisData['lastDay']
            if lastDay != None:
                tradingDayTime = time.mktime(time.strptime(tradingDay, "%Y-%m-%d"))
                lastDayTime = time.mktime(time.strptime(lastDay, "%Y-%m-%d"))
                if int(tradingDayTime) > int(lastDayTime):
                    getBasisByJYFM(productCode)
        if (retDailyBasisData == None):
            getBasisByJYFM(productCode)

        # 重新弄查询最新基差数据
        reQueryCondition = {}
        reQueryCondition['productCode'] = productCode
        reQueryBasisData = dailyBasis.find_one_from_mongodb(reQueryCondition)

        # 获取待查询的日期下标
        category = reQueryBasisData['category']
        basisValue = reQueryBasisData['basisValue']
        if category != None:
            tradingDayIndex = category.index(tradingDay)

            if tradingDayIndex != None:
                # 计算基差结构
                basisStru = calBasisStruV2(basisValue, tradingDayIndex)
                return basisStru

    except Exception as e:
        errorMsg = '品种：' + productCode + '，系统异常！' + str(e)
        return errorMsg


# 初始化当日基差信息表
# for productNameAndCode in productNameAndCodeList:
#     productName = productNameAndCode['productName']
#     productCode = productNameAndCode['productCode']
# getBasisByJYFM(productCode)

# productName = ""
# productCode = "A"
# basisStru = getBasisByTypeAndTradingDay(productCode, "2022-11-21")

# dayList = ["2022-11-07","2022-11-08","2022-11-09","2022-11-10","2022-11-11","2022-11-14","2022-11-15","2022-11-16","2022-11-17","2022-11-18"]
# dayList = ["2022-11-14","2022-11-15","2022-11-16","2022-11-17","2022-11-18"]
# dayList = ["2022-11-21","2022-11-22","2022-11-23","2022-11-24","2022-11-25"]
# dayList = ["2022-11-14"]
# for day in dayList:
#     basisStru = getBasisByTypeAndTradingDay(productCode, day)
#     print(day + "|productCode:" + productCode + "|basisStru: " + str(basisStru))
#     print()

# 品种期限结构表，按照顺序初始化
# tradingDay = '2022-11-08'
# for productNameAndCode in productNameAndCodeList:
#     productCode = productNameAndCode['productCode']
#     getStructureByJYFM(productCode,tradingDay)

# 品种基本信息表，按照顺序初始化
# newNameAndCodeList = []
# for name in productNameList:
#     for product in productNameAndCodeList:
#         if name == product['productName']:
#             dailyBaseInfo.insert_one_to_mongodb(product)
#             newNameAndCodeList.append(product)
# print('newNameAndCodeList: ',newNameAndCodeList)


# 查询仓单原始数据（交易法门）
def getWarehouseByJYFM(tradingDay, type):
    # 通过code查询name
    productCode = type
    productName = getNameByCode(productCode)

    try:
        r = requests.get('https://www.jiaoyifamen.com/tools/api//warehouse/query?t=1673595879133&type=' + type)
    except Exception as e:
        errorMsg = '品种：' + productName + '，查询仓单日报（交易法门），接口异常！' + str(e)
        print(errorMsg)
        return

    retData = r.json()
    # print('retData: ',retData)

    # 获取日期集合
    data = retData['data']
    dataCategory = data['dataCategory']
    category = data['category']

    lengh = len(category)
    if lengh > 1:
        lastDay = category[lengh - 1]

    # 通过当前日期，获取指定日期下标
    dayExlYear = tradingDay[-5:]
    dataIndex = dataCategory.index(dayExlYear)

    yearData = data['year2023']
    warehouse = calWarehouseTrend(yearData, dataIndex)

    # con = {}
    # con['productCode'] = productCode
    # dailyWarehouse.delete_one_from_mongodb(con)

    data = {}
    data['productCode'] = productCode
    data['productName'] = productName
    data['warehouse'] = warehouse
    data['dataCategory'] = dataCategory
    data['yearData'] = yearData
    data['lastDay'] = lastDay
    dailyWarehouse.insert_one_to_mongodb(data)


# 3日基差均值，上穿10日基差均值 UP
def calWarehouseTrend(yearData, index):
    subListMA2 = yearData[index - 1:index + 1]
    subListMa10 = yearData[index - 5:index + 1]
    # print("subListMA2:",subListMA2)
    # print("subListMa10:",subListMa10)

    for item in subListMA2:
        if str(item).isdigit() == False:
            subListMA2.remove(item)
    # print(subListMA2)

    for item in subListMa10:
        if str(item).isdigit() == False:
            subListMa10.remove(item)
        # if item == "NaN":
        #     subListMa10.remove(item)
    # print(subListMa10)

    avgValueMA2 = sum(subListMA2) / len(subListMA2)
    avgValueMa10 = sum(subListMa10) / len(subListMa10)
    # print("avgValueMA2:",avgValueMA2)
    # print("avgValueMa10:",avgValueMa10)

    warehouse = 'OTHER'
    if avgValueMA2 > avgValueMa10:
        warehouse = 'UP'
    if avgValueMA2 < avgValueMa10:
        warehouse = 'DOWN'

    return warehouse


# getWarehouseByJYFM("2023-01-12","Y","03")


# 获得历史仓单（交易法门）
def getWarehouseByTypeAndTradingDay(type, tradingDay):
    try:
        productCode = type
        dailyWarehouseCondition = {}
        dailyWarehouseCondition['productCode'] = productCode
        retDailyWarehouseData = dailyWarehouse.find_one_from_mongodb(dailyWarehouseCondition)

        # 查询记录不为空，分情况
        # 1、为指定生意社基差数据 则往下执行，直接查询计算 #and (type not in selectList)
        # 2、为普通品种，则判断交易日期是否大于系统之前保存的日期，大于，则重新查询
        if (retDailyWarehouseData != None):
            reQueryFlag = False
            lastDay = retDailyWarehouseData['lastDay']
            if lastDay != None:
                tradingDayTime = time.mktime(time.strptime(tradingDay, "%Y-%m-%d"))
                lastDayTime = time.mktime(time.strptime(lastDay, "%Y-%m-%d"))
                if int(tradingDayTime) > int(lastDayTime):
                    getWarehouseByJYFM(tradingDay, productCode)
        if (retDailyWarehouseData == None):
            getWarehouseByJYFM(tradingDay, productCode)

        # 重新弄查询最新基差数据
        reQueryCondition = {}
        reQueryCondition['productCode'] = productCode
        reQueryBasisData = dailyWarehouse.find_one_from_mongodb(reQueryCondition)

        # 获取待查询的日期下标
        category = reQueryBasisData['dataCategory']
        yearData = reQueryBasisData['yearData']
        if category != None:
            tradingDayIndex = category.index(tradingDay[-5:])

            if tradingDayIndex != None:
                # 计算仓单趋势
                warehouse = calWarehouseTrend(yearData, tradingDayIndex)
                return warehouse

    except Exception as e:
        errorMsg = '品种：' + productCode + '，系统异常！' + str(e)
        return errorMsg

# getWarehouseByTypeAndTradingDay("2023-01-10","Y")
