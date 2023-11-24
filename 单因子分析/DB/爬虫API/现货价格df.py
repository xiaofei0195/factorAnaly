# 查询近月远月价差TEMP
import pandas as pd
import requests


def getCashPriceDf(productCode):

    url = ""
    if productCode == "JM":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=jm2401&spotIndexId=8047439'
    if productCode == "J":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=j2401&spotIndexId=3276468'
    if productCode == "I":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=i2401&spotIndexId=2911136'
    if productCode == "RB":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=rb2401&spotIndexId=20868'
    if productCode == "HC":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=hc2401&spotIndexId=20892'
    if productCode == "SM":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=SM2401&spotIndexId=2959498'
    if productCode == "SF":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=SF2401&spotIndexId=4789796'

    if productCode == "A":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=a2401&spotIndexId=2856697'
    if productCode == "B":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=b2401&spotIndexId=2856687'
    if productCode == "PK":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=PK2401&spotIndexId=3279122'
    if productCode == "M":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=m2401&spotIndexId=2856711'
    if productCode == "Y":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=y2401&spotIndexId=2888539'
    if productCode == "P":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=p2401&spotIndexId=2954722'
    if productCode == "RM":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=RM2401&spotIndexId=3279181'
    if productCode == "OI":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=OI2401&spotIndexId=3280887'
    if productCode == "SR":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=SR2401&spotIndexId=4242401'

    if productCode == "CF":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=CF2401&spotIndexId=2885871'
    if productCode == "JD":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=jd2401&spotIndexId=4346147'
    if productCode == "LH":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=lh2401&spotIndexId=4320937'
    if productCode == "C":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=c2401&spotIndexId=15332881'
    if productCode == "CS":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=cs2401&spotIndexId=4412577'
    if productCode == "AP":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=AP2401&spotIndexId=4863975'
    if productCode == "CJ":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=CJ2401&spotIndexId=13232910'

    # YS = ['CU', 'AL', 'ZN', 'PB', 'NI', 'SN', 'SS', 'AU', 'AG', 'AO', 'SI', 'LC']
    if productCode == "CU":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=cu2312&spotIndexId=2981535'
    if productCode == "AL":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=al2312&spotIndexId=2865592'
    if productCode == "ZN":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=zn2312&spotIndexId=2865578'
    if productCode == "PB":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=pb2312&spotIndexId=25475'
    if productCode == "NI":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=ni2312&spotIndexId=2981540'
    if productCode == "SN":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=sn2312&spotIndexId=2981539'
    if productCode == "SS":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=ss2401&spotIndexId=4419402'
    if productCode == "AU":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=au2402&spotIndexId=2865601'
    if productCode == "AG":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=ag2402&spotIndexId=2865596'
    if productCode == "AO":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=ao2401&spotIndexId=4227277'
    if productCode == "SI":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=si2401&spotIndexId=6159072'
    if productCode == "LC":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=lc2401&spotIndexId=17498544'


    # NH = ['EG', 'BU', 'SP', 'SA', 'FG', 'L', 'MA', 'PP', 'EB', 'NR', 'UR', 'RU', 'FU', 'V', 'PF', 'TA', 'LU',
    #       'PG', 'BR', 'PX', 'SH', 'EC']
    if productCode == "EG":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=eg2401&spotIndexId=3994600'
    if productCode == "BU":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=bu2401&spotIndexId=4163702'
    if productCode == "SP":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=sp2401&spotIndexId=4807554'

    if productCode == "SA":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=SA2401&spotIndexId=2825734'
    if productCode == "FG":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=FG2401&spotIndexId=5470470'
    if productCode == "L":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=l2401&spotIndexId=2836796'
    if productCode == "MA":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=MA2401&spotIndexId=3994516'
    if productCode == "PP":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=pp2401&spotIndexId=4242351'
    if productCode == "EB":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=eb2312&spotIndexId=4077400'
    if productCode == "NR":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=nr2401&spotIndexId=3058218'
    if productCode == "UR":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=UR2401&spotIndexId=4527348'
    if productCode == "RU":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=ru2401&spotIndexId=4321828'
    if productCode == "FU":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=fu2401&spotIndexId=9138369'


    if productCode == "V":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=v2401&spotIndexId=2825715'
    if productCode == "PF":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=PF2402&spotIndexId=5402526'
    if productCode == "TA":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=TA2401&spotIndexId=2835975'
    if productCode == "LU":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=lu2402&spotIndexId=9138373'
    if productCode == "PG":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=pg2312&spotIndexId=5028348'
    if productCode == "BR":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=br2401&spotIndexId=3012015'
    if productCode == "PX":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=PX2405&spotIndexId=2863173'
    if productCode == "SH":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=SH2405&spotIndexId=5402459'
    if productCode == "EC":
        url = 'http://fupage.10jqka.com.cn/futgwapi/api/market/v1/basisData/getTrendData?contract=ec2404&spotIndexId=8618297'


    try:
        r = requests.get(url)
    except Exception as e:
        errorMsg = '接口异常！' + str(e)
        print(errorMsg)
        return None

    retData = r.json()
    data = retData['data']
    if data != None:
        dataRaw_df = pd.DataFrame(data)
        dataRaw_df.rename(columns={"tranDate": "date", "futuresConvertPrice": "cashPrice"}, inplace=True)
        dataRaw_df['date'] = pd.to_datetime(dataRaw_df['date'], format='%Y.%m.%d')
        data_df = dataRaw_df.loc[:, ["date", "cashPrice"]]
        data_df['code'] = productCode
        return data_df

    return pd.DataFrame()



# HS = ['JM', 'J', 'I', 'RB', 'HC', 'SM', 'SF']
# NCP = ['A', 'B', 'PK', 'M', 'Y', 'P', 'RM', 'OI', 'SR', 'CF', 'JD', 'LH', 'C', 'CS', 'AP', 'CJ']
NH = ['EG', 'BU', 'SP', 'SA', 'FG', 'L', 'MA', 'PP', 'EB', 'NR', 'UR', 'RU', 'FU', 'V', 'PF', 'TA', 'LU',
      'PG', 'BR', 'PX', 'SH', 'EC']
# YS = ['CU', 'AL', 'ZN', 'PB', 'NI', 'SN', 'SS', 'AU', 'AG', 'AO', 'SI', 'LC']


if __name__ == '__main__':

    for productCode in NH:
        data_df = getCashPriceDf(productCode)
        print(data_df)
        data_df.to_csv("../CSV导出/现货折盘面/" + productCode + "_现货折盘面价格.csv")



