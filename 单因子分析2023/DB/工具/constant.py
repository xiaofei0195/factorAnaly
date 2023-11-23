import pandas as pd
import pytz

tz = pytz.timezone('Asia/Shanghai')


'''
####################################################
指定数据抓取日期范围
####################################################
'''
startStr = '2010-01-01'
endStr = '2023-10-01'


'''
####################################################
指定回测日期范围
####################################################
'''
start_date = pd.to_datetime('2020-01-01')
end_date = pd.to_datetime('2023-10-01')

sector_names = {
    0: "hei se",
    1: "nong chan pin",
    2: "neng hua",
    3: "you se",
}

'''
####################################################
回测指定品种
####################################################
'''
#
HS = ['JM', 'J', 'I', 'RB', 'HC', 'SM', 'SF']
NCP = ['A', 'B', 'PK', 'M', 'Y', 'P', 'RM', 'OI', 'SR', 'CF', 'JD', 'LH', 'C', 'CS', 'AP', 'CJ']
NH = ['EG', 'BU', 'SP', 'SA', 'FG', 'L', 'MA', 'PP', 'EB', 'NR', 'UR', 'RU', 'FU', 'V', 'PF', 'TA', 'LU',
      'PG', 'BR', 'PX', 'SH', 'EC']
YS = ['CU', 'AL', 'ZN', 'PB', 'NI', 'SN', 'SS', 'AU', 'AG', 'AO', 'SI', 'LC']

# 有效品种
CODE_LIST = HS + NCP + NH + YS


'''
####################################################
回测指定月份
####################################################
'''

mon07 = {'mon1': '07', 'mon2': '09'}
mon08 = {'mon1': '08', 'mon2': '10'}
mon09 = {'mon1': '09', 'mon2': '11'}
mon10 = {'mon1': '10', 'mon2': '12'}
mon11 = {'mon1': '11', 'mon2': '01'}
mon12 = {'mon1': '12', 'mon2': '02'}
monList = [mon07,mon08,mon09,mon10,mon11,mon12]


'''
####################################################
期货交易所，合约代码
####################################################
'''
# 【上海期货交易所】
SHFE_LIST = ['PB', 'AG', 'AL', 'AU', 'BU', 'CU', 'FU', 'HC', 'NI', 'RB', 'RU', 'SN', 'SP', 'SS', 'WR', 'ZN', 'BR', 'EC',
             'AO']

# 【郑州期货交易所】
CZCE_LIST = ['MA', 'AP', 'CF', 'CJ', 'CY', 'FG', 'JR', 'LR', 'ZC', 'OI', 'PF', 'PK', 'PM', 'RI', 'RM', 'RS',
             'SA', 'SF', 'SM', 'SR', 'TA', 'UR', 'WH', 'PX', 'SH']

# 【大连期货交易所】
DCE_LIST = ['B', 'J', 'JD', 'JM', 'L', 'LH', 'M', 'P', 'PG', 'PP', 'RR', 'V', 'Y', 'I', 'FB', 'EG', 'EB',
            'CS', 'C', 'BB', 'A']

# 【上海国际能源交易中心】
INE_LIST = ['BC', 'LU', 'NR', 'SC']

# 【广期所	】
GFEX_LIST = ['SI', 'LC']

EXC_CODE_LIST = SHFE_LIST + CZCE_LIST + DCE_LIST + INE_LIST + GFEX_LIST
