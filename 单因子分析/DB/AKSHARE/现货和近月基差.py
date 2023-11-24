from 单因子分析.DB.工具 import constant

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

# 现货折盘面价格


constant