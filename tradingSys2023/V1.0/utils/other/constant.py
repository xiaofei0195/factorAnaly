# -*- coding:utf-8 -*-
STRU_BACK = 'BACK'
STRU_CONTANGO = 'CONTANGO'

# 期限结构
PriceStruDailyUrl = 'https://www.jiaoyifamen.com/tools/api//future/basis/structure?t=1667631967944&type='

# 基差日报
BasisDailyUrl = 'https://www.jiaoyifamen.com/tools/api/future-basis/query?type='

# 仓单
WarehouseDailyUrl = 'https://www.jiaoyifamen.com/tools/api//warehouse/query?t=1673595879133&type='

# 现货【交易法门】
CashPriceByJYFMUrl = 'https://www.jiaoyifamen.com/tools/api//future-basis/daily'

# 跨期价差
SpreadDailyUrl = 'https://www.jiaoyifamen.com/tools/api//future/spread/free'

# 指定品种【生意社】
selectList = ['聚丙烯', '乙二醇', '短纤', '塑料', '豆一', '菜粕', '油菜籽', '鸡蛋', '生猪', '棉纱', '菜油', '棉花', '白糖']

# 重命名列表【生意社】
product_mapping = {
    '涤纶短纤': '短纤',
    '低硫燃料油': '低燃油',
    '液化石油气': '液化气',
    '石油沥青': '沥青',
    '燃料油': '燃油',
    '天然橡胶': '橡胶',
    '甲醇MA': '甲醇',
    '聚氯乙烯': 'PVC',
    '聚乙烯': '塑料',
    '铜': '沪铜',
    '锌': '沪锌',
    '铝': '沪铝',
    '铅': '沪铅',
    '镍': '沪镍',
    '白银': '沪银',
    '锡': '沪锡',
    '棕榈油': '棕榈',
    '玉米淀粉': '淀粉',
    '菜籽粕': '菜粕',
    '菜籽油OI': '菜油',
    '螺纹钢': '螺纹',
    '热轧卷板': '热卷'
}
