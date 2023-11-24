productNameAndCodeList = [{'productName': '沪铜', 'productCode': 'CU'}, {'productName': '沪镍', 'productCode': 'NI'},
                          {'productName': '沪锡', 'productCode': 'SN'}, {'productName': '沪铅', 'productCode': 'PB'},
                          {'productName': '沪银', 'productCode': 'AG'}, {'productName': '沪铝', 'productCode': 'AL'},
                          {'productName': '沪锌', 'productCode': 'ZN'}, {'productName': '铁矿石', 'productCode': 'I'},
                          {'productName': '不锈钢', 'productCode': 'SS'}, {'productName': '螺纹', 'productCode': 'RB'},
                          {'productName': '热卷', 'productCode': 'HC'}, {'productName': '锰硅', 'productCode': 'SM'},
                          {'productName': '硅铁', 'productCode': 'SF'}, {'productName': '焦煤', 'productCode': 'JM'},
                          {'productName': '焦炭', 'productCode': 'J'}, {'productName': '豆一', 'productCode': 'A'},
                          {'productName': '棕榈', 'productCode': 'P'}, {'productName': '豆油', 'productCode': 'Y'},
                          {'productName': '菜油', 'productCode': 'OI'}, {'productName': '豆粕', 'productCode': 'M'},
                          {'productName': '菜粕', 'productCode': 'RM'}, {'productName': '玉米', 'productCode': 'C'},
                          {'productName': '淀粉', 'productCode': 'CS'}, {'productName': '棉花', 'productCode': 'CF'},
                          {'productName': '油菜籽', 'productCode': 'RS'}, {'productName': '鸡蛋', 'productCode': 'JD'},
                          {'productName': '生猪', 'productCode': 'LH'}, {'productName': '棉纱', 'productCode': 'CY'},
                          {'productName': '聚丙烯', 'productCode': 'PP'}, {'productName': '乙二醇', 'productCode': 'EG'},
                          {'productName': '短纤', 'productCode': 'PF'}, {'productName': '沥青', 'productCode': 'BU'},
                          {'productName': '塑料', 'productCode': 'L'}, {'productName': '原油', 'productCode': 'SC'},
                          {'productName': '燃油', 'productCode': 'FU'}, {'productName': '低燃油', 'productCode': 'LU'},
                          {'productName': '液化气', 'productCode': 'PG'}, {'productName': '橡胶', 'productCode': 'RU'},
                          {'productName': '20号胶', 'productCode': 'NR'}, {'productName': '玻璃', 'productCode': 'FG'},
                          {'productName': 'PVC', 'productCode': 'V'}, {'productName': '纯碱', 'productCode': 'SA'},
                          {'productName': '甲醇', 'productCode': 'MA'}, {'productName': 'PTA', 'productCode': 'TA'},
                          {'productName': '尿素', 'productCode': 'UR'}, {'productName': '纸浆', 'productCode': 'SP'},
                          {'productName': '苯乙烯', 'productCode': 'EB'}]


productNameList = ['沪铜', '沪镍', '沪锡', '沪铅', '沪银', '沪铝', '沪锌', '铁矿石', '不锈钢', '螺纹', '热卷', '锰硅', '硅铁', '焦煤', '焦炭', '豆一',
                   '棕榈', '豆油', '菜油', '豆粕', '菜粕', '玉米', '淀粉', '棉花', '油菜籽', '鸡蛋', '生猪', '棉纱', '聚丙烯', '乙二醇', '短纤', '沥青',
                   '塑料', '原油', '燃油', '低燃油', '液化气', '橡胶', '20号胶', '玻璃', 'PVC', '纯碱', '甲醇', 'PTA', '尿素', '纸浆', '苯乙烯']

productExcludeList = ['沪金', '沪银', '国际铜', '焦煤', '焦炭', '纤维板', '胶合板', '胶合板', '油菜籽', '棉纱', '生猪', '红枣', '花生', '早稻', '晚稻',
                      '粳米', '普麦', '强麦']

productSpecAgriList = ['棉花', '白糖', '淀粉', '玉米', '菜油', '豆一', '豆粕', '菜粕', '豆油']


# 通过生意社，查询现货价格品种
# 聚丙烯 乙二醇 涤纶短纤 聚乙烯
# 豆一  菜籽粕  油菜籽  鸡蛋  生猪 棉纱 菜籽油OI
selectList = ['聚丙烯', '乙二醇', '短纤', '豆一', '菜粕', '鸡蛋', '菜油']