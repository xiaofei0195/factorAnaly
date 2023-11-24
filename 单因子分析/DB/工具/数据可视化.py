import pandas as pd
import matplotlib.pyplot as plt

# 读取CSV文件
data = pd.read_csv('AKSHARE_全品种指数价格信息.csv')

# 将日期列转换为日期时间类型
data['date'] = pd.to_datetime(data['date'])

# 按照日期和商品代码分组并计算数值总和
grouped_data = data.groupby(['date', 'code']).sum()

# 重置索引以便于绘图
grouped_data = grouped_data.reset_index()

# 创建一个画布和子图
fig, ax = plt.subplots()

# 循环遍历每个商品代码
for code in grouped_data['code'].unique():
    # 选择对应商品代码的数据
    df = grouped_data[grouped_data['code'] == code]
    # 绘制日期和数值
    ax.plot(df['date'], df['close'], label=code)

# 设置图例
ax.legend()

# 设置x轴标签为日期
ax.set_xlabel('date')

# 设置y轴标签
ax.set_ylabel('close')

# 自动调整日期标签的格式
fig.autofmt_xdate()

# 显示图形
plt.show()