# 你当前使用的模型为gpt-3.5-turbo! 站长合作邮箱：wxgpt@qq.com
#
# 以下是一个简单的商品期货跨期套利网格策略的Python代码示例：

import numpy as np
import pandas as pd

def grid_trading_strategy(data, upper_limit, lower_limit, grid_size):
    # 计算价格的网格范围
    price_range = np.arange(lower_limit, upper_limit, grid_size)

    # 初始化持仓和盈亏
    position = 0
    pnl = []

    for i in range(len(data)):
        # 当前价格
        current_price = data[i]

        # 计算当前价格所在的网格
        current_grid = int(current_price / grid_size)

        # 判断是否有交易信号
        if current_grid > position:
            # 价格上涨，买入
            position = current_grid
            pnl.append(-current_price)
        elif current_grid < position:
            # 价格下跌，卖出
            position = current_grid
            pnl.append(current_price)
        else:
            # 价格不变，无交易
            pnl.append(0)

    # 计算累积盈亏
    cumulative_pnl = np.cumsum(pnl)

    return cumulative_pnl

# 测试数据
data = [100, 110, 120, 130, 125, 115, 105, 95, 105, 115]
upper_limit = 150
lower_limit = 90
grid_size = 5

# 运行策略
pnl = grid_trading_strategy(data, upper_limit, lower_limit, grid_size)

# 输出结果
df = pd.DataFrame({'Price': data, 'P&L': pnl})
print(df)


'''
在上述代码中，`grid_trading_strategy`函数实现了商品期货跨期套利网格策略，
参数`data`为价格序列，`upper_limit`和`lower_limit`为价格的上下限，`grid_size`为每个网格的大小。
函数会根据价格的变动情况决定是否买入或卖出，并计算累积盈亏。最后，将价格和盈亏结果存储在DataFrame中并输出。
'''