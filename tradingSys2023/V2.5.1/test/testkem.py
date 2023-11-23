import numpy as np
from pykalman import KalmanFilter

# 定义跨期套利策略的参数
initial_state_mean = [0, 0]  # 初始状态均值
initial_state_covariance = np.eye(2)  # 初始状态协方差矩阵
transition_matrix = np.eye(2)  # 状态转移矩阵
observation_matrix = np.vstack([[1, 0], [0, 1]])  # 观测矩阵
observation_covariance = np.eye(2)  # 观测协方差矩阵

# 创建Kalman滤波器对象
kf = KalmanFilter(
    initial_state_mean=initial_state_mean,
    initial_state_covariance=initial_state_covariance,
    transition_matrices=transition_matrix,
    observation_matrices=observation_matrix,
    observation_covariance=observation_covariance
)

# 定义跨期套利策略的数据
price_current_period = np.array([100, 105, 110, 115, 120])  # 当前期的价格
price_next_period = np.array([105, 108, 115, 118, 125])  # 下一期的价格

# 使用Kalman滤波器进行状态估计
state_means, state_covariances = kf.filter(price_current_period)

# 计算跨期套利指标
spread = price_current_period - price_next_period
z_score = (spread - state_means[:, 0]) / np.sqrt(state_covariances[:, 0, 0])

# 根据跨期套利指标进行交易决策
positions = np.where(z_score > 1, -1, np.where(z_score < -1, 1, 0))

# 打印交易决策
print("交易决策：", positions)