import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader

# 定义Attention-LSTM模型
class AttentionLSTM(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(AttentionLSTM, self).__init__()
        self.hidden_size = hidden_size
        self.lstm = nn.LSTM(input_size, hidden_size)
        self.attention = nn.Linear(hidden_size, output_size)

    def forward(self, input):
        output, _ = self.lstm(input)
        attention_weights = self.attention(output)
        attention_weights = torch.softmax(attention_weights, dim=0)
        output = torch.sum(output * attention_weights, dim=0)
        return output

# 定义数据集类
class FuturesDataset(Dataset):
    def __init__(self, data):
        self.data = data

    def __len__(self):
        return len(self.data)

    def __getitem__(self, index):
        return self.data[index]

# 加载数据
data = pd.read_csv('futures_data.csv')
train_data = data.iloc[:-100]
test_data = data.iloc[-100:]

# 准备训练数据
train_dataset = FuturesDataset(train_data.values)
train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True)

# 准备测试数据
test_dataset = FuturesDataset(test_data.values)
test_loader = DataLoader(test_dataset, batch_size=1, shuffle=False)

# 设置模型超参数
input_size = 10
hidden_size = 32
output_size = 1
learning_rate = 0.001
num_epochs = 10

# 创建模型和优化器
model = AttentionLSTM(input_size, hidden_size, output_size)
optimizer = optim.Adam(model.parameters(), lr=learning_rate)
criterion = nn.MSELoss()

# 训练模型
for epoch in range(num_epochs):
    for batch_data in train_loader:
        inputs = batch_data[:, :-1]
        labels = batch_data[:, -1]

        optimizer.zero_grad()

        outputs = model(inputs.unsqueeze(0).float())
        loss = criterion(outputs, labels.float())

        loss.backward()
        optimizer.step()

    print(f'Epoch {epoch+1}/{num_epochs}, Loss: {loss.item()}')

# 测试模型
model.eval()
with torch.no_grad():
    test_loss = 0
    for batch_data in test_loader:
        inputs = batch_data[:, :-1]
        labels = batch_data[:, -1]

        outputs = model(inputs.unsqueeze(0).float())
        test_loss += criterion(outputs, labels.float()).item()

    avg_test_loss = test_loss / len(test_loader)
    print(f'Average Test Loss: {avg_test_loss}')

# 使用模型进行套利策略
model.eval()
with torch.no_grad():
    positions = []
    for batch_data in test_loader:
        inputs = batch_data[:, :-1]
        outputs = model(inputs.unsqueeze(0).float())

        if outputs > 0:
            positions.append(1)  # 买入开仓
        else:
            positions.append(-1)  # 卖出开仓

    # 根据持仓情况计算收益
    returns = []
    position = 0
    for i in range(len(test_data)):
        if positions[i] != position:
            if position == 0:
                returns.append(0)  # 空仓
            else:
                returns.append(test_data['close'].iloc[i] - test_data['close'].iloc[i-1])  # 平仓
            position = positions[i]
        else:
            returns.append(0)  # 保持持仓

    total_return = sum(returns)
    print(f'Total Return: {total_return}')