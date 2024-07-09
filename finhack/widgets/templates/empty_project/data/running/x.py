import pickle

# 以二进制读模式打开文件
with open('test.pkl', 'rb') as f:
    data = pickle.load(f)

# 打印读取的数据
print(data)

