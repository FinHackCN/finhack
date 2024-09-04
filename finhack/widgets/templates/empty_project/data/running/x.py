import pickle

# 以二进制读模式打开文件
with open('/data/code/demo_project/data/running/r_ad6709f1d13f642e16d74a40190d06c5.pkl', 'rb') as f:
    data = pickle.load(f)

# 打印读取的数据
print(data)

