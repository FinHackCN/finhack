# 使用官方Python 3.9镜像作为基础镜像
FROM python:3.9

# 设置工作目录为/app
WORKDIR /app

# 安装TA-Lib依赖的基础库
RUN apt-get update && apt-get install -y \
    wget \
    tar \
    build-essential

# 下载TA-Lib的C库并安装
RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
    tar -xvzf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib/ && \
    ./configure --prefix=/usr && \
    make && \
    make install

# 清理不再需要的文件
RUN rm -R ta-lib ta-lib-0.4.0-src.tar.gz

# 设置环境变量，以确保库能被正确找到
ENV LD_LIBRARY_PATH /usr/local/lib:$LD_LIBRARY_PATH

RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple
RUN /usr/local/bin/python -m pip install --upgrade pip
# 安装finhack库
RUN pip install finhack
# 创建一个新的finhack项目
RUN finhack project create --project_path=/app/finhack_project
WORKDIR /app/finhack_project
# 当容器启动时，默认执行命令
CMD ["tail", "-f", "/dev/null"]

#docker exec -ti finhack-docker finhack collector run
