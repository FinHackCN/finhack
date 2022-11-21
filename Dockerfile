FROM alpine

LABEL maintainer woldy <king@woldy.net>


ENV LANG en_US.UTF-8
RUN echo "http://mirrors.aliyun.com/alpine/latest-stable/main/" > /etc/apk/repositories && \
    echo "http://mirrors.aliyun.com/alpine/latest-stable/community/" >> /etc/apk/repositories
 
RUN mkdir -p /data/FinHack/FinHack-Collecter && apk update && \
apk add sudo vim  zip wget libxml2 libxml2-dev libxslt libxslt-dev linux-headers gcc build-base g++ cmake  make  python3 python3-dev py-pip gfortran && \
pip config set global.index-url http://mirrors.aliyun.com/pypi/simple && \
pip config set install.trusted-host mirrors.aliyun.com && \ 
cd /data/FinHack/FinHack-Collecter/ && \
wget https://github.com/FinHackCN/FinHack-Collecter/archive/refs/heads/master.zip && \
unzip master.zip && \ 
mv FinHack-Collecter-master/* ./ && \
pip install -r requirements.txt

ENTRYPOINT ["python3","/data/FinHack/FinHack-Collecter/command/cmd_collect.py"]
