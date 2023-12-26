#!/bin/bash
echo "请注意修改默认密码，并确保 MySQL 与 Redis 不要面向公网访问！"
read -p "是否继续创建 Docker 容器？(y/n) " -n 1 -r
echo    # 新行
if [[ $REPLY =~ ^[Yy]$ ]]
then
    CURRENT_DIR=$(pwd)
    # 设置 MySQL 容器的参数
    MYSQL_CONTAINER_NAME="finhack-mysql"
    MYSQL_ROOT_PASSWORD="finhack"
    MYSQL_DATABASE1="tushare"
    MYSQL_DATABASE2="finhack"
    MYSQL_PORT=3307
    MYSQL_DATA_DIR="$CURRENT_DIR/database/mysql-data"

    # 设置 Redis 容器的参数
    REDIS_CONTAINER_NAME="finhack-redis"
    REDIS_PASSWORD="finhack"
    REDIS_PORT=6380
    REDIS_DATA_DIR="$CURRENT_DIR/database/redis-data"

    # 获取脚本所在的目录的绝对路径
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
    # 设置 SQL 文件的绝对路径
    FINHACK_STRUCTURE_SQL="$SCRIPT_DIR/../database/finhack_structure.sql"

    # 检查并创建 MySQL 数据目录
    if [ ! -d "$MYSQL_DATA_DIR" ]; then
        echo "MySQL 数据目录不存在，正在创建..."
        mkdir -p "$MYSQL_DATA_DIR"
    fi

    # 检查并创建 Redis 数据目录
    if [ ! -d "$REDIS_DATA_DIR" ]; then
        echo "Redis 数据目录不存在，正在创建..."
        mkdir -p "$REDIS_DATA_DIR"
    fi


    # 检查 MySQL 容器是否存在
    if [ $(docker ps -aq -f name=^/${MYSQL_CONTAINER_NAME}$) ]; then
        # 如果容器存在，询问用户是否删除
        read -p "MySQL 容器已经存在。是否删除并重新创建？(y/n) " -n 1 -r
        echo    # 新行
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            # 删除容器
            docker rm -f $MYSQL_CONTAINER_NAME
        else
            echo "操作已取消。"
            exit 1
        fi
    fi

    # 创建 MySQL 容器
    echo "正在创建 MySQL 容器..."
    docker run --name $MYSQL_CONTAINER_NAME \
        -e MYSQL_ROOT_PASSWORD=$MYSQL_ROOT_PASSWORD \
        -e MYSQL_DATABASE=$MYSQL_DATABASE1 \
        -e MYSQL_DATABASE=$MYSQL_DATABASE2 \
        -p $MYSQL_PORT:3306 \
        -v $MYSQL_DATA_DIR:/var/lib/mysql \
        -d mysql:latest

    # 等待 MySQL 容器完全启动
    echo "正在等待 MySQL 初始化..."
    sleep 30


    # 创建第1个数据库
    echo "正在创建数据库 $MYSQL_DATABASE1..."
    docker exec -i $MYSQL_CONTAINER_NAME mysql -uroot -p$MYSQL_ROOT_PASSWORD -e "CREATE DATABASE IF NOT EXISTS $MYSQL_DATABASE1;"

    # 导入 SQL 文件
    echo "正在导入数据库 $MYSQL_DATABASE2..."
    docker exec -i $MYSQL_CONTAINER_NAME mysql -uroot -p$MYSQL_ROOT_PASSWORD $MYSQL_DATABASE2 < "$FINHACK_STRUCTURE_SQL"





    # 检查 Redis 容器是否存在
    if [ $(docker ps -aq -f name=^/${REDIS_CONTAINER_NAME}$) ]; then
        # 如果容器存在，询问用户是否删除
        read -p "Redis 容器已经存在。是否删除并重新创建？(y/n) " -n 1 -r
        echo    # 新行
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            # 删除容器
            docker rm -f $REDIS_CONTAINER_NAME
        else
            echo "操作已取消。"
            exit 1
        fi
    fi

    # 创建 Redis 容器
    echo "正在创建 Redis 容器..."
    docker run --name $REDIS_CONTAINER_NAME \
        -p $REDIS_PORT:6379 \
        -v $REDIS_DATA_DIR:/data \
        -d redis:latest redis-server --appendonly yes --requirepass $REDIS_PASSWORD

    echo "容器已创建并正在运行。"

else
    # 用户输入不是 'y'，不执行命令
    echo "操作已取消。"
fi
