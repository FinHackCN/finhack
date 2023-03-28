# docker 镜像打包

### 打包web基础Build包

~~~sh
# 编译打包到本地
docker build -f ./docker_env/web/DockerfileBuild -t registry.cn-zhangjiakou.aliyuncs.com/dvadmin-pro/node12-base-web:latest .
# 上传到阿里云仓库
docker push registry.cn-zhangjiakou.aliyuncs.com/dvadmin-pro/node12-base-web:latest

~~~

### 打包Backend基础Build包

~~~sh
# 编译打包到本地
docker build -f ./docker_env/django/DockerfileBuild -t registry.cn-zhangjiakou.aliyuncs.com/dvadmin-pro/python38-base-backend:latest .
# 上传到阿里云仓库
docker push registry.cn-zhangjiakou.aliyuncs.com/dvadmin-pro/python38-base-backend:latest
~~~

### 运行前端

~~~
docker build -f ./docker_env/web/Dockerfile -t dvadmin-pro-web .
~~~

### 运行后端

~~~
docker build -f ./docker_env/django/Dockerfile -t dvadmin-pro-django .
~~~

### 运行celery

~~~
docker build -f ./docker_env/celery/Dockerfile -t dvadmin-pro-celery .
~~~

## docker-compose 运行

~~~
# 先安装docker-compose (自行百度安装),执行此命令等待安装，如有使用celery插件请打开docker-compose.yml中celery 部分注释
docker-compose up -d
# 初始化后端数据(第一次执行即可)
docker exec -ti dvadmin-django bash
python manage.py makemigrations 
python manage.py migrate
python manage.py init -y
exit

前端地址：http://127.0.0.1:8080
后端地址：http://127.0.0.1:8000
# 在服务器上请把127.0.0.1 换成自己公网ip
账号：superadmin 密码：admin123456

# docker-compose 停止
docker-compose down
#  docker-compose 重启
docker-compose restart
#  docker-compose 启动时重新进行 build
docker-compose up -d --build

~~~

