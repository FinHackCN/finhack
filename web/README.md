# Django-Vue-Admin

[![img](https://img.shields.io/badge/license-MIT-blue.svg)](https://gitee.com/liqianglog/django-vue-admin/blob/master/LICENSE)  [![img](https://img.shields.io/badge/python-%3E=3.7.x-green.svg)](https://python.org/)  [![PyPI - Django Version badge](https://img.shields.io/badge/django%20versions-3.2-blue)](https://docs.djangoproject.com/zh-hans/3.2/) [![img](https://img.shields.io/badge/node-%3E%3D%2012.0.0-brightgreen)](https://nodejs.org/zh-cn/) [![img](https://gitee.com/liqianglog/django-vue-admin/badge/star.svg?theme=dark)](https://gitee.com/liqianglog/django-vue-admin)

[预 览](https://demo.django-vue-admin.com) | [官网文档](https://www.django-vue-admin.com) | [群聊](https://qm.qq.com/cgi-bin/qm/qr?k=fOdnHhC8DJlRHGYSnyhoB8P5rgogA6Vs&jump_from=webapi) | [社区](https://bbs.django-vue-admin.com) | [插件市场](https://bbs.django-vue-admin.com/plugMarket.html) | [Github](https://github.com/liqianglog/django-vue-admin) 



💡 **「关于」**

我们是一群热爱代码的青年，在这个炙热的时代下，我们希望静下心来通过Code带来一点我们的色彩和颜色。

因为热爱，所以拥抱未来

## 平台简介

💡 [django-vue-admin](https://gitee.com/dvadmin/django-vue-admin) 是一套全部开源的快速开发平台，毫无保留给个人及企业免费使用。



* 🧑‍🤝‍🧑前端采用[D2Admin](https://github.com/d2-projects/d2-admin) 、[Vue](https://cn.vuejs.org/)、[ElementUI](https://element.eleme.cn/)。
* 👭后端采用 Python 语言 Django 框架以及强大的 [Django REST Framework](https://pypi.org/project/djangorestframework)。
* 👫权限认证使用[Django REST Framework SimpleJWT](https://pypi.org/project/djangorestframework-simplejwt)，支持多终端认证系统。
* 👬支持加载动态权限菜单，多方式轻松权限控制。
* 💏特别鸣谢：[D2Admin](https://github.com/d2-projects/d2-admin) 、[Vue-Element-Admin](https://github.com/PanJiaChen/vue-element-admin)。
* 💡 特别感谢[jetbrains](https://www.jetbrains.com/) 为本开源项目提供免费的 IntelliJ IDEA 授权。



## 在线体验

👩‍👧‍👦演示地址：[http://demo.django-vue-admin.com](http://demo.django-vue-admin.com) 

- 账号：superadmin 

- 密码：admin123456

👩‍👦‍👦文档地址：[https://django-vue-admin.com](https://django-vue-admin.com)



## 交流

- 交流社区：[戳我](https://bbs.django-vue-admin.com)👩‍👦‍👦

- 插件市场：[戳我](https://bbs.django-vue-admin.com/plugMarket.html)👩‍👦‍👦

-  django-vue-admin交流01群(已满)：812482043 [点击链接加入群聊](https://qm.qq.com/cgi-bin/qm/qr?k=aJVwjDvH-Es4MPJQuoO32N0SucK22TE5&jump_from=webapi)
-  django-vue-admin交流02群：687252418  [点击链接加入群聊](https://qm.qq.com/cgi-bin/qm/qr?k=4jJN4IjWGfxJ8YJXbb_gTsuWjR34WLdc&jump_from=webapi)

- 二维码

  <img src='https://images.gitee.com/uploads/images/2022/0530/233203_5fb11883_5074988.jpeg' width='200'>

## 源码地址

gitee地址(主推)：[https://gitee.com/liqianglog/django-vue-admin](https://gitee.com/liqianglog/django-vue-admin)👩‍👦‍👦

github地址：[https://github.com/liqianglog/django-vue-admin](https://github.com/liqianglog/django-vue-admin)👩‍👦‍👦



## 内置功能

1.  👨‍⚕️菜单管理：配置系统菜单，操作权限，按钮权限标识、后端接口权限等。
2.  🧑‍⚕️部门管理：配置系统组织机构（公司、部门、角色）。
3.  👩‍⚕️角色管理：角色菜单权限分配、数据权限分配、设置角色按部门进行数据范围权限划分。
4.  🧑‍🎓权限权限：授权角色的权限范围。
5.  👨‍🎓用户管理：用户是系统操作者，该功能主要完成系统用户配置。
6.  👬接口白名单：配置不需要进行权限校验的接口。
7.  🧑‍🔧字典管理：对系统中经常使用的一些较为固定的数据进行维护。
8.  🧑‍🔧地区管理：对省市县区域进行管理。
9.  📁附件管理：对平台上所有文件、图片等进行统一管理。
10.  🗓️操作日志：系统正常操作日志记录和查询；系统异常信息日志记录和查询。
11.  🔌[插件市场 ](https://bbs.django-vue-admin.com/plugMarket.html)：基于Django-Vue-Admin框架开发的应用和插件。

##  插件市场 🔌

- Celery异步任务：[dvadmin-celery](https://gitee.com/huge-dream/dvadmin-celery)
- 升级中心后端：[dvadmin-upgrade-center](https://gitee.com/huge-dream/dvadmin-upgrade-center)
- 升级中心前端：[dvadmin-upgrade-center-web](https://gitee.com/huge-dream/dvadmin-upgrade-center-web)

## 准备工作
~~~
Python >= 3.8.0 (推荐3.8+版本)
nodejs >= 14.0 (推荐最新)
Mysql >= 5.7.0 (可选，默认数据库sqlite3，推荐8.0版本)
Redis(可选，最新版)
~~~

## 前端♝

```bash
# 克隆项目
git clone https://gitee.com/liqianglog/django-vue-admin.git

# 进入项目目录
cd web

# 安装依赖
npm install --registry=https://registry.npm.taobao.org

# 启动服务
npm run dev
# 浏览器访问 http://localhost:8080
# .env.development 文件中可配置启动端口等参数
# 构建生产环境
# npm run build
```



## 后端💈

~~~bash
1. 进入项目目录 cd backend
2. 在项目根目录中，复制 ./conf/env.example.py 文件为一份新的到 ./conf 文件夹下，并重命名为 env.py
3. 在 env.py 中配置数据库信息
	mysql数据库版本建议：8.0
	mysql数据库字符集：utf8mb4
4. 安装依赖环境
	pip3 install -r requirements.txt
5. 执行迁移命令：
	python3 manage.py makemigrations
	python3 manage.py migrate
6. 初始化数据
	python3 manage.py init
7. 初始化省市县数据:
	python3 manage.py init_area
8. 启动项目
	python3 manage.py runserver 0.0.0.0:8000
或使用 daphne :
  daphne -b 0.0.0.0 -p 8000 application.asgi:application
~~~

### 访问项目

- 访问地址：[http://localhost:8080](http://localhost:8080) (默认为此地址，如有修改请按照配置文件)
- 账号：`superadmin` 密码：`admin123456`





### docker-compose 运行

~~~shell
# 先安装docker-compose (自行百度安装),执行此命令等待安装，如有使用celery插件请打开docker-compose.yml中celery 部分注释
docker-compose up -d
# 初始化后端数据(第一次执行即可)
docker exec -ti dvadmin-django bash
python manage.py makemigrations 
python manage.py migrate
python manage.py init_area
python manage.py init
exit

前端地址：http://127.0.0.1:8080
后端地址：http://127.0.0.1:8080/api
# 在服务器上请把127.0.0.1 换成自己公网ip
账号：superadmin 密码：admin123456

# docker-compose 停止
docker-compose down
#  docker-compose 重启
docker-compose restart
#  docker-compose 启动时重新进行 build
docker-compose up -d --build
~~~



## 演示图✅

![image-01](https://images.gitee.com/uploads/images/2022/0530/234137_b58c8f98_5074988.png)

![image-02](https://images.gitee.com/uploads/images/2022/0530/234240_39834603_5074988.png)

![image-03](https://images.gitee.com/uploads/images/2022/0530/234339_35e728a0_5074988.png)

![image-04](https://images.gitee.com/uploads/images/2022/0530/234426_957036b0_5074988.png)

![image-05](https://images.gitee.com/uploads/images/2022/0530/234458_898be492_5074988.png)

![image-06](https://images.gitee.com/uploads/images/2022/0530/234521_35b40076_5074988.png)

![image-07](https://images.gitee.com/uploads/images/2022/0530/234615_c2325639_5074988.png)

![image-08](https://images.gitee.com/uploads/images/2022/0530/234639_1ed6cc93_5074988.png)

![image-09](https://images.gitee.com/uploads/images/2022/0530/234815_cea2c53f_5074988.png)

![image-10](https://images.gitee.com/uploads/images/2022/0530/234840_5f3e5f53_5074988.png)



