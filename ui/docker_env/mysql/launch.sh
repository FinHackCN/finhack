cur_path=`pwd`
docker rm mysql
# docker pull mysql:5.7
# docker pull mysql:5.5
docker run -p 3306:3306 --privileged=true --name mysql -v $cur_path/logs:/logs -v $cur_path/data:/var/lib/mysql -v $cur_path/conf.d/my.cnf:/etc/mysql/mysql.conf.d/mysqld.cnf -v $cur_path/run/:/var/run/mysql -e MYSQL_ROOT_PASSWORD=q1w2e3r4 -d mysql:5.7
