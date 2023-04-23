pipreqs ./ --encoding=utf-8 --force
mysqldump --opt -d finhack -u root -p > script/init/finhack.sql
