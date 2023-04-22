rm -f data/cache/price/*
ps -ef | grep task | awk {'print $2'} | xargs kill
nohup python -u command/cmd_task.py > data/logs/task.log &
tail -f data/logs/task.log
