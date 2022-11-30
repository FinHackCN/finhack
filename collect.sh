ps -ef | grep "cmd_collect.py" | awk {'print $2'} | xargs kill
nohup python -u command/cmd_collect.py >data/logs/collect.log &
tail -f data/logs/collect.log
