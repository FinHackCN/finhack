ps -ef | grep "cmd_hyperparam.py" | awk {'print $2'} | xargs kill
nohup python -u command/cmd_hyperparam.py >data/logs/hyperparam.log &
tail -f data/logs/hyperparam.log
