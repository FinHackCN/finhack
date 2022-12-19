ps -ef | grep "cmd_retrain.py" | awk {'print $2'} | xargs kill
nohup python -u command/cmd_retrain.py >data/logs/retrain.log &
tail -f data/logs/retrain.log
