ps -ef | grep "cmd_autotrain.py" | awk {'print $2'} | xargs kill
nohup python -u command/cmd_autotrain.py >data/logs/autotrain.log &
nohup python -u command/cmd_autotrain.py >>data/logs/autotrain.log &
nohup python -u command/cmd_autotrain.py >data/logs/autotrain.log &
nohup python -u command/cmd_autotrain.py >>data/logs/autotrain.log &
tail -f data/logs/autotrain.log
