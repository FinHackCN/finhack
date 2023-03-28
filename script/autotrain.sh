ps -ef | grep "cmd_autotrain.py" | awk {'print $2'} | xargs kill
#nohup python -u command/cmd_autotrain.py 5 10 >>data/logs/autotrain.log  &
nohup python -u command/cmd_autotrain.py 10 20 >>data/logs/autotrain.log  &
nohup python -u command/cmd_autotrain.py 10 20 >>data/logs/autotrain.log  &
#nohup python -u command/cmd_autotrain.py 20 30 >>data/logs/autotrain.log  &
# nohup python -u command/cmd_autotrain.py >data/logs/autotrain.log &
# nohup python -u command/cmd_autotrain.py >>data/logs/autotrain.log &
tail -f data/logs/autotrain.log
