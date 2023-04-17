#/root/anaconda3/bin/python -u /data/code/finhack/command/cmd_backtest.py > /data/code/finhack/data/logs/backtest.log &
#/root/anaconda3/bin/python -u /data/code/finhack/command/cmd_mining.py > /data/code/finhack/data/logs/mining.log &
#/root/anaconda3/bin/python -u /data/code/finhack/command/cmd_autotrain.py > /data/code/finhack/data/logs/aotutrain.log &

nohup ./script/service/guard.sh  >/dev/null 2>&1 &