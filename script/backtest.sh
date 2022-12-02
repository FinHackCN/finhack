ps -ef | grep "cmd_backtest.py" | awk {'print $2'} | xargs kill
nohup python -u command/cmd_backtest.py >data/logs/backtest.log &
tail -f data/logs/backtest.log
