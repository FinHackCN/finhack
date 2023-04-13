#ps -ef | grep cmd_mining.py | awk {'print $2'} | xargs kill
ps -ef | grep cmd_autotrain.py | awk {'print $2'} | xargs kill
# ps -ef | grep cmd_backtest.py | awk {'print $2'} | xargs kill
# ps -ef | grep cmd_hyperparam.py | awk {'print $2'} | xargs kill
