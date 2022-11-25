import pandas as pd
import importlib
import os
import sys
sys.path.append('/data/code/finhack')
from library.backtest import bt





bt_instance=bt.run(cash=10000,strategy_name="aiTopN",data_path="/tmp/lgb_model.pkl")
print(bt_instance['returns'])
print(bt_instance['total_value'])

bt.analyse(instance=bt_instance,benchmark='000001.SH')