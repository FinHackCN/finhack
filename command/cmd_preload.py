import sys
import os
sys.path.append("..")
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from library.astock import AStock

# cache_path=os.path.dirname(os.path.dirname(__file__))+"/cache/price/"

# df=AStock.getStockDailyPrice()
# df.to_pickle(cache_path+'all')
#TODO