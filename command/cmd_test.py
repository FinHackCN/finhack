import sys
import os
import time
import random
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from library.astock import AStock
from factors.indicatorCompute import indicatorCompute
df=AStock.getStockDailyPriceByCode(code='000796.sz',fq='qfq',cache=False)
print(df)