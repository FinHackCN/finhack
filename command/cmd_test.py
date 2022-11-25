import sys
import os
import time
import random
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from library.astock import AStock

df=AStock.getStockDailyPriceByCode(code="000552.sz",cache=False)
print(df)