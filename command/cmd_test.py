import sys
import os
import time
import random
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from library.astock import AStock
from factors.indicatorCompute import indicatorCompute
from library.mydb import mydb
from library.globalvar import *

 

df=indicatorCompute.computeFactorByStock(ts_code="002624.SZ",factor_name="PPSR_0")
print(df)

