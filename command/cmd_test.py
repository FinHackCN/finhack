import sys
import os
import time
import random
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from library.astock import AStock
from factors.indicatorCompute import indicatorCompute
from library.mydb import mydb



df=indicatorCompute.computeListByStock(ts_code="689009.SH",list_name='none',where='',factor_list=['pe_0','totalMv_0'],pure=True,check=True)
print(df)