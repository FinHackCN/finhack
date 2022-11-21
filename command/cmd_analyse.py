import sys
import os
import time
import random
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from library.config import config
from library.mydb import mydb
from library.astock import AStock

from factors.factorAnalyzer import factorAnalyzer
from factors.factorManager import factorManager
 
t1=time.time()


factors=factorManager.getFactorsList()
#random.shuffle(factors)
for factor in factors:
    try:
        analysis=factorAnalyzer.analys(factor)
    except Exception as e:
        print(factor+"error:"+str(e))
print(time.time()-t1)


