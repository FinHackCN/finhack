import sys
import os
import time
import shutil
#compute.test()
#compute.computeAll('tushare')
t1=time.time()
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from train.lgbtrain import lgbtrain



def auto_lgbtrain():
    lgbtrain.run()
    

auto_lgbtrain()

print(time.time()-t1)