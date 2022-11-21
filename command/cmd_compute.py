import sys
import os
import time
import shutil
#compute.test()
#compute.computeAll('tushare')
t1=time.time()
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from factors.compute import compute
# compute.computeList('rim',['rimn_0','rimi_0','rimv_0','rimve_0','rimvm_0','rimvp_0','rimvep_0','rimvmp_0','rimrc_0'])



if os.path.exists('/home/woldy/finhack/FinHack-Factors/data/factor_tmp'):
    shutil.rmtree('/home/woldy/finhack/FinHack-Factors/data/factor_tmp')
os.makedirs('/home/woldy/finhack/FinHack-Factors/data/factor_tmp')


compute.computeAll('tushare')

#compute.computeList('test',['MINIDX_0','MAXIDX_0','alpha191-095_0','COSH_0','EXP_0','SINH_0','alpha191-132_0','MA90_0','MIDPOINT_0','MIDPRICE_0','TRIMA_0','OBV_0','MIN_0','SUM_0'])


#compute.computeList('choose',['pe_0','pe_ttm_0','pb'])

#compute.computeFactorByName('dif_10_16_10')


shutil.move('/home/woldy/finhack/FinHack-Factors/data/factor','/home/woldy/finhack/FinHack-Factors/data/factor_old')
shutil.move('/home/woldy/finhack/FinHack-Factors/data/factor_tmp','/home/woldy/finhack/FinHack-Factors/data/factor')
print(time.time()-t1)
