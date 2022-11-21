import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(__file__))+"/collect")
sys.path.append(os.path.dirname(os.path.dirname(__file__))+"/collect/ts")

import datetime
from library.alert import alert
from collect.ts.collecter import tsCollecter

starttime = datetime.datetime.now()
c_ts=tsCollecter()
c_ts.getAll()
endtime = datetime.datetime.now()
print ("------ Tushare数据同步完毕，共耗时:"+str(endtime - starttime)+"------")
alert.send('tsCollecter','同步完毕',"Tushare数据同步完毕，共耗时:"+str(endtime - starttime))