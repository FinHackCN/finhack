import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(__file__))+"/collect")
sys.path.append(os.path.dirname(os.path.dirname(__file__))+"/collect/ts")
sys.path.append(os.path.dirname(os.path.dirname(__file__))+"/collect/choice")

import datetime
from library.alert import alert
from collect.ts.collecter import tsCollecter

starttime = datetime.datetime.now()
c_ts=tsCollecter()
c_ts.getAll()
endtime = datetime.datetime.now()
print ("------ Tushare数据同步完毕，共耗时:"+str(endtime - starttime)+"------")
alert.send('tsCollecter','同步完毕',"Tushare数据同步完毕，共耗时:"+str(endtime - starttime))
exit()



from astock.astock import AStock
from library.mydb import mydb
from datetime import datetime

from library.globalvar import *
from collect.choice.choice import choiceCollecter


collecter=choiceCollecter()


codes_df=mydb.selectToDf('SELECT ts_code FROM `tushare`.`astock_basic`','tushare')
all_codes=codes_df['ts_code'].values
all_codes=list(set(all_codes))
all_codes.sort(reverse=False)
#

today=datetime.today().strftime('%Y%m%d')

trade_date_df=mydb.selectToDf("select cal_date  FROM `tushare`.`astock_trade_cal` where is_open=1 and cal_date<'%s'  order by cal_date desc " % today,'tushare')
trade_dates=trade_date_df['cal_date'].values
trade_dates=list(set(trade_dates))
trade_dates.sort(reverse=True)

for trade_date in trade_dates:
    print(trade_date)
    for i in range(0,len(all_codes),1000):
        codes=all_codes[i:i+1000]
        codes=','.join(codes)
        collecter.getCSS(codes,'INPVOLUME,OUTPVOLUME',trade_date)



# for code in all_codes:
#     collecter.getCSD(code,'HIGHLIMIT,LOWLIMIT',"2000-01-01","2023-05-14")



# data=c.csd("000002.SZ","HIGHLIMIT,LOWLIMIT","2023-05-14","2023-05-14","period=1,adjustflag=1,curtype=1,order=1,market=CNSESH")



for trade_date in trade_dates:
    collecter.getCTR('000852.SH',trade_date)
