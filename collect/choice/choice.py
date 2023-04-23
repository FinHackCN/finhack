import sys
from library.config import config
from library.mydb import mydb

from library.globalvar import *
from EmQuantAPI import *
from datetime import timedelta, datetime
import time as _time
import traceback
import hashlib
import pandas as pd

def mainCallback(quantdata):
    """
    mainCallback 是主回调函数，可捕捉如下错误
    在start函数第三个参数位传入，该函数只有一个为c.EmQuantData类型的参数quantdata
    :param quantdata:c.EmQuantData
    :return:
    """
    print ("mainCallback",str(quantdata))
    #登录掉线或者 登陆数达到上线（即登录被踢下线） 这时所有的服务都会停止
    if str(quantdata.ErrorCode) == "10001011" or str(quantdata.ErrorCode) == "10001009":
        print ("Your account is disconnect. You can force login automatically here if you need.")
    #行情登录验证失败（每次连接行情服务器时需要登录验证）或者行情流量验证失败时，会取消所有订阅，用户需根据具体情况处理
    elif str(quantdata.ErrorCode) == "10001021" or str(quantdata.ErrorCode) == "10001022":
        print ("Your all csq subscribe have stopped.")
    #行情服务器断线自动重连连续6次失败（1分钟左右）不过重连尝试还会继续进行直到成功为止，遇到这种情况需要确认两边的网络状况
    elif str(quantdata.ErrorCode) == "10002009":
        print ("Your all csq subscribe have stopped, reconnect 6 times fail.")
    # 行情订阅遇到一些错误(这些错误会导致重连，错误原因通过日志输出，统一转换成EQERR_QUOTE_RECONNECT在这里通知)，正自动重连并重新订阅,可以做个监控
    elif str(quantdata.ErrorCode) == "10002012":
        print ("csq subscribe break on some error, reconnect and request automatically.")
    # 资讯服务器断线自动重连连续6次失败（1分钟左右）不过重连尝试还会继续进行直到成功为止，遇到这种情况需要确认两边的网络状况
    elif str(quantdata.ErrorCode) == "10002014":
        print ("Your all cnq subscribe have stopped, reconnect 6 times fail.")
    # 资讯订阅遇到一些错误(这些错误会导致重连，错误原因通过日志输出，统一转换成EQERR_INFO_RECONNECT在这里通知)，正自动重连并重新订阅,可以做个监控
    elif str(quantdata.ErrorCode) == "10002013":
        print ("cnq subscribe break on some error, reconnect and request automatically.")
    # 资讯登录验证失败（每次连接资讯服务器时需要登录验证）或者资讯流量验证失败时，会取消所有订阅，用户需根据具体情况处理
    elif str(quantdata.ErrorCode) == "10001024" or str(quantdata.ErrorCode) == "10001025":
        print("Your all cnq subscribe have stopped.")
    else:
        pass

class choiceCollecter():
    config={}
    def __init__(self):
        self.config=config.getConfig('choice')
        
        startoptions = "ForceLogin=1" + ",UserName=" + self.config['username'] + ",Password=" + self.config['password'];
        loginResult = c.start(startoptions, '', mainCallback)        
        print(loginResult)
        
        

    def getCSS(self,codes,fields,date,ispandas=0):
        hashstr=codes+fields+date
        md5=hashlib.md5(hashstr.encode(encoding='utf-8')).hexdigest()
        cache_path=CHOICE_CACHE_DIR+'css_'+md5+'_'+date+'.pkl'
        
        
        if os.path.isfile(cache_path):
            # df=pd.read_pickle(cache_path)
            # print(df)
            return
        else:
            data = c.css(codes, fields, "TradeDate="+date+", Ispandas=1")

            if not isinstance(data, c.EmQuantData):
                data.to_pickle(cache_path)
                print(data)
            else:
                if data.ErrorCode==0 and data.Data=={}:
                    return
                
                if(data.ErrorCode != 0):
                    with open("error.txt", mode='a') as f:
                        f.write(hashstr+"request css Error, "+str(data.ErrorMsg)+"\n")
                    print("request css Error, ", data.ErrorMsg)
                else:
                    for code in data.Codes:
                        for i in range(0, len(data.Indicators)):
                            print(data.Data[code][i])


        
    def getCSD(self,codes,fields,start_date,end_date,ispandas=0):
        hashstr=codes+fields+start_date+end_date
        md5=hashlib.md5(hashstr.encode(encoding='utf-8')).hexdigest()
        cache_path=CHOICE_CACHE_DIR+'csd_'+md5+'_'+codes+'.pkl'        
        
        
        if os.path.isfile(cache_path):
            # df=pd.read_pickle(cache_path)
            # print(df)
            return
        else:
            data = c.csd(codes,fields,start_date,end_date,"RowIndex=1,Ispandas=1")

            if not isinstance(data, c.EmQuantData):
                data.to_pickle(cache_path)
                print(data)
            else:
                if data.ErrorCode==0 and data.Data=={}:
                    return
                
                if(data.ErrorCode != 0):
                    with open("error.txt", mode='a') as f:
                        f.write(hashstr+"request css Error, "+str(data.ErrorMsg)+"\n")
                    print("request csd Error, ", data.ErrorMsg)
                    exit()
                else:
                    for code in data.Codes:
                        for i in range(0, len(data.Indicators)):
                            print(data.Data[code][i])
        
    
    def getCTR(self,idx_code='000852.SH',end_date='20230423'):
        hashstr=idx_code+end_date
        md5=hashlib.md5(hashstr.encode(encoding='utf-8')).hexdigest()
        cache_path=CHOICE_CACHE_DIR+'csd_'+md5+'_'+idx_code+'.pkl'        
        
        
        if os.path.isfile(cache_path):
            # df=pd.read_pickle(cache_path)
            # print(df)
            return
        else:
            data=c.ctr("INDEXCOMPOSITION","INDEXCODE,SECUCODE,TRADEDATE","IndexCode=%s,EndDate=%s,Ispandas=1" %(idx_code,end_date))
            if not isinstance(data, c.EmQuantData):
                data.to_pickle(cache_path)
                print(data)
            else:
                if data.ErrorCode==0 and data.Data=={}:
                    return
                
                if(data.ErrorCode != 0):
                    with open("error.txt", mode='a') as f:
                        f.write(hashstr+"request css Error, "+str(data.ErrorMsg)+"\n")
                    print("request csd Error, ", data.ErrorMsg)
                    exit()
                else:
                    for code in data.Codes:
                        for i in range(0, len(data.Indicators)):
                            print(data.Data[code][i])        
        
        
        

        
 
    



