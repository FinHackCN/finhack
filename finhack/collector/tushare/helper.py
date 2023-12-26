import re
import sys
import time
import datetime
import traceback
import pandas as pd

from finhack.library.mydb import mydb
from finhack.library.alert import alert
from finhack.library.monitor import tsMonitor
import finhack.library.log as Log

class tsSHelper:
    
    def getAllAStockIndex(pro=None,db='default'):
        sql='select * from astock_index_basic'
        data=mydb.selectToDf(sql,db)
        return data
    
    def getAllAStock(fromDB=True,pro=None,db='default'):
        if fromDB:
            sql='select * from astock_basic'
            data=mydb.selectToDf(sql,db)
             
        else:
            all_stock=[]
            data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
            all_stock.append(data)
            data = pro.stock_basic(exchange='', list_status='D', fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
            all_stock.append(data)
            data = pro.stock_basic(exchange='', list_status='P', fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
            all_stock.append(data)
            data=pd.concat(all_stock,axis=0,ignore_index=True)
        return data
  
  
    def setIndex(table,db='default'):
        index_list=['ts_code','end_date','trade_date']
        for index in index_list:
            sql="CREATE INDEX "+index+" ON "+table+" ("+index+"(10)) "
            mydb.exec(sql,db)
  
    def getAllFund(db='default'):
        sql='select * from fund_basic'
        data=mydb.selectToDf(sql,db)
        return data      
       
    #重新获取数据 
    def getDataAndReplace(pro,api,table,db):
        mydb.exec("drop table if exists "+table+"_tmp",db)
        #mydb.truncateTable(table,db)
        engine=mydb.getDBEngine(db)
        f = getattr(pro, api)
        data = f()
        data.to_sql(table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
        mydb.exec('rename table '+table+' to '+table+'_old;',db);
        mydb.exec('rename table '+table+'_tmp to '+table+';',db);
        mydb.exec("drop table if exists "+table+'_old',db)
        tsSHelper.setIndex(table,db)
    
    
    #根据最后日期获取数据
    def getDataWithLastDate(pro,api,table,db,filed='trade_date',ts_code=''):
        engine=mydb.getDBEngine(db)
        lastdate=tsSHelper.getLastDateAndDelete(table=table,filed=filed,ts_code=ts_code,db=db)
        begin = datetime.datetime.strptime(lastdate, "%Y%m%d")
        end = datetime.datetime.now()
        i=0
        while i<(end - begin).days+1:
            day = begin + datetime.timedelta(days=i)
            day=day.strftime("%Y%m%d")
            f = getattr(pro, api)
            try_times=0
            while True:
                try:
                    df=pd.DataFrame()
                    if(ts_code==''):
                        if filed=='trade_date':
                            df=f(trade_date=day)
                        elif( filed=='ann_date'):
                            df=f(ann_date=day)
                        elif(filed=='end_date'):
                            df=f(end_date=day)
                        elif(filed=='date'):
                            df=f(date=day)
                        elif(filed=='nav_date'):
                            df=f(nav_date=day)
                        elif(filed=='cal_date'):
                            df=f(cal_date=day)
                        else:
                            alert.send(api,'函数异常',filed+"未处理")
                    else:
                        if filed=='trade_date':
                            df=f(trade_date=day,ts_code=ts_code)
                        elif( filed=='ann_date'):
                            df=f(ann_date=day,ts_code=ts_code)
                        elif(filed=='end_date'):
                            df=f(end_date=day,ts_code=ts_code)  
                        elif(filed=='date'):
                            df=f(date=day,ts_code=ts_code)  
                        elif(filed=='nav_date'):
                            df=f(nav_date=day,ts_code=ts_code)
                        elif(filed=='cal_date'):
                            df=f(cal_date=day)
                        else:
                            alert.send(api,'函数异常',filed+"未处理")
                    
                        
                        
                    if(not df.empty):
                        res = df.to_sql(table, engine, index=False, if_exists='append', chunksize=5000)
                    break
                except Exception as e:
                    if "每分钟最多访问" in str(e):
                        Log.logger.warning(api+":触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    
                    if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                        Log.logger.warning(api+":今日权限用完。\n"+str(e))
                        return
                         
                   
                    elif "您没有访问该接口的权限" in str(e):
                        Log.logger.warning(api+":没有访问该接口的权限。\n"+str(e))
                        return
                    
                    else:
                        if try_times<10:
                            try_times=try_times+1;
                            Log.logger.error(api+":函数异常，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:                        
                            info = traceback.format_exc()
                            alert.send(api,'函数异常',str(info))
                            
                            Log.logger.error(api+"\n"+info)
                            return
            #print(table+'-'+str(len(df))+'-'+day)

            i=i+1        
            
    
    def getDataWithCodeAndClear(pro,api,table,db):
        #mydb.truncateTable(table,db)
        mydb.exec("drop table if exists "+table+"_tmp",db)
        engine=mydb.getDBEngine(db)
        data=tsSHelper.getAllAStock(True,pro,db)
        stock_list=data['ts_code'].tolist()
        f = getattr(pro, api)
        for code in stock_list:
            try_times=0
            while True:
                try:
                    df =f(ts_code=code)
                    df.to_sql(table+"_tmp", engine, index=False, if_exists='append', chunksize=5000)
                    break
                except Exception as e:
                    if "每分钟最多访问" in str(e):
                        Log.logger.warning(api+":触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    
                    if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                        Log.logger.warning(api+":今日权限用完。\n"+str(e))
                        return
                   
                    elif "您没有访问该接口的权限" in str(e):
                        Log.logger.warning(api+":没有访问该接口的权限。\n"+str(e))
                        return
                    
                    else:
                        if try_times<10:
                            try_times=try_times+1;
                            Log.logger.error(api+":函数异常，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:                            
                            info = traceback.format_exc()
                            alert.send(api,'函数异常',str(info))
                            Log.logger.error(str(info))
                            return
     
        mydb.exec('rename table '+table+' to '+table+'_old;',db);
        mydb.exec('rename table '+table+'_tmp to '+table+';',db);
        mydb.exec("drop table if exists "+table+'_old',db)
        tsSHelper.setIndex(table,db)
        
    
    #查一下最后的数据是哪天
    def getLastDateAndDelete(table,filed,ts_code="",db='default'):
        db,cursor=mydb.getDB(db)
        sql = "show tables"
        cursor.execute(sql)
        tables = cursor.fetchall()
        tables_list = re.findall('(\'.*?\')',str(tables))
        tables_list = [re.sub("'",'',each)for each in tables_list]
        #tables_list=[table]
        lastdate="20000101"
        data=[lastdate];
        sql=""
        try:
            if table in tables_list:
                
                if ts_code!="":
                    if filed=="":#有代码无日期字段，直接删掉代码对应记录
                        sql="delete  from "+table+" where ts_code=\""+ts_code+"\""
                        cursor.execute(sql)
                    else:#有代码有日期字段，找出最大的字段并删除
                        sql="select max("+filed+")  as res from "+table+" where ts_code=\""+ts_code+"\""
                        cursor.execute(sql)
                        data = cursor.fetchone()
                        if data['res']!=None:
                            lastdate=data['res']
                            sql="delete  from "+table+" where "+filed+"=\""+data['res']+"\" and ts_code=\""+ts_code+"\""
                            Log.logger.debug(sql)
                            cursor.execute(sql)
                    db.commit()
                else:#没代码，找出最大字段
                    sql="select max("+filed+")  as res from "+table
                    cursor.execute(sql)
                    data = cursor.fetchone()
                    if data['res']!=None:
                        sql="delete  from "+table+" where "+filed+"=\""+data['res']+"\""
                        cursor.execute(sql)
                        db.commit()
                        lastdate=data['res']
            else:
                pass
        except Exception as e:
            Log.logger.error("MySQL max Error:%s" % sql)
            info = traceback.format_exc()
            alert.send('GetLast','函数异常',sql+"\n"+str(info))
            Log.logger.error(info)
            db.close()
            return lastdate
        db.close()
        return lastdate