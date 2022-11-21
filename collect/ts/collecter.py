import sys
from library.config import config
from library.mydb import mydb
from collect.ts.astockbasic import tsAStockBasic
from collect.ts.astockprice import tsAStockPrice
from collect.ts.astockfinance import tsAStockFinance
from collect.ts.astockindex import tsAStockIndex
from collect.ts.astockother import tsAStockOther
from collect.ts.astockmarket import tsAStockMarket
from collect.ts.futures import tsFuntures
from collect.ts.fund import tsFund
from collect.ts.other import tsOther
from collect.ts.econo import tsEcono
from collect.ts.ustock import tsUStock
from collect.ts.hstock import tsHStock
from collect.ts.cb import tsCB
from collect.ts.fx import tsFX
from collect.ts.helper import tsSHelper

from library.thread import collectThread
import tushare as ts


class tsCollecter:
    def __init__(self):
        cfgTS=config.getConfig('ts')
        ts.set_token(cfgTS['token'])
        self.pro = ts.pro_api()
        self.db=cfgTS['db']
        self.engine=mydb.getDBEngine(cfgTS['db'])
        self.thread_list = []
        
        
    def getAll(self):
        #tsCB.cb_share(pro=self.pro,db=self.db)
        self.getAStockBasic()
        self.getAStockPrice()
        self.getAStockFinance()
        self.getAStockMarket()
        self.getAStockIndex()
        self.getAStockOther()
        self.getFund()
        self.getEcono()
        self.getOther()
        
        self.getFutures()
        
        self.getUStock()
        self.getHStock()
        self.getCB()
        self.getFX()
        
        
        for t in self.thread_list:
            t.setDaemon(True)
            t.start()

        for t in self.thread_list:
            t.join()
    
        cfgTS=config.getConfig('ts')
        db=cfgTS['db']
        
        tables_list=mydb.selectToList('show tables',db)
        for v in tables_list:
            table=list(v.values())[0]
            tsSHelper.setIndex(table,db)    
    
        

    def getAStockBasic(self):
        tsAStockBasic.stock_basic(self.pro,self.db)
        tsAStockBasic.trade_cal(self.pro,self.db)
        self.mTread(tsAStockBasic,'namechange')
        self.mTread(tsAStockBasic,'hs_const')
        self.mTread(tsAStockBasic,'stock_company')
        self.mTread(tsAStockBasic,'stk_managers')
        self.mTread(tsAStockBasic,'stk_rewards')
        self.mTread(tsAStockBasic,'new_share')


    def getAStockPrice(self):
        self.mTread(tsAStockPrice,'daily')
        self.mTread(tsAStockPrice,'weekly')
        self.mTread(tsAStockPrice,'monthly')
        self.mTread(tsAStockPrice,'adj_factor')
        self.mTread(tsAStockPrice,'suspend_d')
        self.mTread(tsAStockPrice,'daily_basic')
        self.mTread(tsAStockPrice,'moneyflow')
        self.mTread(tsAStockPrice,'stk_limit')
        self.mTread(tsAStockPrice,'limit_list')
        self.mTread(tsAStockPrice,'moneyflow_hsgt')
        self.mTread(tsAStockPrice,'hsgt_top10')
        self.mTread(tsAStockPrice,'ggt_top10')
        self.mTread(tsAStockPrice,'hk_hold')
        self.mTread(tsAStockPrice,'ggt_daily')
        

        # self.mTread(tsAStockPrice,'ggt_monthly')
        # self.mTread(tsAStockPrice,'ccass_hold_detail')
        
    def getAStockFinance(self):
        tsAStockFinance.disclosure_date(self.pro,self.db)
        self.mTread(tsAStockFinance,'income')
        self.mTread(tsAStockFinance,'balancesheet')
        self.mTread(tsAStockFinance,'cashflow')
        self.mTread(tsAStockFinance,'forecast')
        self.mTread(tsAStockFinance,'express')
        self.mTread(tsAStockFinance,'fina_indicator')
        self.mTread(tsAStockFinance,'fina_audit')
        self.mTread(tsAStockFinance,'fina_mainbz')
        self.mTread(tsAStockFinance,'dividend')

       
    def getAStockMarket(self):
        self.mTread(tsAStockMarket,'margin')
        self.mTread(tsAStockMarket,'margin_detail')
        self.mTread(tsAStockMarket,'top_list')
        self.mTread(tsAStockMarket,'top_inst')
        self.mTread(tsAStockMarket,'pledge_stat')
        self.mTread(tsAStockMarket,'pledge_detail')
        self.mTread(tsAStockMarket,'repurchase')
        self.mTread(tsAStockMarket,'concept')
        self.mTread(tsAStockMarket,'concept_detail')
        self.mTread(tsAStockMarket,'share_float')
        self.mTread(tsAStockMarket,'block_trade')
        self.mTread(tsAStockMarket,'stk_holdernumber')
        self.mTread(tsAStockMarket,'stk_holdertrade')
        self.mTread(tsAStockFinance,'top10_holders')
        self.mTread(tsAStockFinance,'top10_floatholders')
        pass
     
    def getAStockIndex(self):
        tsAStockIndex.index_basic(self.pro,self.db)
        tsAStockIndex.index_classify(self.pro,self.db)

        self.mTread(tsAStockIndex,'index_daily')
        self.mTread(tsAStockIndex,'index_weekly')
        self.mTread(tsAStockIndex,'index_monthly')
        self.mTread(tsAStockIndex,'index_weight')
        self.mTread(tsAStockIndex,'index_dailybasic')
        self.mTread(tsAStockIndex,'index_member')
        self.mTread(tsAStockIndex,'daily_info')
        self.mTread(tsAStockIndex,'sz_daily_info')
        pass

    def getAStockOther(self):
        self.mTread(tsAStockOther,'report_rc')
        self.mTread(tsAStockOther,'cyq_perf')
        self.mTread(tsAStockOther,'cyq_chips')
        #broker_recommend
    
    def getFund(self):
        tsFund.fund_basic(self.pro,self.db)
        self.mTread(tsFund,'fund_company')
        self.mTread(tsFund,'fund_manager')
        self.mTread(tsFund,'fund_share')
        self.mTread(tsFund,'fund_nav')
        self.mTread(tsFund,'fund_div')
        self.mTread(tsFund,'fund_portfolio')
        self.mTread(tsFund,'fund_daily')
        self.mTread(tsFund,'fund_adj')
        pass

    def getEcono(self):
        self.mTread(tsEcono,'shibor')
        self.mTread(tsEcono,'shibor_quote')
        self.mTread(tsEcono,'shibor_lpr')
        # self.mTread(tsEcono,'libor')
        # self.mTread(tsEcono,'hibor')
        self.mTread(tsEcono,'wz_index')
        self.mTread(tsEcono,'gz_index')
        self.mTread(tsEcono,'cn_gdp')
        self.mTread(tsEcono,'cn_cpi')
        self.mTread(tsEcono,'cn_ppi')
        self.mTread(tsEcono,'cn_m')
        self.mTread(tsEcono,'us_tycr')
        self.mTread(tsEcono,'us_trycr')
        self.mTread(tsEcono,'us_tbr')
        self.mTread(tsEcono,'us_tltr')
        self.mTread(tsEcono,'us_trltr')
        self.mTread(tsEcono,'eco_cal')
        pass

    def getFutures(self):
        self.mTread(tsFuntures,'fut_basic')
        self.mTread(tsFuntures,'trade_cal')
        self.mTread(tsFuntures,'fut_daily')
        self.mTread(tsFuntures,'fut_holding')
        pass

    def getUStock(self):
        tsUStock.us_basic(self.pro,self.db)
        self.mTread(tsUStock,'us_tradecal')
        self.mTread(tsUStock,'us_daily')
        pass

    def getHStock(self):
        tsHStock.hk_basic(self.pro,self.db)
        self.mTread(tsHStock,'hk_tradecal')
        self.mTread(tsHStock,'hk_daily')
        pass

    def getCB(self):
        tsCB.cb_basic(self.pro,self.db)
        self.mTread(tsCB,'cb_issue')
        self.mTread(tsCB,'cb_call')
        self.mTread(tsCB,'cb_daily')
        #elf.mTread(tsCB,'cb_price_chg')
        pass    
    
    def getFX(self):
        tsFX.fx_basic(self.pro,self.db)
        self.mTread(tsFX,'fx_daily')
        pass   

    def getOther(self):
        #self.mTread(tsOther,'cctv_news')
        pass
    
    def mTread(self,className,functionName):
        thread = collectThread(className,functionName,self.pro,self.db)
        print(functionName)
        self.thread_list.append(thread)


    

       



