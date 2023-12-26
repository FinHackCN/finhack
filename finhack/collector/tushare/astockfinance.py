import sys
import time
import datetime
import traceback
import pandas as pd

from finhack.library.mydb import mydb
from finhack.library.alert import alert
from finhack.library.monitor import tsMonitor
from finhack.collector.tushare.helper import tsSHelper
import finhack.library.log as Log

class tsAStockFinance:
    
    def getPeriodList(db):
        lastdate_sql="select max(end_date) as max from astock_finance_disclosure_date"
        lastdate=mydb.selectToDf(lastdate_sql,db)
        if(type(lastdate) == bool or lastdate.empty):
            lastdate='19980321'            
        else:
            lastdate=lastdate['max'].tolist()[0]
        
        
        plist=[]
        end_date_list=['0331','0630','0930','1231']
        end_year=time.strftime("%Y", time.localtime())
        for i in range(1999,int(end_year)+1):
            for d in end_date_list:
                p=end_year+str(d)
                if p<lastdate:
                    plist.append(p)
        return plist

    
    def getEndDateListDiff(table,ts_code,db,report_type=0):
        table_sql="select end_date from "+table+" where ts_code='"+ts_code+"'"
        if(report_type>0):
            table_sql=table_sql+" and report_type="+str(report_type)
        table_df=mydb.selectToDf(table_sql,db)
        disclosure_sql="select end_date from astock_finance_disclosure_date where ts_code='"+ts_code+"'  and not ISNULL(actual_date)"
        disclosure_df=mydb.selectToDf(disclosure_sql,db)
        table_list=[]
        disclosure_list=[]
        if(type(table_df) != bool and not table_df.empty):
            table_list=table_df['end_date'].unique().tolist()
       
        if(type(disclosure_df) != bool and not disclosure_df.empty):
            disclosure_list=disclosure_df['end_date'].unique().tolist()
        
        #若无计划披露数据，跑个全量
        if(type(table_df) == bool or table_df.empty):
            disclosure_df=tsAStockFinance.getPeriodList(db)
        elif(type(disclosure_df) == bool or disclosure_df.empty):
            disclosure_df=tsAStockFinance.getPeriodList(db)
            
        diff_list = set(disclosure_list)-set(table_list)
        diff_list=list(diff_list)
        diff_list.sort()
        return diff_list
        
        
  
    def getLastDateCountDiff(table,end_date,ts_code,db,report_type=0):
        table_sql="select * from "+table+" where ts_code='"+ts_code+"' and end_date='"+end_date+"'"
        if(report_type>0):
            table_sql=table_sql+" and report_type="+str(report_type)
        table_res=mydb.selectToDf(table_sql,db)
        if type(table_res) == bool:
            table_count=0
        else:
            table_count=len(table_res)
        disclosure_sql="select * from astock_finance_disclosure_date where ts_code='"+ts_code+"' and end_date='"+end_date+"' and not ISNULL(actual_date)"
        disclosure_res=mydb.selectToDf(disclosure_sql,db)
        disclosure_count=len(disclosure_res)
        #print(str(table_count)+","+str(disclosure_count)+","+ts_code+","+str(report_type))
        return table_count<disclosure_count

   
    
    def getFinance(pro,api,table,fileds,db,report_type=0):
        stock_list_data=tsSHelper.getAllAStock(True,pro,db)
        stock_list=stock_list_data['ts_code'].tolist()
        #stock_list=['002624.SZ']
        #print(len(stock_list))
        #exit()
  
        for ts_code in stock_list:
            if datetime.datetime.now().second==0:
                Log.logger.info(api+","+ts_code+",report_type="+str(report_type))
            diff_list=tsAStockFinance.getEndDateListDiff(table,ts_code,db,report_type)
            #print(diff_list)
            #exit()
            
            lastdate_sql="select max(end_date) as max from "+table+" where ts_code='"+ts_code+"'"
            if(report_type>0):
                lastdate_sql=lastdate_sql+" and report_type="+str(report_type)
            lastdate=mydb.selectToDf(lastdate_sql,db)
            if(type(lastdate) == bool or lastdate.empty):
                lastdate='20000321'
            else:
                lastdate=lastdate['max'].tolist()[0]
            if lastdate==None:
                lastdate='20000321'
            diff_count=tsAStockFinance.getLastDateCountDiff(table,lastdate,ts_code,db,report_type)
            if(diff_count):
                sql="delete from "+table+" where ts_code='"+ts_code+"' and end_date='"+lastdate+"'"
                if(report_type>0):
                    sql=sql+" and report_type="+str(report_type)
                mydb.delete(sql,db)
                diff_list.insert(0,lastdate)
            
            #print(diff_count)
            # print(lastdate)
            #exit()
            
            df=pd.DataFrame()
            engine=mydb.getDBEngine(db)
            
            end_list=[]
            for end_date in diff_list:
                if(lastdate>end_date):
                    continue
                end_list.append(end_date)
                
            if end_list==[]:
                continue
            f = getattr(pro, api)
            try_times=0
            while True:
                try:
                    # print(lastdate)
                    # print(end_date)
                    # print(end_list)
                    # exit()
                    if report_type>0:
                        if len(end_list)>1:
                            df=f(ts_code=ts_code,start_date=end_list[0],end_date=datetime.datetime.now().strftime('%Y%m%d'),fileds=fileds,report_type=report_type)
                        else:
                            df=f(ts_code=ts_code,period=end_list[-1],fileds=fileds,report_type=report_type)
                    #有些api没有report type字段
                    else:
                        if len(end_list)>1:
                            df=f(ts_code=ts_code,start_date=end_list[0],end_date=datetime.datetime.now().strftime('%Y%m%d'),fileds=fileds)
                        else:
                            df=f(ts_code=ts_code,period=end_list[-1],fileds=fileds)
                    df.to_sql(table, engine, index=False, if_exists='append', chunksize=5000)
                    break
                except Exception as e:
                    if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                        Log.logger.warning("api:触发最多访问。\n"+str(e)) 
                        return
                    if "最多访问" in str(e):
                        Log.logger.warning(api+":触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    elif "未知错误" in str(e):
                        info = traceback.format_exc()
                        Log.logger.error(ts_code)
                        Log.logger.error(period)
                        Log.logger.error(fileds)
                        Log.logger.error(report_type)
                        alert.send(api,'未知错误',str(info))
                        Log.logger.error(info)
                        break
                    else:
                        if try_times<10:
                            try_times=try_times+1;
                            Log.logger.error(api+":函数异常，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:
                            info = traceback.format_exc()
                            alert.send(api,'函数异常',str(info))
                            Log.logger.error(info)
                            break
     
    
    @tsMonitor
    def disclosure_date(pro,db):
        end_date_list=['0331','0630','0930','1231']
        engine=mydb.getDBEngine(db)
        lastdate=tsSHelper.getLastDateAndDelete(table='astock_finance_disclosure_date',filed='end_date',ts_code="",db=db)
        api='disclosure_date'
        
        start_year=int(lastdate[0:4])
        start_mounth=int(lastdate[4:6])
        end_year=time.strftime("%Y", time.localtime())
        end_mounth=time.strftime("%m", time.localtime())
        
        end_list=[]
        for year in range(int(start_year),int(end_year)+1):
            for date in end_date_list:
                if(year==int(start_year)):
                    #首年表中最后公告日期比List日期大，则调过
                    if(start_mounth>int(date[0:2])):
                        continue;
                if(year==int(end_year)):
                    #还没到这个月份呢，跳过，先不获取
                    if(int(date[0:2])>int(end_mounth)):
                        continue
                end_list.append(str(year)+date)
        df=None
        for end_date in end_list:
            for i in range(0,100):
                try_times=0
                while True:
                    try:
                        df = pro.disclosure_date(end_date=end_date,limit=1000,offset=1000*i)
                        df.to_sql('astock_finance_disclosure_date', engine, index=False, if_exists='append', chunksize=5000)
                        break
                    except Exception as e:
                        if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                            Log.logger.warning("disclosure_date:触发最多访问。\n"+str(e)) 
                            return
                        if "最多访问" in str(e):
                            Log.logger.warning("disclosure_date:触发限流，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:
                            if try_times<10:
                                try_times=try_times+1;
                                Log.logger.error(api+":函数异常，等待重试。\n"+str(e))
                                time.sleep(15)
                                continue
                            else:
                                info = traceback.format_exc()
                                alert.send(api,'函数异常',str(info))
                                Log.logger.error(info)
                if df.empty:
                    break
        
    
    @tsMonitor
    def income(pro,db):
        fileds="ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit,rd_exp,fin_exp_int_exp,fin_exp_int_inc,transfer_surplus_rese,transfer_housing_imprest,transfer_oth,adj_lossgain,withdra_legal_surplus,withdra_legal_pubfund,withdra_biz_devfund,withdra_rese_fund,withdra_oth_ersu,workers_welfare,distr_profit_shrhder,prfshare_payable_dvd,comshare_payable_dvd,capit_comstock_div,net_after_nr_lp_correct,credit_impa_loss,net_expo_hedging_benefits,oth_impair_loss_assets,total_opcost,amodcost_fin_assets,oth_income,asset_disp_income,continued_net_profit,end_net_profit,update_flag"
        for i in  [1,6]:
            tsAStockFinance.getFinance(pro,'income','astock_finance_income',fileds,db,i)
    
    @tsMonitor
    def balancesheet(pro,db):
        fileds="ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,total_share,cap_rese,undistr_porfit,surplus_rese,special_rese,money_cap,trad_asset,notes_receiv,accounts_receiv,oth_receiv,prepayment,div_receiv,int_receiv,inventories,amor_exp,nca_within_1y,sett_rsrv,loanto_oth_bank_fi,premium_receiv,reinsur_receiv,reinsur_res_receiv,pur_resale_fa,oth_cur_assets,total_cur_assets,fa_avail_for_sale,htm_invest,lt_eqt_invest,invest_real_estate,time_deposits,oth_assets,lt_rec,fix_assets,cip,const_materials,fixed_assets_disp,produc_bio_assets,oil_and_gas_assets,intan_assets,r_and_d,goodwill,lt_amor_exp,defer_tax_assets,decr_in_disbur,oth_nca,total_nca,cash_reser_cb,depos_in_oth_bfi,prec_metals,deriv_assets,rr_reins_une_prem,rr_reins_outstd_cla,rr_reins_lins_liab,rr_reins_lthins_liab,refund_depos,ph_pledge_loans,refund_cap_depos,indep_acct_assets,client_depos,client_prov,transac_seat_fee,invest_as_receiv,total_assets,lt_borr,st_borr,cb_borr,depos_ib_deposits,loan_oth_bank,trading_fl,notes_payable,acct_payable,adv_receipts,sold_for_repur_fa,comm_payable,payroll_payable,taxes_payable,int_payable,div_payable,oth_payable,acc_exp,deferred_inc,st_bonds_payable,payable_to_reinsurer,rsrv_insur_cont,acting_trading_sec,acting_uw_sec,non_cur_liab_due_1y,oth_cur_liab,total_cur_liab,bond_payable,lt_payable,specific_payables,estimated_liab,defer_tax_liab,defer_inc_non_cur_liab,oth_ncl,total_ncl,depos_oth_bfi,deriv_liab,depos,agency_bus_liab,oth_liab,prem_receiv_adva,depos_received,ph_invest,reser_une_prem,reser_outstd_claims,reser_lins_liab,reser_lthins_liab,indept_acc_liab,pledge_borr,indem_payable,policy_div_payable,total_liab,treasury_share,ordin_risk_reser,forex_differ,invest_loss_unconf,minority_int,total_hldr_eqy_exc_min_int,total_hldr_eqy_inc_min_int,total_liab_hldr_eqy,lt_payroll_payable,oth_comp_income,oth_eqt_tools,oth_eqt_tools_p_shr,lending_funds,acc_receivable,st_fin_payable,payables,hfs_assets,hfs_sales,cost_fin_assets,fair_value_fin_assets,cip_total,oth_pay_total,long_pay_total,debt_invest,oth_debt_invest,oth_eq_invest,oth_illiq_fin_assets,oth_eq_ppbond,receiv_financing,use_right_assets,lease_liab,contract_assets,contract_liab,accounts_receiv_bill,accounts_pay,oth_rcv_total,fix_assets_total,update_flag"
        for i in [1,6]:
            tsAStockFinance.getFinance(pro,'balancesheet','astock_finance_balancesheet',fileds,db,i)
    
    @tsMonitor
    def cashflow(pro,db):
        fileds=""
        for i in  [1,6]:
            tsAStockFinance.getFinance(pro,'cashflow','astock_finance_cashflow',fileds,db,i)
    
    @tsMonitor
    def forecast(pro,db):
        fileds=""
        tsAStockFinance.getFinance(pro,'forecast','astock_finance_forecast',fileds,db)
    
    @tsMonitor
    def express(pro,db):
        fileds=""
        tsAStockFinance.getFinance(pro,'express','astock_finance_express',fileds,db)
    
    @tsMonitor
    def dividend(pro,db):
        engine=mydb.getDBEngine(db)
        table='astock_finance_dividend'
        mydb.exec("drop table if exists "+table+"_tmp",db)
        stock_list_data=tsSHelper.getAllAStock(True,pro,db)
        stock_list=stock_list_data['ts_code'].tolist()
        for ts_code in stock_list:
            try_times=0
            while True:
                try:
                    df = pro.dividend(ts_code=ts_code)
                    df.to_sql('astock_finance_dividend_tmp', engine, index=False, if_exists='append', chunksize=5000)
                    break
                except Exception as e:
                    if "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                        Log.logger.warning("dividend:触发最多访问。\n"+str(e)) 
                        return
                    if "最多访问" in str(e):
                        Log.logger.warning("dividend:触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    else:
                        if try_times<10:
                            try_times=try_times+1;
                            Log.logger.warning("dividend:函数异常，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:
                            info = traceback.format_exc()
                            alert.send('dividend','函数异常',str(info))
                            Log.logger.error(info)
        mydb.exec('rename table '+table+' to '+table+'_old;',db);
        mydb.exec('rename table '+table+'_tmp to '+table+';',db);
        mydb.exec("drop table if exists "+table+'_old',db)
        tsSHelper.setIndex(table,db)
            
    @tsMonitor
    def fina_indicator(pro,db):
        fileds="ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,capital_rese_ps,surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,gross_margin,current_ratio,quick_ratio,cash_ratio,invturn_days,arturn_days,inv_turn,ar_turn,ca_turn,fa_turn,assets_turn,op_income,valuechange_income,interst_income,daa,ebit,ebitda,fcff,fcfe,current_exint,noncurrent_exint,interestdebt,netdebt,tangible_asset,working_capital,networking_capital,invest_capital,retained_earnings,diluted2_eps,bps,ocfps,retainedps,cfps,ebit_ps,fcff_ps,fcfe_ps,netprofit_margin,grossprofit_margin,cogs_of_sales,expense_of_sales,profit_to_gr,saleexp_to_gr,adminexp_of_gr,finaexp_of_gr,impai_ttm,gc_of_gr,op_of_gr,ebit_of_gr,roe,roe_waa,roe_dt,roa,npta,roic,roe_yearly,roa2_yearly,roe_avg,opincome_of_ebt,investincome_of_ebt,n_op_profit_of_ebt,tax_to_ebt,dtprofit_to_profit,salescash_to_or,ocf_to_or,ocf_to_opincome,capitalized_to_da,debt_to_assets,assets_to_eqt,dp_assets_to_eqt,ca_to_assets,nca_to_assets,tbassets_to_totalassets,int_to_talcap,eqt_to_talcapital,currentdebt_to_debt,longdeb_to_debt,ocf_to_shortdebt,debt_to_eqt,eqt_to_debt,eqt_to_interestdebt,tangibleasset_to_debt,tangasset_to_intdebt,tangibleasset_to_netdebt,ocf_to_debt,ocf_to_interestdebt,ocf_to_netdebt,ebit_to_interest,longdebt_to_workingcapital,ebitda_to_debt,turn_days,roa_yearly,roa_dp,fixed_assets,profit_prefin_exp,non_op_profit,op_to_ebt,nop_to_ebt,ocf_to_profit,cash_to_liqdebt,cash_to_liqdebt_withinterest,op_to_liqdebt,op_to_debt,roic_yearly,total_fa_trun,profit_to_op,q_opincome,q_investincome,q_dtprofit,q_eps,q_netprofit_margin,q_gsprofit_margin,q_exp_to_sales,q_profit_to_gr,q_saleexp_to_gr,q_adminexp_to_gr,q_finaexp_to_gr,q_impair_to_gr_ttm,q_gc_to_gr,q_op_to_gr,q_roe,q_dt_roe,q_npta,q_opincome_to_ebt,q_investincome_to_ebt,q_dtprofit_to_profit,q_salescash_to_or,q_ocf_to_sales,q_ocf_to_or,basic_eps_yoy,dt_eps_yoy,cfps_yoy,op_yoy,ebt_yoy,netprofit_yoy,dt_netprofit_yoy,ocf_yoy,roe_yoy,bps_yoy,assets_yoy,eqt_yoy,tr_yoy,or_yoy,q_gr_yoy,q_gr_qoq,q_sales_yoy,q_sales_qoq,q_op_yoy,q_op_qoq,q_profit_yoy,q_profit_qoq,q_netprofit_yoy,q_netprofit_qoq,equity_yoy,rd_exp,update_flag"
        tsAStockFinance.getFinance(pro,'fina_indicator','astock_finance_indicator',fileds,db)
    
    @tsMonitor
    def fina_audit(pro,db):
        fileds=""
        tsAStockFinance.getFinance(pro,'fina_audit','astock_finance_audit',fileds,db)
    
    @tsMonitor
    def fina_mainbz(pro,db):
        fileds=""
        tsAStockFinance.getFinance(pro,'fina_mainbz','astock_finance_mainbz',fileds,db)

    @tsMonitor
    def top10_holders(pro,db):
        fileds=""
        tsAStockFinance.getFinance(pro,'top10_holders','astock_market_top10_holders',fileds,db)
        

    @tsMonitor
    def top10_floatholders(pro,db):
        fileds=""
        tsAStockFinance.getFinance(pro,'top10_floatholders','astock_market_top10_floatholders',fileds,db)