import sys
import time
import datetime
import traceback
import pandas as pd
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from finhack.library.db import DB
from finhack.library.alert import alert
from finhack.library.monitor import tsMonitor
from finhack.collector.tushare.helper import tsSHelper
import finhack.library.log as Log

class tsAStockFinanceVIP:
    """
    Tushare财务数据VIP版本收集器
    实现按季度获取数据的逻辑：
    1. 获取max的end_date并删除
    2. 按年度+季度逐个获取下一个end_date
    """
    
    @staticmethod
    def get_quarter_end_dates():
        """获取季度结束日期列表"""
        return ['0331', '0630', '0930', '1231']
    
    @staticmethod
    def get_next_quarter_periods(current_year=None):
        """
        获取需要更新的季度期间列表
        格式：YYYYMMDD (如20231231)
        """
        if current_year is None:
            current_year = datetime.datetime.now().year
        
        periods = []
        quarter_ends = tsAStockFinanceVIP.get_quarter_end_dates()
        
        # 从2000年开始到当前年份
        for year in range(2000, current_year + 1):
            for quarter_end in quarter_ends:
                period = f"{year}{quarter_end}"
                periods.append(period)
        
        return periods
    
    @staticmethod
    def get_max_end_date_from_table(table_name, db='default'):
        """
        从指定表获取最大的end_date
        """
        try:
            sql = f"select max(end_date) as max_end_date from {table_name}"
            result = DB.select_to_df(sql, db)
            
            if result is not None and not result.empty and result['max_end_date'].iloc[0] is not None:
                return result['max_end_date'].iloc[0]
            else:
                return None
        except Exception as e:
            Log.logger.warning(f"获取表{table_name}最大end_date失败: {str(e)}")
            return None
    
    @staticmethod
    def delete_max_end_date_records(table_name, max_end_date, db='default'):
        """
        删除最大end_date的记录
        """
        try:
            sql = f"delete from {table_name} where end_date = '{max_end_date}'"
            DB.delete(sql, db)
            Log.logger.info(f"已删除表{table_name}中end_date为{max_end_date}的记录")
            return True
        except Exception as e:
            Log.logger.error(f"删除表{table_name}中end_date为{max_end_date}的记录失败: {str(e)}")
            return False
    
    @staticmethod
    def get_periods_to_update(table_name, db='default', batch_size=20):
        """
        获取需要更新的期间列表
        逻辑：获取所有季度期间，排除已存在的期间，按时间顺序返回
        
        Args:
            table_name: 表名
            db: 数据库名称
            batch_size: 每批处理的期间数量，避免API限制
        """
        try:
            # 获取所有可能的期间
            all_periods = tsAStockFinanceVIP.get_next_quarter_periods()
            
            # 检查表是否存在
            from finhack.library.db import DB
            adapter = DB.get_adapter(db)
            if not adapter.table_exists(table_name):
                # 表不存在，返回前batch_size个期间开始获取
                Log.logger.info(f"表 {table_name} 不存在，将从最早期间开始获取数据")
                return all_periods[:batch_size] if all_periods else []
            
            # 获取表中已存在的期间
            sql = f"select distinct end_date from {table_name} order by end_date"
            existing_periods_df = DB.select_to_df(sql, db)
            
            existing_periods = []
            if existing_periods_df is not None and not existing_periods_df.empty:
                existing_periods = existing_periods_df['end_date'].tolist()
            
            # 找出需要更新的期间（不在现有期间中的）
            periods_to_update = [p for p in all_periods if p not in existing_periods]

            # 【修死循环】排除"已尝试且为空"的老期间(>1年前): 空期间不写数据表, 不排除会每次重抓最老的空期间,
            # 永远卡在那里(如 express 卡在 2000-2003 空 7 年)。近 1 年的空期间保留, 允许重试(财报可能晚出)。
            cutoff = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime('%Y%m%d')
            attempted_empty = tsAStockFinanceVIP._attempted_empty_old_periods(table_name, cutoff, db)
            if attempted_empty:
                before = len(periods_to_update)
                periods_to_update = [p for p in periods_to_update if p not in attempted_empty]
                Log.logger.info(f"表 {table_name}: 跳过 {before - len(periods_to_update)} 个已尝试过的老空期间(<{cutoff})")

            if not periods_to_update:
                Log.logger.info(f"表 {table_name} 的所有期间数据都已存在，无需更新")
                return []
            
            # 返回需要更新的期间，但限制每次处理的数量
            periods_count = len(periods_to_update)
            batch_periods = periods_to_update[:batch_size]
            
            Log.logger.info(f"表 {table_name} 共有 {periods_count} 个期间需要更新，本批次处理 {len(batch_periods)} 个期间")
            Log.logger.info(f"本批次期间范围: {batch_periods[0]} - {batch_periods[-1]}")
            
            return batch_periods
                
        except Exception as e:
            Log.logger.error(f"获取需要更新的期间列表失败: {str(e)}")
            # 如果表不存在或查询失败，返回最早的期间
            all_periods = tsAStockFinanceVIP.get_next_quarter_periods()
            return [all_periods[0]] if all_periods else []
    
    @staticmethod
    def _ensure_attempt_table(db='default'):
        """已尝试期间追踪表 —— 修"空期间死循环": 返回空的期间不写数据表, 若不记下来, 每次都当它
        "不存在"重新抓, 永远卡在最老的空期间(如 express 卡在 2000-2003)。"""
        try:
            DB.exec("CREATE TABLE IF NOT EXISTS _vip_period_attempts (table_name TEXT, end_date TEXT, had_data INTEGER, attempt_ts TEXT, PRIMARY KEY(table_name, end_date))", db)
        except Exception as e:
            Log.logger.warning(f"创建 _vip_period_attempts 失败: {e}")

    @staticmethod
    def _record_period_attempt(table_name, end_date, had_data, db='default'):
        """记录一个期间已尝试过(had_data: 是否有数据)。INSERT OR REPLACE: 同期间重试会刷新状态。"""
        tsAStockFinanceVIP._ensure_attempt_table(db)
        try:
            hd = 1 if had_data else 0
            DB.exec(f"INSERT OR REPLACE INTO _vip_period_attempts(table_name,end_date,had_data,attempt_ts) VALUES('{table_name}','{end_date}',{hd},datetime('now'))", db)
        except Exception as e:
            Log.logger.warning(f"记录期间尝试失败 {table_name}/{end_date}: {e}")

    @staticmethod
    def _attempted_empty_old_periods(table_name, before_period, db='default'):
        """返回"已尝试且为空、且期间早于 before_period"的列表 —— 这些老空期间永久跳过, 避免死循环。
        近期(>= before_period)的空期间不跳过, 留给上层重试(财报数据可能晚出)。"""
        tsAStockFinanceVIP._ensure_attempt_table(db)
        try:
            rows = DB.select_to_list(f"SELECT end_date FROM _vip_period_attempts WHERE table_name='{table_name}' AND had_data=0 AND end_date < '{before_period}'", db)
            return [r['end_date'] for r in rows] if rows else []
        except Exception:
            return []

    @staticmethod
    def collect_vip_data(pro, api_name, table_name, fields, db='default', max_retries=3):
        """
        使用VIP接口收集财务数据的通用方法
        支持批量获取和自动重试机制
        
        Args:
            pro: tushare pro对象
            api_name: API名称
            table_name: 表名
            fields: 字段列表
            db: 数据库名称
            max_retries: 最大重试次数
        """
        try:
            Log.logger.info(f"{api_name} - 开始收集数据到表 {table_name}")
            
            # 获取最大end_date
            max_end_date = tsAStockFinanceVIP.get_max_end_date_from_table(table_name, db)
            
            # 如果存在最大end_date，先删除该期间的数据（确保数据完整性）
            if max_end_date:
                Log.logger.info(f"{api_name} - 发现最大end_date: {max_end_date}")
                tsAStockFinanceVIP.delete_max_end_date_records(table_name, max_end_date, db)
            
            # 循环获取数据直到所有期间都获取完毕
            retry_count = 0
            while retry_count <= max_retries:
                # 获取需要更新的期间
                periods_to_update = tsAStockFinanceVIP.get_periods_to_update(table_name, db, batch_size=20)
                
                if not periods_to_update:
                    Log.logger.info(f"{api_name} - 所有期间数据已获取完毕")
                    break
                
                Log.logger.info(f"{api_name} - 开始获取期间: {periods_to_update}")
                
                # 获取API函数
                api_func = getattr(pro, f"{api_name}_vip")
                
                success_count = 0
                failed_periods = []
                
                for i, period in enumerate(periods_to_update, 1):
                    try:
                        Log.logger.info(f"{api_name} - 正在获取期间 {period} 的数据 ({i}/{len(periods_to_update)})")
                        
                        # 调用VIP接口
                        if fields:
                            df = api_func(period=period, fields=fields)
                        else:
                            df = api_func(period=period)
                        
                        if df is not None and not df.empty:
                            # 保存数据
                            DB.safe_to_sql(df, table_name, db, index=False, if_exists='append', chunksize=5000)
                            Log.logger.info(f"{api_name} - 成功获取并保存期间 {period} 的数据，共 {len(df)} 条记录")
                            success_count += 1
                            tsAStockFinanceVIP._record_period_attempt(table_name, period, True, db)
                        else:
                            Log.logger.warning(f"{api_name} - 期间 {period} 没有返回数据")
                            success_count += 1  # 空数据也算成功，避免无限重试(批内)
                            tsAStockFinanceVIP._record_period_attempt(table_name, period, False, db)  # 记录空期间, 跨run跳过, 避免死循环
                        
                        # 避免请求过于频繁
                        time.sleep(0.5)
                        
                    except Exception as e:
                        error_str = str(e)
                        if "每天最多访问" in error_str or "每小时最多访问" in error_str:
                            Log.logger.warning(f"{api_name} - 触发每日/每小时访问限制: {error_str}")
                            Log.logger.info(f"{api_name} - 本次已成功获取 {success_count} 个期间的数据，请稍后重新运行继续获取剩余数据")
                            return
                        elif "最多访问" in error_str:
                            Log.logger.warning(f"{api_name} - 触发访问限制，等待重试: {error_str}")
                            time.sleep(30)  # 增加等待时间
                            failed_periods.append(period)
                            continue
                        else:
                            Log.logger.error(f"{api_name} - 获取期间 {period} 数据失败: {error_str}")
                            failed_periods.append(period)
                            continue
                
                # 如果有失败的期间，记录并继续下次批处理
                if failed_periods:
                    Log.logger.warning(f"{api_name} - 本批次有 {len(failed_periods)} 个期间获取失败，将在下次运行时重试")
                
                Log.logger.info(f"{api_name} - 本批次处理完成，成功获取 {success_count} 个期间的数据")
                
                # 如果本批次处理完成且没有触发访问限制，检查是否还有更多数据需要获取
                remaining_periods = tsAStockFinanceVIP.get_periods_to_update(table_name, db, batch_size=1)
                if not remaining_periods:
                    Log.logger.info(f"{api_name} - 所有历史数据获取完毕！")
                    break
                else:
                    Log.logger.info(f"{api_name} - 还有更多数据需要获取，继续下一批次...")
                    time.sleep(2)  # 批次间稍作等待
                
                retry_count += 1
            
            if retry_count > max_retries:
                Log.logger.warning(f"{api_name} - 达到最大重试次数，请稍后重新运行继续获取剩余数据")
            
        except Exception as e:
            Log.logger.error(f"{api_name} - 收集数据失败: {str(e)}")
            traceback.print_exc()
    
    # 各个具体的财务数据收集方法
    
    @tsMonitor
    def income_vip(pro, db):
        """利润表VIP接口"""
        fields = "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,end_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit,rd_exp,fin_exp_int_exp,fin_exp_int_inc,transfer_surplus_rese,transfer_housing_imprest,transfer_oth,adj_lossgain,withdra_legal_surplus,withdra_legal_pubfund,withdra_biz_devfund,withdra_rese_fund,withdra_oth_ersu,workers_welfare,distr_profit_shrhder,prfshare_payable_dvd,comshare_payable_dvd,capit_comstock_div,net_after_nr_lp_correct,credit_impa_loss,net_expo_hedging_benefits,oth_impair_loss_assets,total_opcost,amodcost_fin_assets,oth_income,asset_disp_income,continued_net_profit,end_net_profit,update_flag"
        table_name = "astock_finance_income"
        
        Log.logger.info("开始使用VIP接口获取利润表数据")
        tsAStockFinanceVIP.collect_vip_data(pro, "income", table_name, fields, db)
    
    @tsMonitor
    def balancesheet_vip(pro, db):
        """资产负债表VIP接口"""
        fields = "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,total_share,cap_rese,undistr_porfit,surplus_rese,special_rese,money_cap,trad_asset,notes_receiv,accounts_receiv,oth_receiv,prepayment,div_receiv,int_receiv,inventories,amor_exp,nca_within_1y,sett_rsrv,loanto_oth_bank_fi,premium_receiv,reinsur_receiv,reinsur_res_receiv,pur_resale_fa,oth_cur_assets,total_cur_assets,fa_avail_for_sale,htm_invest,lt_eqt_invest,invest_real_estate,time_deposits,oth_assets,lt_rec,fix_assets,cip,const_materials,fixed_assets_disp,produc_bio_assets,oil_and_gas_assets,intan_assets,r_and_d,goodwill,lt_amor_exp,defer_tax_assets,decr_in_disbur,oth_nca,total_nca,cash_reser_cb,depos_in_oth_bfi,prec_metals,deriv_assets,rr_reins_une_prem,rr_reins_outstd_cla,rr_reins_lins_liab,rr_reins_lthins_liab,refund_depos,ph_pledge_loans,refund_cap_depos,indep_acct_assets,client_depos,client_prov,transac_seat_fee,invest_as_receiv,total_assets,st_loan,st_borr,cb_borr,depos_ib_deposits,loan_oth_bank,trading_fl,notes_payable,acct_payable,adv_receipts,sold_for_repur_fa,comm_payable,payroll_payable,taxes_payable,int_payable,div_payable,oth_payable,acc_exp,deferred_inc,st_bonds_payable,payable_to_reinsurer,rsrv_insur_cont,acting_trading_sec,acting_uw_sec,non_cur_liab_due_1y,oth_cur_liab,total_cur_liab,bond_payable,lt_payable,specific_payables,estimated_liab,defer_tax_liab,defer_inc_non_cur_liab,oth_ncl,total_ncl,depos_oth_bfi,deriv_liab,depos,agency_bus_liab,oth_liab,prem_receiv_adva,depos_received,ph_invest,reser_une_prem,reser_outstd_claims,reser_lins_liab,reser_lthins_liab,indept_acc_liab,pledge_borr,indem_payable,policy_div_payable,total_liab,treasury_share,ordin_risk_reser,forex_differ,invest_loss_unconf,minority_int,total_hldr_eqy_exc_min_int,total_hldr_eqy_inc_min_int,total_liab_hldr_eqy,lt_payroll_payable,oth_comp_income,oth_eqt_tools,oth_eqt_tools_p_shr,lending_funds,acc_receivable,st_fin_payable,payables,hfs_assets,hfs_sales,cost_fin_assets,fair_value_fin_assets,cip_total,oth_pay_total,long_pay_total,debt_invest,oth_debt_invest,oth_eq_invest,oth_illiq_fin_assets,oth_eq_ppbond,receiv_financing,use_right_assets,lease_liab,contract_assets,contract_liab,accounts_receiv_bill,accounts_pay,oth_rcv_total,fix_assets_total,update_flag"
        table_name = "astock_finance_balancesheet"
        
        Log.logger.info("开始使用VIP接口获取资产负债表数据")
        tsAStockFinanceVIP.collect_vip_data(pro, "balancesheet", table_name, fields, db)
    
    @tsMonitor
    def cashflow_vip(pro, db):
        """现金流量表VIP接口"""
        fields = "ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,net_profit,finan_exp,c_fr_sale_sg,recp_tax_rends,n_depos_incr_fi,n_incr_loans_cb,n_inc_borr_oth_fi,prem_fr_orig_contr,n_incr_insured_dep,n_reinsur_prem,n_incr_disp_tfa,ifc_cash_incr,n_incr_disp_faas,n_incr_loans_oth_bank,n_cap_incr_repur,c_fr_oth_operate_a,c_inf_fr_operate_a,c_paid_goods_s,c_paid_to_for_empl,c_paid_for_taxes,n_incr_clt_loan_adv,n_incr_dep_cbob,c_pay_claims_orig_inco,pay_handling_chrg,pay_comm_insur_plcy,oth_cash_pay_oper_act,st_cash_out_act,n_cashflow_act,oth_recp_ral_inv_act,c_disp_withdrwl_invest,c_recp_return_invest,n_recp_disp_fiolta,n_recp_disp_sobu,stot_inflows_inv_act,c_pay_acq_const_fiolta,c_paid_invest,n_disp_subs_oth_biz,oth_pay_ral_inv_act,n_incr_pledge_loan,stot_out_inv_act,n_cashflow_inv_act,c_recp_borrow,proc_issue_bonds,oth_cash_recp_ral_fnc_act,stot_cash_in_fnc_act,free_cashflow,c_prepay_amt_borr,c_pay_dist_dpcp_int_exp,incl_dvd_profit_paid_sc_ms,oth_cashpay_ral_fnc_act,stot_cashout_fnc_act,n_cash_flows_fnc_act,eff_fx_flu_cash,n_incr_cash_cash_equ,c_cash_equ_beg_period,c_cash_equ_end_period,c_recp_cap_contrib,incl_cash_rec_saims,uncon_invest_loss,prov_depr_assets,depr_fa_coga_dpba,amort_intang_assets,lt_amort_deferred_exp,decr_deferred_exp,incr_acc_exp,loss_disp_fiolta,loss_scr_fa,loss_fv_chg,invest_loss,decr_def_inc_tax_assets,incr_def_inc_tax_liab,decr_inventories,decr_oper_payable,incr_oper_payable,others,im_net_cashflow_oper_act,conv_debt_into_cap,conv_copbonds_due_within_1y,fa_fnc_leases,end_bal_cash,beg_bal_cash,end_bal_cash_equ,beg_bal_cash_equ,im_n_incr_cash_equ,update_flag"
        table_name = "astock_finance_cashflow"
        
        Log.logger.info("开始使用VIP接口获取现金流量表数据")
        tsAStockFinanceVIP.collect_vip_data(pro, "cashflow", table_name, fields, db)
    
    @tsMonitor
    def forecast_vip(pro, db):
        """业绩预告VIP接口"""
        fields = "ts_code,ann_date,end_date,type,p_change_min,p_change_max,net_profit_min,net_profit_max,last_parent_net,first_ann_date,summary,change_reason"
        table_name = "astock_finance_forecast"
        
        Log.logger.info("开始使用VIP接口获取业绩预告数据")
        tsAStockFinanceVIP.collect_vip_data(pro, "forecast", table_name, fields, db)
    
    @tsMonitor
    def express_vip(pro, db):
        """业绩快报VIP接口"""
        fields = "ts_code,ann_date,end_date,revenue,operate_profit,total_profit,n_income,total_assets,total_hldr_eqy_exc_min_int,diluted_eps,diluted_roe,yoy_net_profit,bps,yoy_sales,yoy_op,yoy_tp,yoy_dedt_np,yoy_eps,yoy_roe,growth_assets,yoy_equity,growth_bps,or_last_year,op_last_year,tp_last_year,np_last_year,eps_last_year,open_net_assets,open_bps,perf_summary,is_audit,remark"
        table_name = "astock_finance_express"
        
        Log.logger.info("开始使用VIP接口获取业绩快报数据")
        tsAStockFinanceVIP.collect_vip_data(pro, "express", table_name, fields, db)
    
    @tsMonitor
    def fina_indicator_vip(pro, db):
        """财务指标VIP接口"""
        fields = "ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,capital_rese_ps,surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,gross_margin,current_ratio,quick_ratio,cash_ratio,invturn_days,arturn_days,inv_turn,ar_turn,ca_turn,fa_turn,assets_turn,op_income,valuechange_income,interst_income,daa,ebit,ebitda,fcff,fcfe,current_exint,noncurrent_exint,interestdebt,netdebt,tangible_asset,working_capital,networking_capital,invest_capital,retained_earnings,diluted2_eps,bps,ocfps,retainedps,cfps,ebit_ps,fcff_ps,fcfe_ps,netprofit_margin,grossprofit_margin,cogs_of_sales,expense_of_sales,profit_to_gr,saleexp_to_gr,adminexp_of_gr,finaexp_of_gr,impai_ttm,gc_of_gr,op_of_gr,ebit_of_gr,roe,roe_waa,roe_dt,roa,npta,roic,roe_yearly,roa2_yearly,roe_avg,opincome_of_ebt,investincome_of_ebt,n_op_profit_of_ebt,tax_to_ebt,dtprofit_to_profit,salescash_to_or,ocf_to_or,ocf_to_opincome,capitalized_to_da,debt_to_assets,assets_to_eqt,dp_assets_to_eqt,ca_to_assets,nca_to_assets,tbassets_to_totalassets,int_to_talcap,eqt_to_talcapital,currentdebt_to_debt,longdeb_to_debt,ocf_to_shortdebt,debt_to_eqt,eqt_to_debt,eqt_to_interestdebt,tangibleasset_to_debt,tangasset_to_intdebt,tangibleasset_to_netdebt,ocf_to_debt,ocf_to_interestdebt,ocf_to_netdebt,ebit_to_interest,longdebt_to_workingcapital,ebitda_to_debt,turn_days,roa_yearly,roa_dp,fixed_assets,profit_prefin_exp,non_op_profit,op_to_ebt,nop_to_ebt,ocf_to_profit,cash_to_liqdebt,cash_to_liqdebt_withinterest,op_to_liqdebt,op_to_debt,roic_yearly,total_fa_trun,profit_to_op,q_opincome,q_investincome,q_dtprofit,q_eps,q_netprofit_margin,q_gsprofit_margin,q_exp_to_sales,q_profit_to_gr,q_saleexp_to_gr,q_adminexp_to_gr,q_finaexp_to_gr,q_impair_to_gr_ttm,q_gc_to_gr,q_op_to_gr,q_roe,q_dt_roe,q_npta,q_opincome_to_ebt,q_investincome_to_ebt,q_dtprofit_to_profit,q_salescash_to_or,q_ocf_to_sales,q_ocf_to_or,basic_eps_yoy,dt_eps_yoy,cfps_yoy,op_yoy,ebt_yoy,netprofit_yoy,dt_netprofit_yoy,ocf_yoy,roe_yoy,bps_yoy,assets_yoy,eqt_yoy,tr_yoy,or_yoy,q_gr_yoy,q_gr_qoq,q_sales_yoy,q_sales_qoq,q_op_yoy,q_op_qoq,q_profit_yoy,q_profit_qoq,q_netprofit_yoy,q_netprofit_qoq,equity_yoy,rd_exp,update_flag"
        table_name = "astock_finance_indicator"
        
        Log.logger.info("开始使用VIP接口获取财务指标数据")
        tsAStockFinanceVIP.collect_vip_data(pro, "fina_indicator", table_name, fields, db)
    
    @tsMonitor
    def fina_audit_vip(pro, db):
        """财务审计意见VIP接口"""
        fields = "ts_code,ann_date,end_date,audit_result,audit_fees,audit_agency,audit_sign"
        table_name = "astock_finance_audit"
        
        Log.logger.info("开始使用VIP接口获取财务审计意见数据")
        tsAStockFinanceVIP.collect_vip_data(pro, "fina_audit", table_name, fields, db) 