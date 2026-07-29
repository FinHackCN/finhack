# -*- coding: utf-8 -*-
"""
因子分析器（重写版，market-aware）。

与旧版 /data/ssd2/finhack 的差异：
  - 取数：factorManager.loadFactors(matrix_list, code_list, market, freq) → 索引 (time, code)
  - 字段：ts_code/trade_date → time/code（全链路统一，无双轨）
  - 股票池：stock_pool 用 DataInterface + 财务因子（去 tushare SQL）
  - 行业：alphalens 按 MarketContext.industry_mode(market) 分发（cn_stock 用 AStock，其余跳过）
  - 存储：MySQL 兼容（factors_analysis 表，hash 去重）+ 不可达时降级 parquet
  - market 参数贯穿，支持 cn_stock/hk_stock/us_stock/global_cryptospot/cn_future

数学逻辑（ICIR/rank_ICIR/Sharpe/收益构造）从旧版照搬，仅字段名适配。
"""
import os
import math
import hashlib
import traceback
import numpy as np
import pandas as pd
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=RuntimeWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

from finhack.library.db import DB
from finhack.factor.default.factorManager import factorManager
from finhack.library.market_context import industry_mode
from runtime.constant import *
import finhack.library.log as Log


# ===================== MySQL 兼容（不可达自动降级） =====================

def _db_select(sql, conn='finhack'):
    try:
        return DB.select_to_df(sql, conn)
    except Exception as e:
        Log.logger.debug(f"DB select 降级({conn}): {e}")
        return pd.DataFrame()


def _db_exec(sql, conn='finhack'):
    try:
        DB.exec(sql, conn)
        return True
    except Exception as e:
        Log.logger.warning(f"DB exec 降级({conn})，结果未入库: {e}")
        return False


def _analysis_parquet_path(market):
    d = os.path.join(DATA_DIR, 'factors', 'analysis', market)
    os.makedirs(d, exist_ok=True)
    return d


class factorAnalyzer():

    # ===================== 股票池 =====================
    @staticmethod
    def stock_pool(market='cn_stock', max_num=300):
        """按 pe/pb/ps/total_mv 三分位分层抽样构造股票池。
        多市场：该市场无财务因子时降级为前 max_num 只。"""
        from finhack.library.data import get_data_interface
        sl = get_data_interface().get_stock_list(market=market, use_cache=False)
        if sl is None or sl.empty:
            return []
        codes_all = sl['code'].tolist()

        try:
            df = factorManager.loadFactors(
                matrix_list=['pe', 'pb', 'ps', 'total_mv'],
                market=market, freq='1d',
                start_date='20180101', end_date='20251231')
            if df is None or df.empty:
                return codes_all[:max_num]
            df = df.reset_index().sort_values('time').groupby('code').tail(1)
            df = df.dropna(subset=['pe', 'pb', 'ps', 'total_mv'])
            if df.empty:
                return codes_all[:max_num]
        except Exception as e:
            Log.logger.warning(f"stock_pool 取财务因子失败，降级前{max_num}只: {e}")
            return codes_all[:max_num]

        agg = ['pe', 'pb', 'ps', 'total_mv']
        for c in agg:
            df[c] = pd.to_numeric(df[c], errors='coerce')
        df = df.dropna(subset=agg)

        selected = []
        pool = df.copy()
        while len(selected) < max_num and not pool.empty:
            for col in agg:
                if pool.empty or len(selected) >= max_num:
                    break
                q = pool[col].quantile([1 / 3, 2 / 3]).values
                for lo, hi in [(None, q[0]), (q[0], q[1]), (q[1], None)]:
                    if lo is None:
                        seg = pool[pool[col] <= hi]
                    elif hi is None:
                        seg = pool[pool[col] > lo]
                    else:
                        seg = pool[(pool[col] > lo) & (pool[col] <= hi)]
                    if seg.empty:
                        continue
                    take = max(1, (max_num - len(selected)) // (3 * len(agg) * 4))
                    picks = seg.sort_values(by=col).head(take)['code'].tolist()
                    selected += picks
                    pool = pool[~pool['code'].isin(picks)]
                    if len(selected) >= max_num:
                        break

        selected = list(dict.fromkeys(selected))[:max_num]
        return selected if selected else codes_all[:max_num]

    # ===================== 主分析 =====================
    @staticmethod
    def analys(factor_name, df=pd.DataFrame(), days=[1, 2, 3, 5, 8, 13, 21],
               source='mining', start_date='20100101', end_date='20200101',
               formula="", replace=False, table='factors_analysis', ignore_error=False,
               code_list=[], market='cn_stock', freq='1d'):
        try:
            hashstr = f"{factor_name}-{days}-{source}-{start_date}:{end_date}#{formula}"
            md5 = hashlib.md5(hashstr.encode('utf-8')).hexdigest()

            has = _db_select(f"select * from {table} where hash='{md5}'", 'finhack') if table else pd.DataFrame()
            if (has is not None) and (not has.empty) and (not replace):
                return True

            if df is None or df.empty:
                df = factorManager.loadFactors(
                    matrix_list=['close', 'open', factor_name],
                    code_list=code_list, market=market, freq=freq,
                    start_date=start_date, end_date=end_date)
            if df is None or df.empty:
                Log.logger.warning(f"analys: {factor_name} 数据为空")
                return False

            lvl0 = df.index.names[0]
            t_col, c_col = ('time', 'code') if lvl0 == 'time' else ('trade_date', 'ts_code')

            df = df.reset_index()
            df[t_col] = pd.to_datetime(df[t_col])
            if start_date != '':
                df = df[df[t_col] >= pd.to_datetime(start_date)]
            if end_date != '':
                df = df[df[t_col] < pd.to_datetime(end_date)]
            if code_list:
                df = df[df[c_col].isin(code_list)]
            df = df.set_index([t_col, c_col])

            df = df.replace([np.inf, -np.inf], np.nan)
            df = df.dropna(subset=[factor_name, 'close', 'open'])

            desc = df[factor_name].describe()
            if 'mean' in desc and desc.get('mean') == desc.get('max'):
                if factor_name != 'alpha' and table:
                    _db_exec(f"update finhack.factors_list set check_type=14, status='acvivate' where factor_name='{factor_name}'", 'finhack')
                return False

            IC_list, IR_list, Sharpe_list = [], [], []
            for day in days:
                df['return'] = df.groupby(c_col, group_keys=False).apply(
                    lambda x: x['close'].shift(-day) / x['open'].shift(-1))
                df_tmp = df.dropna(subset=['return']).copy()
                if df_tmp.empty:
                    continue
                Sharpe_list.append(factorAnalyzer.Sharpe(df_tmp, factor_name=factor_name, t_col=t_col))
                IC, IR = factorAnalyzer.ICIR(df_tmp, factor_name, t_col=t_col, c_col=c_col, period=120)
                IC_list.append(IC)
                IR_list.append(IR)
                del df_tmp

            if not IC_list:
                return False
            IC = np.sum(IC_list) / len(IC_list)
            IR = np.sum(IR_list) / len(IR_list)
            max_sharpe = np.max(Sharpe_list) if Sharpe_list else 0
            score = abs(IC) * 10 + abs(IR) + abs(max_sharpe)

            msg = f"factor_name:{factor_name},IC={IC},IR={IR},Sharpe={max_sharpe},score={score}"
            Log.logger.info((formula + "\n" + msg) if formula else msg)

            if pd.isna(score):
                return False

            row = {'factor_name': factor_name, 'days': str(days), 'source': source,
                   'start_date': start_date, 'end_date': end_date, 'formula': formula,
                   'IC': str(IC), 'IR': str(IR), 'Sharpe': str(max_sharpe), 'score': str(score), 'hash': md5}
            if table:
                if (has is not None) and (not has.empty) and replace:
                    _db_exec(f"DELETE FROM `finhack`.`{table}` WHERE `hash`='{md5}'", 'finhack')
                insert_sql = (
                    f"INSERT INTO `finhack`.`{table}`(`factor_name`,`days`,`source`,`start_date`,"
                    f"`end_date`,`formula`,`IC`,`IR`,`Sharpe`,`score`,`hash`) VALUES ("
                    f"'{row['factor_name']}','{row['days']}','{row['source']}','{row['start_date']}',"
                    f"'{row['end_date']}','{row['formula']}',{row['IC']},{row['IR']},{row['Sharpe']},"
                    f"{row['score']},'{row['hash']}')")
                ok = _db_exec(insert_sql, 'finhack')
                if not ok:
                    pd.DataFrame([row]).to_parquet(
                        os.path.join(_analysis_parquet_path(market), f"{factor_name}_{md5}.parquet"))

            return factor_name, IC, IR, max_sharpe, score

        except Exception as e:
            if not ignore_error:
                Log.logger.info(f"{factor_name} error: {e}\n{traceback.format_exc()}")
            return False

    # ===================== IC/IR（Pearson） =====================
    @staticmethod
    def ICIR(df, factor_name, period=120, t_col='time', c_col='code'):
        IC_all = []
        df_reset = df.reset_index()
        df_reset[t_col] = pd.to_datetime(df_reset[t_col])
        grouped = df_reset.groupby(pd.Grouper(key=t_col, freq=f'{period}D'))
        for name, group in grouped:
            sub = group.dropna(subset=[factor_name, 'return'])
            if len(sub) < 2:
                continue
            corr = sub[factor_name].corr(sub['return'])
            if not math.isnan(corr) and not math.isinf(corr):
                IC_all.append(corr)
        if not IC_all:
            return 0, 0
        IC = np.mean(IC_all)
        IC_std = np.std(IC_all, ddof=1)
        IR = IC / IC_std if IC_std != 0 else 0
        return IC, IR

    # ===================== rank IC/IR（Spearman） =====================
    @staticmethod
    def rank_ICIR(df, factor_name, period=120, t_col='time', c_col='code'):
        rank_IC_all = []
        df_reset = df.reset_index()
        df_reset[t_col] = pd.to_datetime(df_reset[t_col])
        grouped = df_reset.groupby(pd.Grouper(key=t_col, freq=f'{period}D'))
        for name, group in grouped:
            sub = group.dropna(subset=[factor_name, 'return'])
            if len(sub) < 2:
                continue
            corr = sub[factor_name].rank(method='average').corr(sub['return'].rank(method='average'))
            if not math.isnan(corr) and not math.isinf(corr):
                rank_IC_all.append(corr)
        if not rank_IC_all:
            return 0, 0
        rank_IC = np.mean(rank_IC_all)
        rank_IC_std = np.std(rank_IC_all, ddof=1)
        rank_IR = rank_IC / rank_IC_std if rank_IC_std != 0 else 0
        return rank_IC, rank_IR

    # ===================== 分位数夏普 =====================
    @staticmethod
    def Sharpe(df, factor_name, n=10, t_col='time'):
        df['alpha_std'] = df.groupby(t_col)[factor_name].transform(
            lambda x: (x - x.mean()) / x.std() if x.std() != 0 else 0)

        def quantile_cut(group, n):
            try:
                res, bins = pd.qcut(group, q=n, retbins=True, duplicates='drop')
                labels = range(1, len(bins))
                return pd.cut(group, bins=bins, labels=labels, include_lowest=True)
            except Exception:
                return pd.Series([None] * len(group), index=group.index)

        df['alpha_quantile'] = df.groupby(t_col)['alpha_std'].transform(lambda x: quantile_cut(x, n))
        df = df.dropna(subset=['alpha_quantile', 'return'])

        grouped = df.groupby([t_col, 'alpha_quantile'])
        quantile_perf = {}
        for q in df['alpha_quantile'].unique():
            mean_df = grouped['return'].mean().fillna(1)
            returns = mean_df.loc[pd.IndexSlice[:, q]].tolist()
            if not returns:
                continue
            excess = [x - 1 for x in returns]
            sd = np.std(excess)
            quantile_perf[q] = {
                'net_value': np.cumprod(returns)[-1],
                'sharpe_ratio': (np.mean(excess) / sd) if sd != 0 else 0,
            }
        if not quantile_perf:
            return 0
        total_nv = sum(v['net_value'] for v in quantile_perf.values())
        if total_nv == 0:
            return 0
        return sum((v['net_value'] / total_nv) * v['sharpe_ratio'] for v in quantile_perf.values())

    # ===================== alphalens 全景（行业按市场分发） =====================
    @staticmethod
    def alphalens(factor_name='alpha', df=pd.DataFrame(), market='cn_stock', freq='1d',
                  code_list=[], notebook=False):
        try:
            import alphalens as al
            from alphalens.utils import get_clean_factor_and_forward_returns
        except Exception as e:
            Log.logger.warning(f"alphalens 未安装，跳过: {e}")
            return

        if df is None or df.empty:
            df = factorManager.loadFactors(
                matrix_list=['close', factor_name], code_list=code_list,
                market=market, freq=freq)
        else:
            base = factorManager.loadFactors(matrix_list=['close'], code_list=code_list,
                                             market=market, freq=freq)
            base[factor_name] = df
            df = base
        if df is None or df.empty:
            Log.logger.warning("alphalens: 数据为空")
            return

        df = df.reset_index()
        t_col, c_col = ('time', 'code') if 'time' in df.columns else ('trade_date', 'ts_code')
        df[t_col] = pd.to_datetime(df[t_col])

        groupby = None
        if industry_mode(market) == 'astock':
            try:
                from finhack.market.astock.astock import AStock
                from scipy.stats import zscore
                ind = AStock.getStockIndustry()
                df['industry'] = df[c_col].map(ind).fillna('其他')
                df[factor_name] = df.groupby([t_col, 'industry'])[factor_name].transform(
                    lambda x: zscore(x) if len(x) > 1 else x)
                groupby = df['industry']
            except Exception as e:
                Log.logger.warning(f"行业中性化跳过({market}): {e}")
                groupby = None

        df = df.dropna(subset=[factor_name]).set_index([t_col, c_col])
        prices = df['close'].unstack()
        factor = df[factor_name]

        kwargs = dict(factor=factor, prices=prices, periods=(1, 5, 10))
        if groupby is not None:
            kwargs['groupby'] = groupby
        factor_data = get_clean_factor_and_forward_returns(**kwargs)

        mean_return_by_qt, _ = al.performance.mean_return_by_quantile(factor_data)
        ic_by_day = al.performance.factor_information_coefficient(factor_data)
        autocorrelation = al.performance.factor_rank_autocorrelation(factor_data)
        mean_monthly_ic = al.performance.mean_information_coefficient(factor_data, by_time='M')

        if notebook:
            al.tears.create_full_tear_sheet(factor_data, long_short=True, group_neutral=False, by_group=False)
        else:
            print("\nmean_return_by_qt\n", mean_return_by_qt)
            print("\nic_by_day\n", ic_by_day.head())
            print("\nautocorrelation\n", autocorrelation.head())
            print("\nmean_monthly_ic\n", mean_monthly_ic.head())
