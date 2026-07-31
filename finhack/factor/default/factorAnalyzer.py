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
            # 1m/分钟级：resample 到日（每 code 每日最后值），避免 groupby(time) 数十万点超时；
            # 语义=1m 因子(每日最后值) vs 日收益，同时降噪
            if freq in ('1m', '5m', '15m', '30m', '1h'):
                df['_date'] = df[t_col].dt.normalize()
                df = df.groupby([c_col, '_date']).last().reset_index()
                df[t_col] = df['_date']
                df = df.drop(columns=['_date'])
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
            score = abs(IC) * 10 + (abs(IR) if not pd.isna(IR) else 0) + abs(max_sharpe)

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

    # ===================== 深度因子分析（dashboard 用，WorldQuant 风） =====================
    @staticmethod
    def factor_detail(factor_name, market='cn_stock', freq='1d', start_date='20200101', end_date='20210101',
                      code_list=None, n_quantiles=10, days=(1, 2, 3, 5, 8, 13, 21)):
        """返回深度因子分析 dict（IC 时序/衰减/分位分层/多空/分布）。
        additive：不依赖 analys/Sharpe/ICIR 的返回，直接复用其数学，老函数零改动。"""
        try:
            df = factorManager.loadFactors(
                matrix_list=['close', 'open', factor_name], code_list=code_list,
                market=market, freq=freq, start_date=start_date, end_date=end_date)
            if df is None or df.empty:
                return {'error': f'{factor_name} 数据为空'}
            lvl0 = df.index.names[0]
            t_col, c_col = ('time', 'code') if lvl0 == 'time' else ('trade_date', 'ts_code')
            df = df.reset_index()
            df[t_col] = pd.to_datetime(df[t_col])
            if freq in ('1m', '5m', '15m', '30m', '1h'):           # 分钟级 resample 到日
                df['_date'] = df[t_col].dt.normalize()
                df = df.groupby([c_col, '_date']).last().reset_index()
                df[t_col] = df['_date']
                df = df.drop(columns=['_date'])
            if start_date:
                df = df[df[t_col] >= pd.to_datetime(start_date)]
            if end_date:
                df = df[df[t_col] < pd.to_datetime(end_date)]
            if code_list:
                df = df[df[c_col].isin(code_list)]
            df = df.set_index([t_col, c_col])
            df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=[factor_name, 'close', 'open'])
            if df.empty:
                return {'error': f'{factor_name} 清洗后为空'}

            days_t = tuple(days) if days else (1, 2, 3, 5, 8, 13, 21)
            main_day = days_t[len(days_t) // 2]                    # 中位 horizon 做 IC 时序/分层
            df['return'] = df.groupby(c_col)['close'].shift(-main_day) / df.groupby(c_col)['open'].shift(-1)
            dfr = df.dropna(subset=['return']).copy()
            if dfr.empty:
                return {'error': 'forward return 全 NaN'}

            # ---- IC 时序（每日横截面 Pearson IC）----
            ic_series = factorAnalyzer._ic_series(dfr, factor_name, t_col=t_col)
            ICs = [x['ic'] for x in ic_series]
            IC = float(np.mean(ICs)) if ICs else 0.0
            IC_std = float(np.std(ICs, ddof=1)) if len(ICs) > 1 else 0.0
            IR = float(IC / IC_std) if IC_std else 0.0

            # ---- 分位分层净值 ----
            qcurves = factorAnalyzer._quantile_curves(dfr, factor_name, t_col=t_col, n=n_quantiles)
            qsharpes = [q['sharpe'] for q in qcurves]
            max_sharpe = float(max(qsharpes)) if qsharpes else 0.0
            score = abs(IC) * 10 + (abs(IR) if not pd.isna(IR) else 0) + abs(max_sharpe)

            # ---- IC 衰减（各 horizon 的日均 IC）----
            ic_decay = factorAnalyzer._ic_decay(df, factor_name, t_col=t_col, c_col=c_col, days=days_t)

            # ---- 多空（top - bottom 分位）----
            long_short = None
            if len(qcurves) >= 2:
                top = max(qcurves, key=lambda q: q['q'])
                bot = min(qcurves, key=lambda q: q['q'])
                if top['daily'] and bot['daily']:
                    ls = [a - b for a, b in zip(top['daily'], bot['daily'])]
                    nv = float(np.prod([1 + x for x in ls]) - 1) if ls else 0.0
                    long_short = {'daily': ls, 'nv': nv}

            # ---- 分布直方图 ----
            vals = dfr[factor_name].dropna()
            distribution = []
            if len(vals) > 1:
                try:
                    hist, edges = np.histogram(vals, bins=30)
                    distribution = [{'bin': f'{edges[i]:.3g}~{edges[i+1]:.3g}', 'count': int(hist[i])}
                                    for i in range(len(hist))]
                except Exception:
                    pass

            desc = dfr[factor_name].describe()
            total_rows = len(df)
            coverage = float(dfr[factor_name].notna().sum() / total_rows) if total_rows else 0.0

            def _f(v):
                try:
                    return float(v) if pd.notna(v) else 0.0
                except Exception:
                    return 0.0

            return {
                'name': factor_name, 'market': market, 'freq': freq,
                'start_date': start_date, 'end_date': end_date, 'main_day': int(main_day),
                'summary': {
                    'IC': IC, 'IR': IR, 'Sharpe': max_sharpe, 'score': float(score),
                    'coverage': coverage,
                    'mean': _f(desc.get('mean', 0)), 'std': _f(desc.get('std', 0)),
                    'min': _f(desc.get('min', 0)), 'max': _f(desc.get('max', 0)),
                    'skew': _f(dfr[factor_name].skew()) if hasattr(dfr[factor_name], 'skew') else 0.0,
                    'count': int(desc.get('count', 0)),
                },
                'ic_series': ic_series,           # [{date, ic}]
                'ic_decay': ic_decay,             # [{days, ic}]
                'quantile_returns': qcurves,      # [{q, nv, sharpe, daily:[decimal]}]
                'long_short': long_short,         # {daily:[decimal], nv}
                'distribution': distribution,     # [{bin, count}]
            }
        except Exception as e:
            Log.logger.warning(f"factor_detail {factor_name} error: {e}\n{traceback.format_exc()}")
            return {'error': str(e)}

    @staticmethod
    def _ic_series(df, factor_name, t_col='time'):
        """每日横截面 Pearson IC → [{date, ic}]。"""
        out = []
        d = df.reset_index()
        d[t_col] = pd.to_datetime(d[t_col])
        for date, group in d.groupby(t_col):
            sub = group.dropna(subset=[factor_name, 'return'])
            if len(sub) < 5:
                continue
            c = sub[factor_name].corr(sub['return'])
            if pd.isna(c) or np.isinf(c):
                continue
            out.append({'date': pd.Timestamp(date).strftime('%Y-%m-%d'), 'ic': float(c)})
        return out

    @staticmethod
    def _ic_decay(df, factor_name, t_col='time', c_col='code', days=(1, 2, 3, 5, 8, 13, 21)):
        """各 forward horizon 的日均横截面 IC → [{days, ic}]。"""
        out = []
        base = df.copy()
        for day in days:
            base['ret_d'] = base.groupby(c_col)['close'].shift(-day) / base.groupby(c_col)['open'].shift(-1)
            tmp = base.dropna(subset=['ret_d']).reset_index()
            if tmp.empty:
                out.append({'days': int(day), 'ic': 0.0})
                continue
            ics = []
            for _, g in tmp.groupby(t_col):
                s = g.dropna(subset=[factor_name, 'ret_d'])
                if len(s) < 5:
                    continue
                c = s[factor_name].corr(s['ret_d'])
                if not pd.isna(c) and not np.isinf(c):
                    ics.append(c)
            out.append({'days': int(day), 'ic': float(np.mean(ics)) if ics else 0.0})
        return out

    @staticmethod
    def _quantile_curves(df, factor_name, t_col='time', n=10):
        """逐分位日收益序列 + 累积净值（复用 Sharpe 的 qcut 逻辑）→ [{q, nv, sharpe, daily}]。
        daily=日收益率(decimal)，nv=cumprod(1+daily)-1。"""
        d = df.copy()
        d['alpha_std'] = d.groupby(t_col)[factor_name].transform(
            lambda x: (x - x.mean()) / x.std() if x.std() != 0 else 0)

        def _qcut(g, n):
            try:
                _, bins = pd.qcut(g, q=n, retbins=True, duplicates='drop')
                return pd.cut(g, bins=bins, labels=range(1, len(bins)), include_lowest=True)
            except Exception:
                return pd.Series([None] * len(g), index=g.index)

        d['q'] = d.groupby(t_col)['alpha_std'].transform(lambda x: _qcut(x, n))
        d = d.dropna(subset=['q', 'return'])
        out = []
        for q in sorted(d['q'].unique()):
            daily = d[d['q'] == q].groupby(t_col)['return'].mean().sort_index().tolist()
            if not daily:
                continue
            rets = [float(x) - 1 for x in daily]                  # ratio → decimal
            sd = float(np.std(rets, ddof=1)) if len(rets) > 1 else 0.0
            out.append({
                'q': int(q),
                'nv': float(np.prod([1 + r for r in rets]) - 1),
                'sharpe': float(np.mean(rets) / sd) if sd else 0.0,
                'daily': rets,
            })
        return out

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
