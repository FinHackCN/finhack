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


# ===================== Testing 检查项默认 cutoff（factor_detail 用，前端可覆盖） =====================
# val=PASS 门槛，warn=WARNING 门槛；op 比较方向。返回 factor_detail 时原样透传给前端齿轮面板。
DEFAULT_CUTOFFS = {
    'ic_abs':        {'op': '>=', 'val': 0.02, 'warn': 0.02},   # |IC| 因子-收益相关性强度
    'ir':            {'op': '>=', 'val': 0.50, 'warn': 0.30},   # IC 稳定性
    'sharpe':        {'op': '>=', 'val': 1.00, 'warn': 0.50},   # 分位分层夏普
    'coverage':      {'op': '>=', 'val': 0.80, 'warn': 0.50},   # 非空因子值占比
    'turnover_low':  {'op': '>=', 'val': 0.10, 'warn': 0.10},   # rank 自相关下限（信号别太翻转）
    'turnover_high': {'op': '<=', 'val': 0.70, 'warn': 0.85},   # rank 自相关上限（别太粘滞）
    'win_rate':      {'op': '>=', 'val': 0.52, 'warn': 0.50},   # IC 同向占比
    'monotonicity':  {'op': '>=', 'val': 0.70, 'warn': 0.40},   # 分位净值单调
    'ic_autocorr':   {'op': '>=', 'val': 0.00, 'warn': -0.10},  # lag-1 IC 自相关（方向稳）
}


def _eval_one(value, cutoff_meta):
    """单值 vs 单边 cutoff（>= 或 <=）→ 'pass'/'warning'/'fail'。"""
    op = cutoff_meta.get('op', '>=')
    val, warn = cutoff_meta['val'], cutoff_meta['warn']
    def _ge(x, t):
        return x >= t if op == '>=' else x <= t
    if _ge(value, val):
        return 'pass'
    if _ge(value, warn):
        return 'warning'
    return 'fail'


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
            hashstr = f"{factor_name}-{days}-{source}-{market}-{freq}-{start_date}:{end_date}#{formula}"
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
            df[t_col] = pd.to_datetime(df[t_col]).dt.normalize()
            # 时间戳归一到日期：TA-Lib 指标存盘用 00:00、OHLCV 用 09:30，拼接后同 code 同日会
            # 落在两行（指标行 close 空 / OHLCV 行因子空）→ dropna 全空；按 code×日 first() 合并。
            # 同时把 1m/分钟级降采样到日（每 code 每日首个非空）。
            df = df.groupby([c_col, t_col]).first().reset_index()
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
                   'IC': str(IC), 'IR': str(IR), 'Sharpe': str(max_sharpe), 'score': str(score), 'hash': md5,
                   'market': market, 'freq': freq}
            if table:
                if (has is not None) and (not has.empty) and replace:
                    _db_exec(f"DELETE FROM `finhack`.`{table}` WHERE `hash`='{md5}'", 'finhack')
                insert_sql = (
                    f"INSERT INTO `finhack`.`{table}`(`factor_name`,`days`,`source`,`start_date`,"
                    f"`end_date`,`formula`,`IC`,`IR`,`Sharpe`,`score`,`hash`,`market`,`freq`) VALUES ("
                    f"'{row['factor_name']}','{row['days']}','{row['source']}','{row['start_date']}',"
                    f"'{row['end_date']}','{row['formula']}',{row['IC']},{row['IR']},{row['Sharpe']},"
                    f"{row['score']},'{row['hash']}','{row['market']}','{row['freq']}')")
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
            df[t_col] = pd.to_datetime(df[t_col]).dt.normalize()
            # 时间戳归一到日期：TA-Lib 指标存盘 00:00、OHLCV 09:30，拼接后同 code 同日两行
            # （因子行 close 空 / OHLCV 行因子空）→ dropna 全空；按 code×日 first() 合并。
            df = df.groupby([c_col, t_col]).first().reset_index()
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
            if df[factor_name].nunique() <= 1:
                return {'error': f'{factor_name} 为常量因子（无方差，无法分析）'}

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
                    ls_dates = (top.get('dates') or [])[:len(ls)]
                    long_short = {'daily': ls, 'nv': nv, 'dates': ls_dates}

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

            # ---- WorldQuant 式扩展指标 ----
            turnover = factorAnalyzer._turnover(dfr, factor_name, t_col=t_col, c_col=c_col)
            ic_autocorr = factorAnalyzer._ic_autocorr(ICs)
            ic_autocorr_lags = factorAnalyzer._ic_autocorr_lags(ICs)
            win_rate = float(sum(1 for x in ICs if x > 0) / len(ICs)) if ICs else 0.0
            monotonicity = factorAnalyzer._monotonicity(qcurves)
            yearly = factorAnalyzer._yearly(ic_series, long_short)
            ls_daily = (long_short or {}).get('daily') or []
            if long_short and ls_daily:
                ls_ann = (1 + long_short['nv']) ** (252.0 / max(len(ls_daily), 1)) - 1
                fitness = max_sharpe * math.sqrt(max(abs(ls_ann), 0.0) / max(turnover, 0.01))
            else:
                ls_ann = None
                fitness = 0.0
            fitness = float(min(fitness, 100.0))

            # ---- Testing 检查（默认 cutoff，前端可覆盖重算）----
            raw_metrics = {
                'IC': IC, 'IR': IR, 'Sharpe': max_sharpe, 'coverage': coverage,
                'turnover': turnover, 'win_rate': win_rate,
                'monotonicity': monotonicity, 'ic_autocorr': ic_autocorr,
            }
            _tlo, _thi = DEFAULT_CUTOFFS['turnover_low'], DEFAULT_CUTOFFS['turnover_high']
            _t_status = ('pass' if _tlo['val'] <= turnover <= _thi['val']
                         else ('warning' if _tlo['warn'] <= turnover <= _thi['warn'] else 'fail'))
            checks = [
                {'key': 'ic_abs', 'name': 'IC 绝对值', 'value': round(abs(IC), 5),
                 'cutoff': DEFAULT_CUTOFFS['ic_abs']['val'], 'direction': '≥', 'desc': '因子-收益相关性强度',
                 'status': _eval_one(abs(IC), DEFAULT_CUTOFFS['ic_abs'])},
                {'key': 'ir', 'name': 'IR', 'value': round(IR, 4),
                 'cutoff': DEFAULT_CUTOFFS['ir']['val'], 'direction': '≥', 'desc': 'IC 稳定性',
                 'status': _eval_one(IR, DEFAULT_CUTOFFS['ir'])},
                {'key': 'sharpe', 'name': 'Sharpe', 'value': round(max_sharpe, 4),
                 'cutoff': DEFAULT_CUTOFFS['sharpe']['val'], 'direction': '≥', 'desc': '分位分层夏普',
                 'status': _eval_one(max_sharpe, DEFAULT_CUTOFFS['sharpe'])},
                {'key': 'coverage', 'name': '覆盖度', 'value': round(coverage, 4),
                 'cutoff': DEFAULT_CUTOFFS['coverage']['val'], 'direction': '≥', 'desc': '非空因子值占比',
                 'status': _eval_one(coverage, DEFAULT_CUTOFFS['coverage'])},
                {'key': 'turnover', 'name': '换手率(rank 自相关)', 'value': round(turnover, 4),
                 'cutoff': f"[{_tlo['val']}, {_thi['val']}]", 'direction': '∈',
                 'desc': '信号稳定性（过低=噪声/过高=粘滞）', 'status': _t_status},
                {'key': 'win_rate', 'name': 'IC 方向一致性', 'value': round(win_rate, 4),
                 'cutoff': DEFAULT_CUTOFFS['win_rate']['val'], 'direction': '≥', 'desc': 'IC 同向占比',
                 'status': _eval_one(win_rate, DEFAULT_CUTOFFS['win_rate'])},
                {'key': 'monotonicity', 'name': '单调性', 'value': round(monotonicity, 4),
                 'cutoff': DEFAULT_CUTOFFS['monotonicity']['val'], 'direction': '≥', 'desc': '分位净值单调',
                 'status': _eval_one(monotonicity, DEFAULT_CUTOFFS['monotonicity'])},
                {'key': 'ic_autocorr', 'name': 'IC 自相关', 'value': round(ic_autocorr, 4),
                 'cutoff': DEFAULT_CUTOFFS['ic_autocorr']['val'], 'direction': '≥', 'desc': 'IC 时序稳定性（lag-1）',
                 'status': _eval_one(ic_autocorr, DEFAULT_CUTOFFS['ic_autocorr'])},
            ]

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
                'quantile_returns': qcurves,      # [{q, nv, sharpe, daily, dates}]
                'long_short': long_short,         # {daily, nv, dates}
                'distribution': distribution,     # [{bin, count}]
                # ---- WorldQuant 式扩展 ----
                'turnover': turnover,
                'ic_autocorr': ic_autocorr,
                'ic_autocorr_lags': ic_autocorr_lags,
                'win_rate': win_rate,
                'monotonicity': monotonicity,
                'fitness': fitness,
                'yearly': yearly,
                'checks': checks,
                'raw_metrics': raw_metrics,
                'default_cutoffs': {k: dict(v) for k, v in DEFAULT_CUTOFFS.items()},
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
            g = d[d['q'] == q].groupby(t_col)['return'].mean().sort_index()
            if g.empty:
                continue
            rets = [float(x) - 1 for x in g.tolist()]            # ratio → decimal
            dates = [pd.Timestamp(x).strftime('%Y-%m-%d') for x in g.index]
            sd = float(np.std(rets, ddof=1)) if len(rets) > 1 else 0.0
            out.append({
                'q': int(q),
                'nv': float(np.prod([1 + r for r in rets]) - 1),
                'sharpe': float(np.mean(rets) / sd) if sd else 0.0,
                'daily': rets,
                'dates': dates,
            })
        return out

    # ===================== WorldQuant 式扩展指标（turnover/自相关/年度/单调/检查） =====================

    @staticmethod
    def _turnover(df, factor_name, t_col='time', c_col='code'):
        """换手率代理 = 日均 lag-1 rank 自相关（cross-section）。
        值越高=信号越稳（少换仓）；越低=日翻转（噪声/滑点吃掉）。
        常量因子/单日 → 0.0。"""
        try:
            d = df.reset_index() if not isinstance(df.index, pd.MultiIndex) else df.reset_index()
            d[t_col] = pd.to_datetime(d[t_col])
            d = d.dropna(subset=[factor_name])
            if d[t_col].nunique() < 2:
                return 0.0
            d['_rank'] = d.groupby(t_col)[factor_name].rank(method='average')
            piv = d.pivot_table(index=t_col, columns=c_col, values='_rank')
            prev = piv.shift(1)
            corr = piv.corrwith(prev, axis=1).dropna()
            return float(corr.mean()) if len(corr) else 0.0
        except Exception:
            return 0.0

    @staticmethod
    def _ic_autocorr(ic_values, lag=1):
        """IC 序列 lag-N 自相关。样本不足返 0.0。"""
        if len(ic_values) < lag + 2:
            return 0.0
        try:
            return float(pd.Series(ic_values).autocorr(lag=lag))
        except Exception:
            return 0.0

    @staticmethod
    def _ic_autocorr_lags(ic_values, max_lag=10):
        """IC 各 lag(1..max_lag) 自相关 → [{lag, ac}]，供前端 mini bar。"""
        out = []
        for k in range(1, max_lag + 1):
            out.append({'lag': k, 'ac': factorAnalyzer._ic_autocorr(ic_values, lag=k)})
        return out

    @staticmethod
    def _yearly(ic_series, long_short):
        """按日历年聚合 → [{year, IC, IR, Sharpe, ls_return, drawdown, win_rate, count}]。
        ic_series: [{date:'YYYY-MM-DD', ic}]；long_short: {daily:[decimal], dates:[...]} 或 None。"""
        if not ic_series:
            return []
        ic_df = pd.DataFrame(ic_series)
        ic_df['year'] = ic_df['date'].str[:4]
        ls_map = None
        if long_short and long_short.get('dates') and long_short.get('daily'):
            ls_df = pd.DataFrame({'date': long_short['dates'], 'r': long_short['daily']})
            ls_df['year'] = ls_df['date'].str[:4]
            ls_map = {y: g['r'].tolist() for y, g in ls_df.groupby('year')}
        out = []
        for year, g in ic_df.groupby('year'):
            ics = [float(x) for x in g['ic'].tolist() if pd.notna(x)]
            IC_mean = float(np.mean(ics)) if ics else 0.0
            IC_std = float(np.std(ics, ddof=1)) if len(ics) > 1 else 0.0
            IR = float(IC_mean / IC_std) if IC_std else 0.0
            win_rate = float(sum(1 for x in ics if x > 0) / len(ics)) if ics else 0.0
            ls_y = (ls_map or {}).get(year, [])
            ls_return = float(np.prod([1 + x for x in ls_y]) - 1) if ls_y else 0.0
            if ls_y:
                eq = np.cumprod([1 + x for x in ls_y])
                peak = np.maximum.accumulate(eq)
                drawdown = float((eq / peak - 1).min())
            else:
                drawdown = 0.0
            sd = float(np.std(ls_y, ddof=1)) if len(ls_y) > 1 else 0.0
            sharpe_y = float(np.mean(ls_y) / sd) if sd and ls_y else 0.0
            out.append({
                'year': str(year), 'IC': IC_mean, 'IR': IR, 'Sharpe': sharpe_y,
                'ls_return': ls_return, 'drawdown': drawdown,
                'win_rate': win_rate, 'count': len(ics),
            })
        return sorted(out, key=lambda x: x['year'])

    @staticmethod
    def _monotonicity(qcurves):
        """分位净值单调性 = Spearman(q, nv)。接近 1=干净单调分层；接近 0=无序。"""
        if len(qcurves) < 2:
            return 0.0
        qs = [q['q'] for q in qcurves]
        nvs = [q['nv'] for q in qcurves]
        try:
            from scipy.stats import spearmanr
            r, _ = spearmanr(qs, nvs)
            return float(r) if not pd.isna(r) else 0.0
        except Exception:
            def _rank(xs):
                order = sorted(range(len(xs)), key=lambda i: xs[i])
                rk = [0] * len(xs)
                for i, idx in enumerate(order):
                    rk[idx] = i + 1
                return rk
            rq, rv = _rank(qs), _rank(nvs)
            mq, mv = np.mean(rq), np.mean(rv)
            num = sum((rq[i] - mq) * (rv[i] - mv) for i in range(len(rq)))
            den = (math.sqrt(sum((x - mq) ** 2 for x in rq)) * math.sqrt(sum((x - mv) ** 2 for x in rv)))
            return float(num / den) if den else 0.0

    @staticmethod
    def ic_series_for(factor_name, market='cn_stock', freq='1d', start_date='20200101',
                      end_date='20210101', code_list=None, main_day=8):
        """薄封装：加载 close/open/factor → 复用 factor_detail 的清洗 → 返回 IC 时序 [{date, ic}]。
        /factor_corr 用它对每个因子单独算 IC 序列，避免重跑整个 factor_detail。"""
        try:
            df = factorManager.loadFactors(
                matrix_list=['close', 'open', factor_name], code_list=code_list,
                market=market, freq=freq, start_date=start_date, end_date=end_date)
            if df is None or df.empty:
                return []
            lvl0 = df.index.names[0]
            t_col, c_col = ('time', 'code') if lvl0 == 'time' else ('trade_date', 'ts_code')
            df = df.reset_index()
            df[t_col] = pd.to_datetime(df[t_col]).dt.normalize()
            df = df.groupby([c_col, t_col]).first().reset_index()
            if start_date:
                df = df[df[t_col] >= pd.to_datetime(start_date)]
            if end_date:
                df = df[df[t_col] < pd.to_datetime(end_date)]
            if code_list:
                df = df[df[c_col].isin(code_list)]
            df = df.set_index([t_col, c_col])
            df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=[factor_name, 'close', 'open'])
            if df.empty:
                return []
            df['return'] = df.groupby(c_col)['close'].shift(-main_day) / df.groupby(c_col)['open'].shift(-1)
            dfr = df.dropna(subset=['return'])
            if dfr.empty:
                return []
            return factorAnalyzer._ic_series(dfr, factor_name, t_col=t_col)
        except Exception as e:
            Log.logger.warning(f"ic_series_for {factor_name} error: {e}")
            return []

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
