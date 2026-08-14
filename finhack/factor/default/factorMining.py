# -*- coding: utf-8 -*-
"""
因子挖掘（重写版，market-aware）。

与旧版差异：
  - 取数：factorManager.loadFactors / list_factors（替代 getFactors/getFactorsList）
  - 字段：(time, code) 索引（无 ts_code/trade_date 双轨）
  - 公式计算：alphaEngine.calc / get_df（新接口，market/code_list 参数化）
  - 结果：MySQL factors_mining（兼容）+ parquet 降级
  - market 参数贯穿

gplearn 的遗传编程 + LLM(gpt/kimi) 生成逻辑保留，仅适配新数据契约。
"""
import time
import numpy as np
import pandas as pd

try:
    import gplearn as gp
    from gplearn.genetic import SymbolicTransformer
    _HAS_GPLEARN = True
except Exception:
    _HAS_GPLEARN = False

from finhack.library.db import DB
import finhack.factor.default.alphaEngine as alphaFunc
from finhack.factor.default.alphaEngine import alphaEngine
from finhack.factor.default.factorAnalyzer import factorAnalyzer
from finhack.factor.default.factorManager import factorManager
from runtime.constant import *
import finhack.library.log as Log


def _db_exec(sql, conn='finhack'):
    try:
        DB.exec(sql, conn)
        return True
    except Exception as e:
        Log.logger.warning(f"DB exec 降级({conn}): {e}")
        return False


class factorMining():

    # 挖掘因子落盘阈值: score = |IC|*10+|IR|+|Sharpe|, 达标才 calc(save=True) 入库(全量算重,
    # 不达标的只在 factors_mining 留记录供参考)
    SAVE_SCORE = 1.0
    # 引擎不支持的算子(依赖行业/市值/基准外部数据), 命中直接跳过省 API 徒劳
    BLACK_OPS = ('indneutralize', 'banchmarkindex', 'cap(', 'self.')

    # ============ LLM 挖掘（gpt/kimi）============
    @staticmethod
    def kimi(prompt, model, code_list, market='cn_stock', freq='1d',
             start_date='', end_date='', max_rounds=5):
        return factorMining.openai(prompt, model, code_list, market, freq, 'kimi',
                                   start_date=start_date, end_date=end_date, max_rounds=max_rounds)

    @staticmethod
    def gpt(prompt, model, code_list, market='cn_stock', freq='1d',
            start_date='', end_date='', max_rounds=5):
        return factorMining.openai(prompt, model, code_list, market, freq, 'gpt',
                                   start_date=start_date, end_date=end_date, max_rounds=max_rounds)

    @staticmethod
    def openai(prompt, model, code_list, market='cn_stock', freq='1d', s='gpt',
               start_date='', end_date='', max_rounds=5):
        import hashlib
        from finhack.library.ai import AI
        # 可用字段 = 已入库因子 + 基础 OHLCV 派生
        flist = (factorManager.list_factors(market=market, freq=freq)
                 + ['open', 'high', 'low', 'close', 'amount', 'volume', 'vwap', 'returns'])

        full_prompt = AI.load_prompt('autoalpha')
        if not full_prompt.strip():
            raise ValueError('prompt 模板缺失/为空: {BASE_DIR}/prompt/autoalpha —— 无法进行 LLM 挖掘')
        # 模板首行是旧项目的"因子列表：..."，注入当前市场实际因子，避免 LLM 大量引用不存在的字段被丢
        lines = full_prompt.split('\n')
        if lines and lines[0].startswith('因子列表'):
            lines[0] = f'因子列表：{",".join(flist)}'
            full_prompt = '\n'.join(lines)
        if prompt:
            full_prompt += f"\n\n补充要求：\n{prompt}"
        # kimi 分支不吃 gpt 系模型名（AI.Kimi 仅 model=="" 时才落 config 的 moonshot 模型）
        if s != 'gpt' and model.startswith('gpt'):
            model = ''

        n_saved = 0
        for rnd in range(max_rounds):
            print(f"第 {rnd + 1}/{max_rounds} 批 alpha 公式生成中...", flush=True)
            try:
                res = AI.ChatGPT(full_prompt, model) if s == 'gpt' else AI.Kimi(full_prompt, model)
            except Exception as e:
                Log.logger.error(f"LLM 调用失败(第{rnd + 1}批): {e}")
                break
            alphas = [ln.strip() for ln in res.splitlines() if '$' in ln and '(' in ln]
            print(f"本批生成 {len(alphas)} 条公式，开始分析...", flush=True)

            for alpha in alphas:
                try:
                    if any(op in alpha for op in factorMining.BLACK_OPS):
                        continue
                    col_list = alphaEngine.get_col_list(alpha)
                    if any(c[1:] not in flist for c in col_list):
                        continue
                    # 准备底数据 + 计算
                    df_check = alphaEngine.get_df(formula=alpha, code_list=code_list,
                                                  market=market, freq=freq,
                                                  start_date=start_date, end_date=end_date)
                    if df_check.empty:
                        continue
                    df_alpha = alphaEngine.calc(formula=alpha, df=df_check.copy(), market=market, freq=freq)
                    if df_alpha is None or df_alpha.empty:
                        continue
                    # 实名入库: mm_{公式hash} —— 前端点挖掘榜行能真正加载到因子数据
                    mm_name = 'mm_' + hashlib.md5(alpha.encode('utf-8')).hexdigest()[:8]
                    df_analys = df_check[['open', 'close']].copy()
                    df_analys[mm_name] = df_alpha
                    r = factorAnalyzer.analys(mm_name, df=df_analys, formula=alpha, source=s,
                                              table='factors_mining', ignore_error=True,
                                              market=market, freq=freq,
                                              start_date=start_date, end_date=end_date)
                    # analys 返回 (name, IC, IR, sharpe, score)；达标才全量落盘入库
                    if isinstance(r, tuple) and len(r) >= 5 and abs(r[4] or 0) >= factorMining.SAVE_SCORE:
                        try:
                            alphaEngine.calc(formula=alpha, name=mm_name, save=True, market=market,
                                             freq=freq, start_date=start_date, end_date=end_date)
                            n_saved += 1
                            print(f"  {mm_name} score={r[4]:.3f} ≥{factorMining.SAVE_SCORE} 已落盘入库", flush=True)
                        except Exception as e:
                            Log.logger.warning(f"{mm_name} 落盘失败: {e}")
                except Exception as e:
                    Log.logger.warning(f"挖掘公式处理失败 [{alpha[:60]}]: {e}")
        print(f"挖掘结束: {max_rounds} 批, 达标落盘 {n_saved} 个因子", flush=True)
        return {'rounds': max_rounds, 'saved': n_saved}

    # ============ 遗传编程挖掘（gplearn）============
    @staticmethod
    def gplearn(train, label, df_tmp, df_check, source='gplearn',
                market='cn_stock', freq='1d', start_date='', end_date=''):
        if not _HAS_GPLEARN:
            Log.logger.error("gplearn 未安装，无法运行因子挖掘")
            return

        init_function = ['add', 'sub', 'mul', 'div', 'sqrt', 'abs', 'sin', 'cos', 'tan']

        def trans_xy(xy, key='x'):
            """gplearn 的 numpy 数组 → 带 (time,code) 索引的 Series，供 alphaFunc 使用"""
            status = True
            if len(xy) < 100:
                return False, xy
            if 'numpy' not in str(type(xy)):
                return False, np.zeros(len(xy))
            if 'numpy.memmap' in str(type(xy)):
                xy = np.array(xy)
            if xy.max() == xy.min():
                return False, np.zeros(len(xy))
            if isinstance(xy, np.ndarray):
                s = df_tmp.copy()
                s[key] = xy
                xy = s[key]   # 继承 df_tmp 的 (time, code) 索引
            return status, xy

        def wrap1(name):
            def f(x):
                st, x = trans_xy(x, 'x')
                if not st:
                    return x
                return np.nan_to_num(getattr(alphaFunc, name)(x).values)
            return f

        def wrap2(name):
            def f(x, y):
                st, x = trans_xy(x, 'x')
                if not st:
                    return x
                st, y = trans_xy(y, 'y')
                if not st:
                    return y
                return np.nan_to_num(getattr(alphaFunc, name)(x, y).values)
            return f

        arity1 = ['rank', 'log', 'ts_sum', 'delta', 'product', 'ts_min', 'ts_max',
                  'ts_rank', 'stddev', 'ts_argmax', 'ts_argmin', 'lowday', 'highday', 'sumac', 'sign']
        arity2_pairs = ['correlation', 'covariance']   # 这两个 alphaFunc 是 (x,y,window)
        # min/max 在 alphaFunc 是 (x,y)；delay_n 单独
        function_set = []
        for nm in arity1:
            try:
                function_set.append(gp.functions.make_function(function=wrap1(nm), name=nm, arity=1))
            except Exception:
                pass
        for nm in arity2_pairs:
            try:
                function_set.append(gp.functions.make_function(function=wrap2(nm), name=nm, arity=2))
            except Exception:
                pass
        for nm in ['min', 'max']:
            try:
                function_set.append(gp.functions.make_function(
                    function=wrap2(nm) if False else (lambda x, y, _n=nm: np.nan_to_num(getattr(alphaFunc, _n)(x, y))),
                    name=nm, arity=2))
            except Exception:
                pass
        for d in [1, 3, 5, 7]:
            def _delay(x, _d=d):
                st, x = trans_xy(x, 'x')
                if not st:
                    return x
                return np.nan_to_num(alphaFunc.delay(x, _d).values)
            try:
                function_set.append(gp.functions.make_function(function=_delay, name=f'delay_{d}', arity=1))
            except Exception:
                pass

        gp1 = SymbolicTransformer(
            generations=10, population_size=500, hall_of_fame=100, n_components=50,
            function_set=function_set + init_function, parsimony_coefficient=0.002,
            tournament_size=20, init_depth=(2, 5), max_samples=0.9, verbose=1,
            p_crossover=0.9, p_subtree_mutation=0.01, p_hoist_mutation=0.01,
            p_point_mutation=0.01, p_point_replace=0.05,
            feature_names=list('$' + n for n in train.columns),
            random_state=int(time.time()), n_jobs=1)

        gp1.fit(train, label)
        _ = gp1.transform(train)

        alphas = list(set(str(f) for f in gp1))
        print(alphas)

        for alpha in alphas:
            try:
                if any(op in alpha for op in factorMining.BLACK_OPS):
                    continue
                df_alpha = alphaEngine.calc(formula=alpha, df=df_check.copy(), market=market, freq=freq)
                if df_alpha is None or df_alpha.empty:
                    continue
                # 实名入库(同 LLM 挖掘): mm_{公式hash}, 前端点行可加载真实因子
                import hashlib
                mm_name = 'mm_' + hashlib.md5(alpha.encode('utf-8')).hexdigest()[:8]
                df_analys = df_check[['open', 'close']].copy()
                df_analys[mm_name] = df_alpha
                r = factorAnalyzer.analys(mm_name, df=df_analys, formula=alpha, source=source,
                                          table='factors_mining', ignore_error=True,
                                          market=market, freq=freq,
                                          start_date=start_date, end_date=end_date)
                if isinstance(r, tuple) and len(r) >= 5 and abs(r[4] or 0) >= factorMining.SAVE_SCORE:
                    try:
                        alphaEngine.calc(formula=alpha, name=mm_name, save=True, market=market,
                                         freq=freq, start_date=start_date, end_date=end_date)
                        print(f"  {mm_name} score={r[4]:.3f} 已落盘入库", flush=True)
                    except Exception as e:
                        Log.logger.warning(f"{mm_name} 落盘失败: {e}")
            except Exception as e:
                Log.logger.warning(f"gplearn 公式处理失败 [{alpha[:60]}]: {e}")
