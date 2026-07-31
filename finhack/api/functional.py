#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
finhack 顶层函数式 API（无状态，每次传 market/freq）。

统一入口，内部委托现有模块（alphaEngine/factorAnalyzer/factorMining/trainer/trader），
市场/频率作为参数，1d/1m 自动适配（底层 loadFactorsAuto + analys resample）。

用法：
    import finhack
    finhack.compute_factors(['alpha191','alpha101'], market='cn_stock', freq='1d',
                            start_date='20200101', end_date='20201231')
    finhack.analyze('alpha191_001', market='cn_stock', freq='1d', start_date='20200101', end_date='20201231')
    model_id = finhack.train(['alpha191_001','alpha101_028'], market='cn_stock', freq='1d',
                             start_date='20180101', valid_date='20200101', end_date='20210101')
    finhack.backtest(strategy='ma_cross', market='cn_stock', freq='1d',
                     start_date='2023-01-01', end_date='2023-06-30')
"""
import sys

_FINHACK_PKG = '/mnt/ssd2/finhack-dev/finhack'
_PROJECT = '/mnt/ssd2/finhack-dev/demo_project'


def _ensure_path():
    if _FINHACK_PKG not in sys.path:
        sys.path.insert(0, _FINHACK_PKG)
    cache = _PROJECT + '/data/cache'
    if cache not in sys.path:
        sys.path.insert(0, cache)


def _project_path():
    try:
        from runtime.constant import BASE_DIR
        return BASE_DIR
    except Exception:
        return _PROJECT


def compute_factors(task_list, market='cn_stock', freq='1d', start_date='', end_date='',
                    code_list=None, process_num='auto', kind='alpha'):
    """计算因子并落盘。1d/1m 自动适配（底层 loadFactorsAuto）。

    Args:
        task_list: alphalist/indicatorlist 文件名列表，如 ['alpha191','alpha101'] 或 ['basics']
        kind: 'alpha' → alphaEngine（alpha 公式）；'indicator' → indicatorEngine（技术指标/基础字段）
    Returns:
        alpha: computeAlphaBatch 结果；indicator: None（批量计算+落盘）
    """
    _ensure_path()
    if kind == 'alpha':
        from finhack.factor.default.alphaEngine import alphaEngine
        alpha_list = alphaEngine.get_alpha_list(market, freq, ','.join(task_list))
        return alphaEngine.computeAlphaBatch(
            market, freq, alpha_list, start_date=start_date, end_date=end_date,
            code_list=code_list or [], process_num=process_num)
    else:
        from finhack.factor.default.indicatorEngine import indicatorEngine
        indicator_list = indicatorEngine.getIndicatorList(market, freq, ','.join(task_list))
        return indicatorEngine.computeIndicatorBatch(
            market, freq, indicator_list, start_date=start_date, end_date=end_date,
            code_list=code_list or [], process_num=process_num)


def analyze(factor_name, market='cn_stock', freq='1d', start_date='', end_date='',
            code_list=None, days=(1, 2, 3, 5, 8, 13, 21), **kwargs):
    """因子分析（IC/IR/Sharpe/score）。1d 原样 / 1m 自动 resample 到日。
    结果入 MySQL factors_analysis（兼容）+ parquet 降级。"""
    _ensure_path()
    from finhack.factor.default.factorAnalyzer import factorAnalyzer
    return factorAnalyzer.analys(
        factor_name, market=market, freq=freq, start_date=start_date, end_date=end_date,
        code_list=code_list or [], days=list(days), **kwargs)


def analyze_detail(factor_name, market='cn_stock', freq='1d', start_date='', end_date='',
                   code_list=None, n_quantiles=10, days=(1, 2, 3, 5, 8, 13, 21)):
    """深度因子分析（dashboard 用）：返回 IC 时序/IC 衰减/分位分层/多空/分布 dict。
    与 analyze 对称，但返回丰富结构供前端画图（不写库）。"""
    _ensure_path()
    from finhack.factor.default.factorAnalyzer import factorAnalyzer
    return factorAnalyzer.factor_detail(
        factor_name, market=market, freq=freq, start_date=start_date, end_date=end_date,
        code_list=code_list, n_quantiles=n_quantiles, days=days)


def mine(prompt='', model='gpt-4', method='gpt', market='cn_stock', freq='1d',
         code_list=None, start_date='', end_date='', **kwargs):
    """因子挖掘（LLM 生成公式 → 计算 → 分析）。method='gpt'/'kimi'。
    gplearn（遗传编程）需 train/label 数据，建议用 CLI: finhack factor mining --method=gplearn。"""
    _ensure_path()
    from finhack.factor.default.factorMining import factorMining
    codes = code_list or []
    if method.lower() in ('gpt', 'chatgpt', 'openai'):
        return factorMining.gpt(prompt, model, codes, market=market, freq=freq,
                                start_date=start_date, end_date=end_date)
    elif method.lower() == 'kimi':
        return factorMining.kimi(prompt, model, codes, market=market, freq=freq,
                                 start_date=start_date, end_date=end_date)
    else:
        raise NotImplementedError("gplearn 函数式入口待补（需 train/label，用 CLI finhack factor mining --method=gplearn）")


def train(factor_list, market='cn_stock', freq='1d', start_date='', valid_date='', end_date='',
          label='abs', shift=10, loss='mse', vector_list=None, **kwargs):
    """ML 训练（lightgbm）。返回 model_id（md5）；pred 入库为因子 pred_<md5>（回测 get_factors 读）。"""
    _ensure_path()
    import runtime.global_var as global_var

    class _Args:
        device = 'cpu'
    global_var.args = _Args()
    from finhack.trainer.lightgbm.lightgbm_trainer import LightgbmTrainer
    t = LightgbmTrainer()
    return t.start_train(
        market=market, freq=freq, start_date=start_date, valid_date=valid_date, end_date=end_date,
        matrix_list=factor_list, vector_list=vector_list or [], label=label, shift=shift,
        loss=loss, param=kwargs.get('param', {}), filter_name=kwargs.get('filter_name', ''),
        replace=kwargs.get('replace', False))


def backtest(strategy, market='cn_stock', freq='1d', start_date='', end_date='',
             cash=1000000, account_type=None, project_path=None, **kwargs):
    """回测（trader/backtest 引擎）。account_type 自动从 market_context 推断。"""
    _ensure_path()
    from finhack.library import market_context
    if account_type is None:
        at = market_context.get_account_type(market)
        account_type = {'CASH': 'stock', 'FUTURES': 'futures', 'CRYPTO': 'crypto'}.get(at, 'stock')
    proj = project_path or _project_path()
    from finhack.core.command.finhack import main
    old_argv = sys.argv
    sys.argv = ['finhack', 'trader', 'run', '--vendor=backtest', f'--project_path={proj}',
                f'market={market}', f'freq={freq}', f'start_time={start_date}', f'end_time={end_date}',
                f'strategy={strategy}', f'cash={cash}', f'account_type={account_type}']
    for _k, _v in kwargs.items():
        sys.argv.append(f'{_k}={_v}')
    try:
        main()
    except SystemExit:
        pass
    finally:
        sys.argv = old_argv


def run_pipeline(steps=('factor', 'analyze', 'train', 'trader'), market='cn_stock', freq='1d',
                 start_date='', end_date='', valid_date='', factor_task=None, factor_list=None,
                 strategy='ma_cross', code_list=None, **kwargs):
    """统一流程编排：依次执行 steps 指定环节（factor→analyze→train→trader）。

    Args:
        steps: 要执行的环节子集，如 ('factor','analyze') 或 ('trader',)
        factor_task: 因子计算的 task_list（alphalist 文件名，如 ['alpha191']）
        factor_list: train 的特征因子名列表（如 ['alpha191_001','alpha101_028']）；也用于 analyze
        strategy: trader 环节的策略名
        valid_date: train 的验证集起始日
    """
    if 'factor' in steps:
        print(f"[pipeline] factor: {factor_task or ['alpha191']}")
        compute_factors(factor_task or ['alpha191'], market, freq, start_date, end_date, code_list=code_list)
    if 'analyze' in steps:
        fl = factor_list or factor_task or []
        for f in fl:
            try:
                print(f"[pipeline] analyze: {f}")
                analyze(f, market, freq, start_date, end_date, code_list=code_list)
            except Exception as e:
                print(f"[pipeline] analyze {f} 跳过: {e}")
    if 'train' in steps and factor_list:
        print(f"[pipeline] train: {factor_list}")
        train(factor_list, market, freq, start_date, valid_date or start_date, end_date,
              **{k: v for k, v in kwargs.items() if k in ('label', 'shift', 'loss')})
    if 'trader' in steps:
        print(f"[pipeline] trader: {strategy}")
        backtest(strategy, market, freq, start_date, end_date, code_list=code_list)

