# -*- coding: utf-8 -*-
"""finhack REST API（Flask Blueprint）。
包装 finhack 函数式 API + 数据查询 + 回测持久化。返回 JSON。
挂载到 default_server 的 app（/api/*）。"""
import os
import sys
import json
import time
import pickle
import glob

from flask import Blueprint, jsonify, request, send_from_directory

api_bp = Blueprint('api', __name__, url_prefix='/api')

# 确保 finhack path
_PKG = '/mnt/ssd2/finhack-dev/finhack'
_CACHE = '/mnt/ssd2/finhack-dev/demo_project/data/cache'
for _p in [_PKG, _CACHE]:
    if _p not in sys.path:
        sys.path.insert(0, _p)


# ===================== 查询接口 =====================

@api_bp.route('/markets')
def markets():
    """列出所有市场 + 频率支持"""
    from finhack.library import market_context as m
    result = []
    for mk in m.list_markets():
        cfg = m.get_market_config(mk)
        result.append({
            'market': mk,
            'freq_support': cfg.get('freq_support', []),
            'account_type': cfg.get('account_type'),
            'currency': cfg.get('currency'),
            'benchmark': cfg.get('benchmark'),
            'is_derivatives': cfg.get('is_derivatives', False),
        })
    return jsonify(result)


@api_bp.route('/factors')
def factors():
    """列出已入库因子"""
    from finhack.factor.default.factorManager import factorManager
    market = request.args.get('market', 'cn_stock')
    freq = request.args.get('freq', '1d')
    return jsonify(factorManager.list_factors(market=market, freq=freq))


@api_bp.route('/factor_data')
def factor_data():
    """取因子值（画图用）"""
    from finhack.library.data import get_data_interface
    name = request.args.get('name')
    if not name:
        return jsonify([])
    market = request.args.get('market', 'cn_stock')
    freq = request.args.get('freq', '1d')
    start = request.args.get('start')
    end = request.args.get('end')
    code = request.args.get('code')
    di = get_data_interface()
    df = di.get_factors(factor_names=[name], codes=code.split(',') if code else None,
                        market=market, freq=freq, start_date=start, end_date=end)
    if df is None or df.empty:
        return jsonify([])
    df = df.reset_index()
    df['time'] = df['time'].astype(str)
    return jsonify(df.to_dict('records')[:5000])


@api_bp.route('/analysis')
def analysis():
    """分析结果（MySQL factors_analysis）"""
    from finhack.library.db import DB
    try:
        df = DB.select_to_df(
            "SELECT factor_name, IC, IR, Sharpe, score, start_date, end_date, source "
            "FROM factors_analysis ORDER BY score DESC LIMIT 200", 'finhack')
        if df is None or df.empty:
            return jsonify([])
        return jsonify(df.fillna('').to_dict('records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/models')
def models():
    """模型列表（MySQL auto_train）"""
    from finhack.library.db import DB
    try:
        df = DB.select_to_df(
            "SELECT hash as model_id, features, label, shift, loss, score, start_date, end_date "
            "FROM auto_train ORDER BY score DESC LIMIT 100", 'finhack')
        if df is None or df.empty:
            return jsonify([])
        return jsonify(df.fillna('').to_dict('records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ===================== 回测结果 =====================

@api_bp.route('/backtests')
def backtests():
    """回测列表（扫 data/backtest/*.pkl）"""
    from runtime.constant import DATA_DIR
    import numpy as np
    bt_dir = os.path.join(DATA_DIR, 'backtest')
    files = sorted(glob.glob(os.path.join(bt_dir, '*.pkl')), reverse=True)
    result = []
    for f in files[:50]:
        try:
            with open(f, 'rb') as fh:
                r = pickle.load(fh)
            perf = r.get('performance', {}).get('indicators', {})
            result.append({
                'instance_id': r.get('instance_id', os.path.basename(f).replace('.pkl', '')),
                'market': r.get('market'),
                'strategy': r.get('strategy'),
                'freq': r.get('freq'),
                'start_date': r.get('start_date'),
                'end_date': r.get('end_date'),
                'total_return': perf.get('total_return'),
                'sharpe': perf.get('sharpe_ratio'),
                'max_drawdown': perf.get('max_drawdown'),
                'trade_num': r.get('performance', {}).get('trade_num'),
                'create_time': r.get('create_time'),
            })
        except Exception:
            pass
    return jsonify(result)


@api_bp.route('/backtest/<instance_id>')
def backtest_detail(instance_id):
    """回测详情：净值(策略+基准+超额)/回撤/daily_history/绩效/交易(分页)。
    净值优先用 daily_history.total_assets（引擎最可靠源），回退 cumprod(returns)。"""
    from runtime.constant import DATA_DIR
    import numpy as np
    bt_dir = os.path.join(DATA_DIR, 'backtest')
    files = glob.glob(os.path.join(bt_dir, f'*{instance_id}*.pkl'))
    if not files:
        return jsonify({'error': 'not found'}), 404
    with open(files[0], 'rb') as f:
        r = pickle.load(f)
    perf = r.get('performance', {}) or {}
    daily_history = r.get('daily_history', []) or []
    initial = float(r.get('cash', 1000000)) or 1000000.0

    # 净值曲线 + 日期（优先 daily_history）
    dates = [d.get('date') for d in daily_history] if daily_history else []
    if daily_history:
        equity = np.array([float(d.get('total_assets', initial)) for d in daily_history], dtype=float)
    else:
        rets = np.array(perf.get('returns', []), dtype=float)
        equity = np.cumprod(1 + rets) * initial if len(rets) else np.array([initial])
    nav = (equity / equity[0] - 1).tolist()

    # 回撤（underwater）
    peak = np.maximum.accumulate(equity)
    drawdown = (equity / peak - 1).tolist()

    # 基准净值（从 bench_returns）
    bench_rets = np.array(perf.get('bench_returns', []), dtype=float)
    bench_nav = (np.cumprod(1 + bench_rets) - 1).tolist() if len(bench_rets) else []

    # 超额（对齐长度）
    n = min(len(nav), len(bench_nav))
    excess_nav = [nav[i] - bench_nav[i] for i in range(n)] if n else []

    # 交易分页
    all_trades = r.get('trades', []) or []
    page = max(1, int(request.args.get('page', 1)))
    size = min(500, max(1, int(request.args.get('size', 50))))
    trades_page = all_trades[(page - 1) * size: page * size]

    return jsonify({
        'instance_id': r.get('instance_id'),
        'market': r.get('market'),
        'strategy': r.get('strategy'),
        'freq': r.get('freq'),
        'start_date': r.get('start_date'),
        'end_date': r.get('end_date'),
        'cash': r.get('cash'),
        'dates': dates,
        'nav': nav,
        'bench_nav': bench_nav,
        'excess_nav': excess_nav,
        'drawdown': drawdown,
        'daily_history': daily_history,
        'indicators': perf.get('indicators', {}),
        'benchmark': perf.get('benchmark', {}),
        'trade_num': perf.get('trade_num'),
        'trade_total': len(all_trades),
        'win_ratio': perf.get('win_ratio'),
        'trades': trades_page,
        'page': page, 'size': size,
    })


# ===================== 异步执行（长任务） =====================

@api_bp.route('/run/compute_factors', methods=['POST'])
def run_compute():
    from finhack.api.functional import compute_factors
    from finhack.server.default.tasks import create_task
    data = request.json or {}
    task_id = create_task(compute_factors, **data)
    return jsonify({'task_id': task_id})


@api_bp.route('/run/analyze', methods=['POST'])
def run_analyze():
    from finhack.api.functional import analyze
    from finhack.server.default.tasks import create_task
    data = request.json or {}
    task_id = create_task(analyze, **data)
    return jsonify({'task_id': task_id})


@api_bp.route('/run/train', methods=['POST'])
def run_train():
    from finhack.api.functional import train
    from finhack.server.default.tasks import create_task
    data = request.json or {}
    task_id = create_task(train, **data)
    return jsonify({'task_id': task_id})


@api_bp.route('/run/backtest', methods=['POST'])
def run_backtest():
    from finhack.api.functional import backtest
    from finhack.server.default.tasks import create_task
    data = request.json or {}
    task_id = create_task(backtest, **data)
    return jsonify({'task_id': task_id})


@api_bp.route('/task/<task_id>')
def task_status(task_id):
    from finhack.server.default.tasks import get_task
    return jsonify(get_task(task_id))


# ===================== 深度因子分析（dashboard 因子研究用） =====================

@api_bp.route('/factor_detail')
def factor_detail():
    """深度因子分析 dict（IC 时序/衰减/分位分层/多空/分布）。
    全市场 ~15s；默认抽样前 500 代码加速到 ~3-5s，可传 code_list=a,b,c 指定。"""
    from finhack.api.functional import analyze_detail
    name = request.args.get('name')
    if not name:
        return jsonify({'error': 'name required'}), 400
    market = request.args.get('market', 'cn_stock')
    freq = request.args.get('freq', '1d')
    start = request.args.get('start', '20200101')
    end = request.args.get('end', '20210101')
    cl = request.args.get('code_list')
    if cl:
        code_list = cl.split(',')
    else:
        code_list = None
        try:
            from finhack.library.data import get_data_interface
            sl = get_data_interface().get_stock_list(market=market)
            if sl is not None and not sl.empty:
                code_list = sl['code'].tolist()[:500]
        except Exception:
            pass
    return jsonify(analyze_detail(name, market=market, freq=freq,
                                  start_date=start, end_date=end, code_list=code_list))


# ===================== 因子挖掘 =====================

@api_bp.route('/mining')
def mining():
    """因子挖掘结果（MySQL factors_mining，与 factors_analysis 同 schema）"""
    from finhack.library.db import DB
    try:
        df = DB.select_to_df(
            "SELECT factor_name, IC, IR, Sharpe, score, source, start_date, end_date, formula "
            "FROM factors_mining ORDER BY score DESC LIMIT 200", 'finhack')
        if df is None or df.empty:
            return jsonify([])
        return jsonify(df.fillna('').to_dict('records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/run/mine', methods=['POST'])
def run_mine():
    from finhack.api.functional import mine
    from finhack.server.default.tasks import create_task
    data = request.json or {}
    task_id = create_task(mine, **data)
    return jsonify({'task_id': task_id})


# ===================== 策略管理（下拉 + 在线编辑器） =====================

def _strategies_dir(market):
    from runtime.constant import BASE_DIR
    return os.path.join(BASE_DIR, 'strategies', market)


@api_bp.route('/strategies')
def strategies():
    """列出某市场的策略文件（glob strategies/{market}/*.py）"""
    import glob as _g
    market = request.args.get('market', 'cn_stock')
    d = _strategies_dir(market)
    out = []
    if os.path.isdir(d):
        for f in sorted(_g.glob(os.path.join(d, '*.py'))):
            base = os.path.basename(f)
            if base == '__init__.py':
                continue
            out.append({'name': base[:-3], 'file': base})
    return jsonify({'market': market, 'strategies': out})


@api_bp.route('/strategy_file')
def strategy_file_get():
    """读策略 .py 源码（编辑器载入）"""
    import re
    market = request.args.get('market', 'cn_stock')
    name = request.args.get('name', '')
    if not re.match(r'^\w+$', name):
        return jsonify({'error': 'invalid name'}), 400
    path = os.path.realpath(os.path.join(_strategies_dir(market), name + '.py'))
    base = os.path.realpath(_strategies_dir(market))
    if not path.startswith(base + os.sep):
        return jsonify({'error': 'path escape'}), 400
    if not os.path.isfile(path):
        return jsonify({'error': 'not found'}), 404
    with open(path, 'r', encoding='utf-8') as f:
        return jsonify({'name': name, 'market': market, 'content': f.read()})


@api_bp.route('/strategy_file', methods=['POST'])
def strategy_file_save():
    """保存策略 .py（校验 name/market/路径不逃逸 strategies/）"""
    import re
    from finhack.library import market_context as m
    data = request.json or {}
    market = data.get('market', 'cn_stock')
    name = data.get('name', '')
    content = data.get('content', '')
    if market not in m.list_markets():
        return jsonify({'error': 'invalid market'}), 400
    if not re.match(r'^\w+$', name):
        return jsonify({'error': 'invalid name (\\w+ only)'}), 400
    d = _strategies_dir(market)
    os.makedirs(d, exist_ok=True)
    path = os.path.realpath(os.path.join(d, name + '.py'))
    base = os.path.realpath(d)
    if not path.startswith(base + os.sep):
        return jsonify({'error': 'path escape'}), 400
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)
    return jsonify({'ok': True, 'path': path})


# ===================== 数据覆盖统计 =====================

@api_bp.route('/data_coverage')
def data_coverage():
    """每市场数据覆盖：代码数 / 各频率因子数 / 日期范围（从 year 目录推断）"""
    import glob as _g
    from finhack.library import market_context as m
    from finhack.factor.default.factorManager import factorManager
    from finhack.library.data import get_data_interface
    from runtime.constant import KLINE_DIR
    di = get_data_interface()
    out = []
    for mk in m.list_markets():
        cfg = m.get_market_config(mk)
        freqs = cfg.get('freq_support', ['1d'])
        row = {'market': mk, 'freq_support': freqs, 'currency': cfg.get('currency'),
               'benchmark': cfg.get('benchmark'), 'is_derivatives': cfg.get('is_derivatives', False)}
        try:
            sl = di.get_stock_list(market=mk, use_cache=False)
            row['codes'] = int(len(sl)) if sl is not None else 0
        except Exception:
            row['codes'] = 0
        for frq in freqs:
            try:
                row['factors_' + frq] = len(factorManager.list_factors(market=mk, freq=frq))
            except Exception:
                row['factors_' + frq] = 0
        # 日期范围：从 codebased year 目录推断
        years = []
        for frq in freqs:
            ydir = os.path.join(KLINE_DIR, 'codebased', mk, frq)
            if os.path.isdir(ydir):
                years += [int(os.path.basename(p)) for p in _g.glob(os.path.join(ydir, '*'))
                          if os.path.isdir(p) and os.path.basename(p).isdigit()]
        row['date_range'] = [min(years), max(years)] if years else None
        out.append(row)
    return jsonify(out)


# ===================== 数据存储详情（磁盘占用/同步时间，对标 check） =====================

@api_bp.route('/data_detail')
def data_detail():
    """数据存储详情：总占用/各子目录/磁盘空闲、每市场 kline+因子大小、代码数、
    因子数、日期范围、最新同步时间。计算 ~10-20s，走缓存；首次/过期自动后台刷新。"""
    from finhack.server.default import data_stats as ds
    res = ds.get_data_stats()
    # 无缓存或过期 → 后台触发刷新
    if res['stats'] is None or res['stale']:
        ds.refresh_data_stats_async()
    return jsonify(res)


@api_bp.route('/data_detail/refresh', methods=['POST'])
def data_detail_refresh():
    """手动触发数据存储统计刷新。"""
    from finhack.server.default import data_stats as ds
    ok = ds.refresh_data_stats_async()
    return jsonify({'ok': True, 'started': ok})


# ===================== 数据健康检查（对标 finhack check 报告） =====================

@api_bp.route('/check_report')
def check_report():
    """读取 finhack check 生成的 check_report.json（健康分/异常/检查点/完整性）。
    返回 {report, age, stale}；过期(>6h)标记 stale。"""
    from runtime.constant import REPORTS_DIR
    p = os.path.join(REPORTS_DIR, 'check_report.json')
    if not os.path.exists(p):
        return jsonify({'report': None, 'age': None, 'stale': True})
    age = time.time() - os.path.getmtime(p)
    try:
        with open(p, 'r', encoding='utf-8') as f:
            rep = json.load(f)
    except Exception as e:
        return jsonify({'report': None, 'error': str(e), 'stale': True})
    return jsonify({'report': rep, 'age': age, 'stale': age > 6 * 3600})


@api_bp.route('/check_report/refresh', methods=['POST'])
def check_report_refresh():
    """异步运行 finhack check（~2min），刷新 check_report.json。返回 task_id。"""
    from finhack.server.default.tasks import create_task

    def _run_check():
        import io as _io
        from finhack.check.default.default_check import DefaultCheck
        from runtime.constant import BASE_DIR

        class _A:
            target = 'all'
        # check 的 DB 查询用相对路径 data/db/tushare.sqlite，必须以 BASE_DIR 为 CWD 才能解析
        _cwd = os.getcwd()
        os.chdir(BASE_DIR)
        c = DefaultCheck(_A())
        _old = sys.stdout
        sys.stdout = _io.StringIO()
        try:
            c.run()
        finally:
            sys.stdout = _old
            os.chdir(_cwd)

    tid = create_task(_run_check)
    return jsonify({'task_id': tid})

