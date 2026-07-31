# -*- coding: utf-8 -*-
"""finhack REST API（Flask Blueprint）。
包装 finhack 函数式 API + 数据查询 + 回测持久化。返回 JSON。
挂载到 default_server 的 app（/api/*）。"""
import os
import sys
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
    """回测详情（净值/绩效/交易）"""
    from runtime.constant import DATA_DIR
    import numpy as np
    bt_dir = os.path.join(DATA_DIR, 'backtest')
    files = glob.glob(os.path.join(bt_dir, f'*{instance_id}*.pkl'))
    if not files:
        return jsonify({'error': 'not found'}), 404
    with open(files[0], 'rb') as f:
        r = pickle.load(f)
    returns = r.get('performance', {}).get('returns', [])
    bench = r.get('performance', {}).get('bench_returns', [])
    nav = (np.cumprod([1 + x for x in returns]) - 1).tolist() if returns else []
    bench_nav = (np.cumprod([1 + x for x in bench]) - 1).tolist() if bench else []
    # 交易记录简化（避免过大）
    trades = r.get('trades', [])[:500]
    return jsonify({
        'instance_id': r.get('instance_id'),
        'market': r.get('market'),
        'strategy': r.get('strategy'),
        'nav': nav,
        'bench_nav': bench_nav,
        'indicators': r.get('performance', {}).get('indicators', {}),
        'trade_num': r.get('performance', {}).get('trade_num'),
        'win_ratio': r.get('performance', {}).get('win_ratio'),
        'trades': trades,
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
