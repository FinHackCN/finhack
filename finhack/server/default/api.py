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

def _native(obj):
    """递归转 numpy 类型为 python 原生（JSON 可序列化）。
    回测 pkl 的 indicators 含 numpy float32/int，jsonify 默认不支持。"""
    import numpy as np
    if isinstance(obj, dict):
        return {k: _native(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_native(v) for v in obj]
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.ndarray):
        return [_native(x) for x in obj.tolist()]
    return obj

def nj(obj):
    """jsonify 但先把 numpy 类型转原生。"""
    return jsonify(_native(obj))

api_bp = Blueprint('api', __name__, url_prefix='/api')

# 确保 finhack path
_PKG = '/mnt/ssd2/finhack-dev/finhack'
_PROJ = '/mnt/ssd2/finhack-dev/demo_project'
_CACHE = '/mnt/ssd2/finhack-dev/demo_project/data/cache'
for _p in [_PKG, _PROJ, _CACHE]:
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
    return nj(result)


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
    """分析结果（MySQL factors_analysis，按 market/freq 过滤，不传则全部）"""
    from finhack.library.db import DB
    import re
    try:
        market = request.args.get('market')
        freq = request.args.get('freq')
        where = []
        if market and re.match(r'^\w+$', market):
            where.append(f"`market`='{market}'")
        if freq and re.match(r'^\w+$', freq):
            where.append(f"`freq`='{freq}'")
        where_clause = ('WHERE ' + ' AND '.join(where)) if where else ''
        df = DB.select_to_df(
            f"SELECT factor_name, IC, IR, Sharpe, score, start_date, end_date, source, market, freq "
            f"FROM factors_analysis {where_clause} ORDER BY score DESC LIMIT 200", 'finhack')
        if df is None or df.empty:
            return jsonify([])
        return nj(df.fillna('').to_dict('records'))
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
                'trade_num': len(r.get('trades', [])),
                'create_time': r.get('create_time'),
            })
        except Exception:
            pass
    return nj(result)


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

    return nj({
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
        'trade_num': len(all_trades),
        'trade_total': len(all_trades),
        'win_ratio': perf.get('win_ratio'),
        'trades': trades_page,
        'page': page, 'size': size,
        'rich_indicators': _rich(perf, daily_history, all_trades),
    })


def _rich(perf, daily_history, trades):
    """服务端现算富绩效指标（empyrical+numpy），失败返回 {}。"""
    try:
        from finhack.server.default.metrics import compute_rich_indicators
        ret = perf.get('returns', [])
        bench = perf.get('bench_returns', [])
        if not len(ret):
            return {}
        return compute_rich_indicators(
            ret, bench if len(bench) else None,
            daily_history=daily_history, trades=trades)
    except Exception as e:
        return {'error': str(e)}


@api_bp.route('/backtest/<instance_id>/quantstats')
def backtest_quantstats(instance_id):
    """生成 quantstats HTML 报告（月度收益/回撤/分布等），缓存到 pickle 同目录 .qs.html。"""
    from runtime.constant import DATA_DIR
    bt_dir = os.path.join(DATA_DIR, 'backtest')
    files = glob.glob(os.path.join(bt_dir, f'*{instance_id}*.pkl'))
    if not files:
        return jsonify({'error': 'not found'}), 404
    pkl = files[0]
    cache = pkl[:-4] + '.qs.html'
    if os.path.exists(cache):
        try:
            import flask
            return flask.Response(open(cache, encoding='utf-8').read(), mimetype='text/html')
        except Exception:
            pass
    import pandas as pd
    with open(pkl, 'rb') as f:
        r = pickle.load(f)
    ret = r.get('performance', {}).get('returns', [])
    if not ret:
        return jsonify({'error': '无收益序列'}), 400
    try:
        import quantstats as qs
        idx = pd.date_range(end=pd.Timestamp.today().normalize(), periods=len(ret), freq='D')
        qs.reports.html(pd.Series(ret, index=idx), benchmark=None, title='finhack 回测报告', output=cache)
        import flask
        return flask.Response(open(cache, encoding='utf-8').read(), mimetype='text/html')
    except Exception as e:
        # quantstats 与 numpy2/pandas2 可能不兼容 → 降级到自渲染指标 HTML
        html = _fallback_report(r, str(e))
        try:
            with open(cache, 'w', encoding='utf-8') as f:
                f.write(html)
        except Exception:
            pass
        import flask
        return flask.Response(html, mimetype='text/html')


def _fallback_report(r, err):
    """quantstats 不可用时的降级 HTML（指标表 + 日收益曲线）。"""
    perf = r.get('performance', {})
    ret = perf.get('returns', [])
    from finhack.server.default.metrics import compute_rich_indicators
    rich = compute_rich_indicators(ret, perf.get('bench_returns'), r.get('daily_history'), trades=r.get('trades'))
    import numpy as np
    eq = list(np.cumprod([1 + x for x in ret])) if ret else []
    rows = ''
    for grp, d in rich.items():
        for k, v in d.items():
            val = f'{v*100:.2f}%' if isinstance(v, float) and abs(v) < 50 else str(v)
            rows += f'<tr><td>{grp}</td><td>{k}</td><td style="text-align:right">{val}</td></tr>'
    pts = ','.join(f'{i},{1/(eq[0] if eq else 1)*v:.4f}' for i, v in enumerate(eq))
    return f'''<html><head><meta charset="utf-8"><style>body{{font-family:system-ui;background:#fff;color:#222;padding:20px}}
.note{{color:#888;font-size:12px;margin-bottom:8px}}table{{border-collapse:collapse;width:100%;font-size:13px}}
td,th{{border:1px solid #eee;padding:6px 10px;text-align:left}}th{{background:#f5f5f5}}</style></head>
<body><div class="note">quantstats 报告生成失败（{err[:80]}），以下为降级指标视图。</div>
<svg width="100%" height="220" viewBox="0 0 {max(len(eq),1)} 1" preserveAspectRatio="none" style="background:#fafafa">
<polyline fill="none" stroke="#2563eb" stroke-width="1" points="{pts}"/></svg>
<table><thead><tr><th>分组</th><th>指标</th><th>值</th></tr></thead><tbody>{rows}</tbody></table></body></html>'''


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


# ===================== 因子库管理（dashboard 因子管理用） =====================

def _find_alphalist_file(market, freq, alphalist_name):
    """按序定位 alphalist 文件：{market}/x{freq}/ → {market}/{freq}/ → 顶层。返回路径或 None。
    对应 alphaEngine.get_alpha_list 的扫描顺序（实测文件都在 x{freq}/ 下）。"""
    from runtime.constant import CONFIG_DIR
    base = os.path.join(CONFIG_DIR, 'factorlist', 'alphalist')
    for sub in (os.path.join(market, 'x' + freq), os.path.join(market, freq), ''):
        p = os.path.join(base, sub, alphalist_name) if sub else os.path.join(base, alphalist_name)
        if os.path.isfile(p):
            return p
    return None


# alphaEngine.calc 跳过的引擎不支持字段（alphaEngine.py:1151）
_BLACK = ['indneutralize', 'cap', 'filter', 'self', 'banchmarkindex']


def _trial_calc(formula, name, market, freq):
    """试算校验：构造小样本人造 df 测 eval（秒级，绕开 data_interface 加载与大数据拉取，
    避免对大数据市场 cn_stock/全历史 cn_future 的 loadFactors 卡住 HTTP）。calc 静默吞异常，
    靠返回空 Series 判失败。人造 df 覆盖 alpha 常用字段（close/open/high/low/volume/amount/returns/vwap）。"""
    import pandas as pd, numpy as np
    from finhack.factor.default.alphaEngine import alphaEngine
    dates = pd.date_range('2020-01-01', periods=120, freq='D')
    codes = ['C001', 'C002', 'C003']
    idx = pd.MultiIndex.from_product([dates, codes], names=['time', 'code'])
    rng = np.random.RandomState(42)
    base = rng.rand(len(idx)) * 100 + 50
    df = pd.DataFrame({
        'close': base, 'open': base + rng.randn(len(idx)),
        'high': base + np.abs(rng.randn(len(idx))) + 1,
        'low': base - np.abs(rng.randn(len(idx))) - 1,
        'volume': rng.rand(len(idx)) * 1e6 + 1e5,
        'amount': rng.rand(len(idx)) * 1e8 + 1e7,
        'returns': rng.randn(len(idx)) * 0.02,
        'vwap': base + rng.randn(len(idx)) * 0.5,
    }, index=idx)
    try:
        res = alphaEngine.calc(formula=formula, name=name, save=False, market=market, freq=freq, df=df)
    except Exception as e:
        return False, f'试算异常: {e}'
    if res is None or (hasattr(res, 'empty') and res.empty):
        return False, '公式试算失败：语法错误或字段缺失（calc 静默吞异常，具体原因见服务端 debug 日志）'
    return True, ''


@api_bp.route('/factor_inspect')
def factor_inspect():
    """列出已入库因子 + 轻量元信息（year 范围/代码数/大小），matrix+vector 合并。纯 inode 不加载 pkl。"""
    from finhack.factor.default.factorManager import factorManager
    market = request.args.get('market', 'cn_stock')
    freq = request.args.get('freq', '1d')
    out = []
    for ftype in ('matrix', 'vector'):
        try:
            out.extend(factorManager.inspectFactorsLight(market=market, freq=freq, factor_type=ftype))
        except Exception:
            pass
    return jsonify(out)


@api_bp.route('/factor/delete', methods=['POST'])
def factor_delete():
    """删除因子（支持批量）：删 pkl（matrix+vector，含分片）+ 清 factors_analysis 记录。
    body: {names:[...], market, freq}"""
    import re
    from finhack.factor.default.factorManager import factorManager
    from finhack.library.db import DB
    data = request.get_json(silent=True) or {}
    market = data.get('market', 'cn_stock')
    freq = data.get('freq', '1d')
    names = data.get('names', [])
    if not isinstance(names, list) or not names:
        return jsonify({'error': '缺少 names'}), 400
    names = [n for n in names if isinstance(n, str) and re.match(r'^\w+$', n)]
    if not names:
        return jsonify({'error': '无有效因子名'}), 400
    removed = sum(factorManager.deleteFactorFiles(n, market=market, freq=freq) for n in names)
    try:
        in_list = ",".join(f"'{n}'" for n in names)
        DB.delete(f"DELETE FROM factors_analysis WHERE factor_name IN ({in_list})", 'finhack')
    except Exception:
        pass
    return jsonify({'ok': True, 'removed': removed, 'db_cleaned': len(names)})


@api_bp.route('/factor_formula')
def factor_formula_get():
    """读因子公式/定义。alpha 类可编辑（返回 file/line/editable=True）；
    indicator 类只读源码（editable=False）；基础字段无公式。"""
    import re
    from runtime.constant import INDICATORS_DIR
    name = request.args.get('name', '')
    market = request.args.get('market', 'cn_stock')
    freq = request.args.get('freq', '1d')
    if not re.match(r'^\w+$', name):
        return jsonify({'error': 'invalid name'}), 400
    parts = name.rsplit('_', 1)
    # alpha 类：name = {alphalist}_{NNN}，公式为 alphalist 文件第 N 行
    if len(parts) == 2 and parts[1].isdigit():
        alphalist_name, idx = parts[0], int(parts[1])
        path = _find_alphalist_file(market, freq, alphalist_name)
        if path:
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                if 1 <= idx <= len(lines):
                    return jsonify({'name': name, 'formula': lines[idx-1].strip(),
                                    'file': path, 'line': idx, 'editable': True,
                                    'kind': 'alpha', 'alphalist': alphalist_name})
            except OSError:
                pass
        # indicator 类：indicator 按文件组织（一个 .py 含多个指标，如 basics.py），
        # 用 getIndicatorInfo 定位 module，只读展示源码（在线编辑整文件风险高，建议在 IDE 改）
        try:
            from finhack.factor.default.indicatorEngine import indicatorEngine
            info = indicatorEngine.getIndicatorInfo(name, market, freq)
            if info and info[0]:
                module_name = info[0]
                for sub in (os.path.join(INDICATORS_DIR, market, 'x' + freq),
                            os.path.join(INDICATORS_DIR, market, freq), INDICATORS_DIR):
                    src = os.path.join(sub, module_name + '.py')
                    if os.path.isfile(src):
                        with open(src, 'r', encoding='utf-8') as f:
                            return jsonify({'name': name, 'formula': f.read(), 'file': src,
                                            'editable': False, 'kind': 'indicator',
                                            'module': module_name,
                                            'note': f'indicator 按文件组织，源码在 {module_name}.py（含多个指标）；在线编辑整文件风险高，建议 IDE 改'})
        except Exception:
            pass
    return jsonify({'name': name, 'formula': '', 'editable': False,
                    'kind': 'basic', 'note': '基础字段或未知定义，无可编辑公式'})


@api_bp.route('/factor_formula/save', methods=['POST'])
def factor_formula_save():
    """编辑保存 alpha 因子公式（仅 alpha 类）：黑名单检查→试算校验→快照→原子写回单行。
    只改行内容不增删行（保住 行号↔因子名 映射稳定）。"""
    import re, time as _t
    data = request.get_json(silent=True) or {}
    market = data.get('market', 'cn_stock')
    freq = data.get('freq', '1d')
    name = data.get('name', '')
    new_formula = (data.get('formula') or '').strip()
    if not re.match(r'^\w+$', name):
        return jsonify({'error': 'invalid name'}), 400
    parts = name.rsplit('_', 1)
    if len(parts) != 2 or not parts[1].isdigit():
        return jsonify({'error': '该因子非 alpha 类，不可编辑'}), 400
    alphalist_name, idx = parts[0], int(parts[1])
    path = _find_alphalist_file(market, freq, alphalist_name)
    if not path:
        return jsonify({'error': f'未找到 alphalist 文件: {alphalist_name}'}), 404
    if not new_formula:
        return jsonify({'error': '公式不能为空'}), 400
    # ① 黑名单（引擎不支持的公式字段）
    hit = [w for w in _BLACK if w in new_formula]
    if hit:
        return jsonify({'error': f'公式含引擎不支持的字段: {",".join(hit)}（依赖行业/市值/基准等外部数据）'}), 400
    # ② 试算校验
    ok, err = _trial_calc(new_formula, name, market, freq)
    if not ok:
        return jsonify({'error': err}), 400
    # ③ 读现文件 + 行号校验
    with open(path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    if idx < 1 or idx > len(lines):
        return jsonify({'error': f'行号越界: {idx}（文件共 {len(lines)} 行）'}), 400
    # ④ 快照到 .versions/（可回滚）
    vd = os.path.join(os.path.dirname(path), '.versions')
    os.makedirs(vd, exist_ok=True)
    ts = _t.strftime('%Y%m%d_%H%M%S')
    try:
        with open(os.path.join(vd, f'{alphalist_name}.{ts}.bak'), 'w', encoding='utf-8') as f:
            f.write(''.join(lines))
    except OSError:
        ts = None
    # ⑤ 原子写回：临时文件 + os.replace
    new_line = new_formula if new_formula.endswith('\n') else new_formula + '\n'
    lines[idx-1] = new_line
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        f.write(''.join(lines))
    os.replace(tmp, path)
    return jsonify({'ok': True, 'snapshot': ts, 'file': path, 'line': idx})


@api_bp.route('/factor_coverage')
def factor_coverage():
    """单因子覆盖度（年份×代码稀疏矩阵，热力图用）。"""
    import re
    from finhack.factor.default.factorManager import factorManager
    name = request.args.get('name', '')
    if not re.match(r'^\w+$', name):
        return jsonify({'error': 'invalid name'}), 400
    market = request.args.get('market', 'cn_stock')
    freq = request.args.get('freq', '1d')
    return jsonify(factorManager.coverageFactor(name, market=market, freq=freq, factor_type='matrix'))


@api_bp.route('/run/recompute_factor', methods=['POST'])
def run_recompute_factor():
    """单因子重算落盘：反查最新公式→calc(save=True)→清旧 factors_analysis 记录。
    calc 不开进程池，daemon 线程安全；进度恒 0%（tasks 正则是回测专用）。"""
    import re
    from finhack.server.default.tasks import create_task
    data = request.get_json(silent=True) or {}
    market = data.get('market', 'cn_stock')
    freq = data.get('freq', '1d')
    name = data.get('name', '')
    start_date = data.get('start_date', '')
    end_date = data.get('end_date', '')
    if not re.match(r'^\w+$', name):
        return jsonify({'error': 'invalid name'}), 400
    if not start_date or not end_date:
        return jsonify({'error': '需指定 start_date / end_date'}), 400

    def _run():
        from finhack.factor.default.alphaEngine import alphaEngine
        from finhack.library.db import DB
        # 反查最新公式（save 后文件已是新值）
        parts = name.rsplit('_', 1)
        formula = ''
        if len(parts) == 2 and parts[1].isdigit():
            p = _find_alphalist_file(market, freq, parts[0])
            if p:
                with open(p, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                i = int(parts[1])
                if 1 <= i <= len(lines):
                    formula = lines[i-1].strip()
        if not formula:
            raise ValueError('未找到因子公式，无法重算（非 alpha 类？）')
        res = alphaEngine.calc(formula=formula, name=name, save=True, market=market, freq=freq,
                               start_date=start_date, end_date=end_date)
        if res is None or (hasattr(res, 'empty') and res.empty):
            raise ValueError('重算失败：公式计算返回空（语法错误或字段缺失）')
        # 公式变→hash 变→旧 factors_analysis 残留，按 factor_name 清掉让重新分析
        try:
            DB.delete(f"DELETE FROM factors_analysis WHERE factor_name = '{name}'", 'finhack')
        except Exception:
            pass
        return {'name': name, 'shape': list(res.shape) if hasattr(res, 'shape') else None}

    tid = create_task(_run)
    return jsonify({'task_id': tid})


@api_bp.route('/factor/create', methods=['POST'])
def factor_create():
    """新建 alpha 因子：黑名单检查→试算校验→追加到 alphalist 末尾→异步重算入库。
    追加到末尾安全（不影响已有 行号↔因子名 映射）。新因子名 = {alphalist}_{新行号:03d}。
    body: {alphalist, formula, market, freq, start_date, end_date}"""
    import re, time as _t
    from runtime.constant import CONFIG_DIR
    data = request.get_json(silent=True) or {}
    market = data.get('market', 'cn_stock')
    freq = data.get('freq', '1d')
    alphalist = data.get('alphalist', '')
    formula = (data.get('formula') or '').strip()
    start_date = data.get('start_date', '')
    end_date = data.get('end_date', '')
    if not re.match(r'^\w+$', alphalist):
        return jsonify({'error': 'alphalist 名仅允许字母数字下划线'}), 400
    if not formula:
        return jsonify({'error': '公式不能为空'}), 400
    if not start_date or not end_date:
        return jsonify({'error': '需指定 start_date / end_date'}), 400
    # 黑名单
    hit = [w for w in _BLACK if w in formula]
    if hit:
        return jsonify({'error': f'公式含引擎不支持的字段: {",".join(hit)}'}), 400
    # 试算校验（用临时 name，不落盘）
    ok, err = _trial_calc(formula, 'tmp_new_factor', market, freq)
    if not ok:
        return jsonify({'error': err}), 400
    # 定位 alphalist 文件（不存在则新建在 {market}/x{freq}/ 下）
    path = _find_alphalist_file(market, freq, alphalist)
    if not path:
        base = os.path.join(CONFIG_DIR, 'factorlist', 'alphalist', market, 'x' + freq)
        os.makedirs(base, exist_ok=True)
        path = os.path.join(base, alphalist)
    # 读现文件 → 新行号
    lines = []
    if os.path.isfile(path):
        with open(path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    new_idx = len(lines) + 1
    name = f"{alphalist}_{str(new_idx).zfill(3)}"
    # 快照（仅当文件已存在）
    ts = None
    if lines:
        vd = os.path.join(os.path.dirname(path), '.versions')
        os.makedirs(vd, exist_ok=True)
        ts = _t.strftime('%Y%m%d_%H%M%S')
        try:
            with open(os.path.join(vd, f'{alphalist}.{ts}.bak'), 'w', encoding='utf-8') as f:
                f.write(''.join(lines))
        except OSError:
            ts = None
    # 原子追加
    new_line = formula if formula.endswith('\n') else formula + '\n'
    lines.append(new_line)
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        f.write(''.join(lines))
    os.replace(tmp, path)
    # 异步重算入库
    from finhack.server.default.tasks import create_task

    def _run():
        from finhack.factor.default.alphaEngine import alphaEngine
        res = alphaEngine.calc(formula=formula, name=name, save=True, market=market, freq=freq,
                               start_date=start_date, end_date=end_date)
        if res is None or (hasattr(res, 'empty') and res.empty):
            raise ValueError('重算失败：公式计算返回空')
        return {'name': name}

    tid = create_task(_run)
    return jsonify({'ok': True, 'name': name, 'line': new_idx, 'task_id': tid})


@api_bp.route('/factor_indicator/save', methods=['POST'])
def factor_indicator_save():
    """编辑 indicator 源码 .py：compile 语法校验→快照→原子写回→清 lru_cache。
    computeIndicator 用 importlib 每次重载模块（无模块缓存），清掉 getIndicatorInfo/
    getIndicatorList 的 lru_cache 后，下次计算即用新代码。body: {name, code, market, freq}"""
    import re, time as _t
    from runtime.constant import INDICATORS_DIR
    data = request.get_json(silent=True) or {}
    market = data.get('market', 'cn_stock')
    freq = data.get('freq', '1d')
    name = data.get('name', '')
    code = data.get('code', '')
    if not re.match(r'^\w+$', name):
        return jsonify({'error': 'invalid name'}), 400
    parts = name.rsplit('_', 1)
    if len(parts) != 2:
        return jsonify({'error': '无效 indicator 名'}), 400
    module_name = parts[0]
    # 定位 .py
    src = None
    for sub in (os.path.join(INDICATORS_DIR, market, 'x' + freq),
                os.path.join(INDICATORS_DIR, market, freq), INDICATORS_DIR):
        p = os.path.join(sub, module_name + '.py')
        if os.path.isfile(p):
            src = p
            break
    if not src:
        return jsonify({'error': f'未找到 indicator 源码: {module_name}'}), 404
    if not code.strip():
        return jsonify({'error': '源码不能为空'}), 400
    # 语法校验
    try:
        compile(code, src, 'exec')
    except SyntaxError as e:
        return jsonify({'error': f'源码语法错误: {e.msg} (line {e.lineno})'}), 400
    # 快照
    vd = os.path.join(os.path.dirname(src), '.versions')
    os.makedirs(vd, exist_ok=True)
    ts = _t.strftime('%Y%m%d_%H%M%S')
    try:
        with open(src, 'r', encoding='utf-8') as f:
            old = f.read()
        with open(os.path.join(vd, f'{module_name}.{ts}.py.bak'), 'w', encoding='utf-8') as f:
            f.write(old)
    except OSError:
        ts = None
    # 原子写回
    tmp = src + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        f.write(code)
    os.replace(tmp, src)
    # 清 lru_cache（getIndicatorInfo/getIndicatorList 缓存了旧解析）
    try:
        from finhack.factor.default.indicatorEngine import indicatorEngine
        indicatorEngine.getIndicatorInfo.cache_clear()
        indicatorEngine.getIndicatorList.cache_clear()
    except Exception:
        pass
    return jsonify({'ok': True, 'snapshot': ts, 'file': src})


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


@api_bp.route('/run/pipeline', methods=['POST'])
def run_pipeline():
    """一键全流程：steps=factor,analyze,train,trader（finhack.api.functional.run_pipeline）。"""
    from finhack.api.functional import run_pipeline
    from finhack.server.default.tasks import create_task
    data = request.json or {}
    task_id = create_task(run_pipeline, **data)
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
    # 覆盖前快照现有文件到 .versions/（仅当目标已存在）
    snap = None
    if os.path.isfile(path):
        snap = _snapshot_strategy(d, name, path)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)
    return jsonify({'ok': True, 'path': path, 'snapshot': snap})


def _versions_dir(market_dir):
    vd = os.path.join(market_dir, '.versions')
    os.makedirs(vd, exist_ok=True)
    return vd


def _snapshot_strategy(market_dir, name, current_path):
    """把 current_path 内容快照到 .versions/{name}.{ts}.py，返回 ts。"""
    import time as _t
    ts = _t.strftime('%Y%m%d_%H%M%S')
    vd = _versions_dir(market_dir)
    snap = os.path.join(vd, f'{name}.{ts}.py')
    try:
        with open(current_path, 'r', encoding='utf-8') as f:
            old = f.read()
        with open(snap, 'w', encoding='utf-8') as f:
            f.write(old)
        return ts
    except Exception as e:
        return None


@api_bp.route('/strategy_versions')
def strategy_versions():
    """列某策略的历史版本（ts/大小/mtime，倒序）。"""
    import glob as _g
    import re
    market = request.args.get('market', 'cn_stock')
    name = request.args.get('name', '')
    if not re.match(r'^\w+$', name):
        return jsonify({'error': 'invalid name'}), 400
    vd = _versions_dir(_strategies_dir(market))
    out = []
    for f in _g.glob(os.path.join(vd, f'{name}.*.py')):
        b = os.path.basename(f)
        m = re.match(rf'^{re.escape(name)}\.(\d{{8}}_\d{{6}})\.py$', b)
        if not m:
            continue
        out.append({'ts': m.group(1), 'file': b, 'size': os.path.getsize(f),
                    'mtime': os.path.getmtime(f)})
    out.sort(key=lambda x: x['ts'], reverse=True)
    return jsonify({'market': market, 'name': name, 'versions': out[:50]})


@api_bp.route('/strategy_version')
def strategy_version_get():
    """读某历史版本内容。"""
    import re
    market = request.args.get('market', 'cn_stock')
    name = request.args.get('name', '')
    ts = request.args.get('ts', '')
    if not re.match(r'^\w+$', name) or not re.match(r'^\d{8}_\d{6}$', ts):
        return jsonify({'error': 'invalid name/ts'}), 400
    vd = _versions_dir(_strategies_dir(market))
    path = os.path.realpath(os.path.join(vd, f'{name}.{ts}.py'))
    base = os.path.realpath(vd)
    if not path.startswith(base + os.sep) or not os.path.isfile(path):
        return jsonify({'error': 'not found'}), 404
    with open(path, 'r', encoding='utf-8') as f:
        return jsonify({'name': name, 'ts': ts, 'content': f.read()})


@api_bp.route('/strategy_restore', methods=['POST'])
def strategy_restore():
    """恢复某历史版本：先快照当前（可逆），再把该版本覆盖回 live 文件。"""
    import re
    from finhack.library import market_context as m
    data = request.json or {}
    market = data.get('market', 'cn_stock')
    name = data.get('name', '')
    ts = data.get('ts', '')
    if market not in m.list_markets() or not re.match(r'^\w+$', name) or not re.match(r'^\d{8}_\d{6}$', ts):
        return jsonify({'error': 'invalid market/name/ts'}), 400
    d = _strategies_dir(market)
    live = os.path.realpath(os.path.join(d, name + '.py'))
    base = os.path.realpath(d)
    if not live.startswith(base + os.sep):
        return jsonify({'error': 'path escape'}), 400
    vd = _versions_dir(d)
    ver = os.path.realpath(os.path.join(vd, f'{name}.{ts}.py'))
    vbase = os.path.realpath(vd)
    if not ver.startswith(vbase + os.sep) or not os.path.isfile(ver):
        return jsonify({'error': 'version not found'}), 404
    snap = _snapshot_strategy(d, name, live) if os.path.isfile(live) else None
    with open(ver, 'r', encoding='utf-8') as f:
        content = f.read()
    with open(live, 'w', encoding='utf-8') as f:
        f.write(content)
    return jsonify({'ok': True, 'restored_ts': ts, 'snapshot_before': snap})


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



# ===================== 定时任务管理(cron) =====================

@api_bp.route('/cron/list')
def cron_list():
    """扫描 root crontab + /etc/cron.d, 返回任务列表(含来源/调度/项目相关/运行状态)"""
    from loader.cron_loader import scan_tasks, is_task_running
    all_flag = request.args.get('all', 'false').lower() in ('true', '1', 'yes')
    tasks = scan_tasks()
    for t in tasks:
        t['running'] = is_task_running(t['command'])
    if not all_flag:
        tasks = [t for t in tasks if t['project']]
    return jsonify({'tasks': tasks, 'count': len(tasks)})


@api_bp.route('/cron/run', methods=['POST'])
def cron_run():
    """手动运行一个任务(后台)。body: {"id": N}"""
    from loader.cron_loader import run_task, scan_tasks
    data = request.get_json(silent=True) or {}
    idn = data.get('id')
    if idn is None:
        return jsonify({'error': '缺少 id'}), 400
    tasks = scan_tasks()
    t = next((x for x in tasks if x['id'] == int(idn)), None)
    if not t:
        return jsonify({'error': f'无 id={idn}'}), 404
    pid = run_task(int(idn), background=True)
    return jsonify({'ok': True, 'pid': pid, 'command': t['command'][:80]})


@api_bp.route('/cron/add', methods=['POST'])
def cron_add():
    """新增任务。body: {"schedule": "0 2 * * *", "cmd": "...", "to": "cron.d"}"""
    from loader.cron_loader import add_task
    data = request.get_json(silent=True) or {}
    sched = data.get('schedule', '').strip()
    cmd = data.get('cmd', '').strip()
    to = data.get('to', 'cron.d')
    if not sched or not cmd:
        return jsonify({'error': '缺少 schedule 或 cmd'}), 400
    try:
        line = add_task(sched, cmd, to)
        return jsonify({'ok': True, 'line': f"{sched} {cmd}", 'to': to})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@api_bp.route('/cron/rm', methods=['POST'])
def cron_rm():
    """删除任务。body: {"id": N}"""
    from loader.cron_loader import rm_task
    data = request.get_json(silent=True) or {}
    idn = data.get('id')
    if idn is None:
        return jsonify({'error': '缺少 id'}), 400
    try:
        raw = rm_task(int(idn))
        if raw is None:
            return jsonify({'error': f'无 id={idn} 或不支持删'}), 404
        return jsonify({'ok': True, 'removed': raw})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
