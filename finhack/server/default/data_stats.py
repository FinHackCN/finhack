# -*- coding: utf-8 -*-
"""数据存储统计（dashboard 数据管理面板用，对标 finhack check 的输出维度）。
统计：总占用/各子目录占用/磁盘空闲、每市场 kline+因子大小、代码数、因子数、
日期范围、最新同步时间（kline/因子）。du+find 走 inode 缓存很快，但仍缓存 + 后台刷新。"""
import os
import time
import shutil
import threading
import subprocess

_CACHE = {'stats': None, 'at': 0, 'computing': False}
_LOCK = threading.Lock()
_TTL = 300  # 5 分钟缓存


def _du_bytes(path):
    """du -sb（apparent bytes）。不存在返回 0。"""
    if not path or not os.path.exists(path):
        return 0
    try:
        out = subprocess.run(['du', '-sb', path], capture_output=True, text=True, timeout=180).stdout
        return int(out.split()[0]) if out.strip() else 0
    except Exception:
        return 0


def _du_depth1(path):
    """du -b --max-depth=1 → {basename: bytes}（含 root 自身键 = 总量）。一次遍历拿全部子目录。
    注意 -s 与 --max-depth 互斥，故只用 --max-depth=1。"""
    if not path or not os.path.isdir(path):
        return {}
    try:
        out = subprocess.run(['du', '-b', '--max-depth=1', path], capture_output=True, text=True, timeout=600).stdout
        res = {}
        for line in out.splitlines():
            parts = line.split(None, 1)
            if len(parts) != 2:
                continue
            sz, p = parts
            name = os.path.basename(os.path.normpath(p))
            res[name] = int(sz)
        return res
    except Exception:
        return {}


def _newest_market(root):
    """某市场目录下最新文件的 mtime。只扫每个 freq 下最新 2 个 year 目录
    （最新数据总在近年），避免遍历整个市场的百万文件。"""
    best = None
    if not root or not os.path.isdir(root):
        return None
    for frq in os.listdir(root):
        yd = os.path.join(root, frq)
        if not os.path.isdir(yd):
            continue
        years = [y for y in os.listdir(yd) if y.isdigit()]
        for y in sorted(years)[-2:]:
            d = os.path.join(yd, y)
            try:
                out = subprocess.run(['find', d, '-type', 'f', '-printf', '%T@\\n'],
                                     capture_output=True, text=True, timeout=60).stdout
                ts = [float(x) for x in out.split() if x]
                if ts:
                    m = max(ts)
                    if best is None or m > best:
                        best = m
            except Exception:
                pass
    return best


def _file_type_stats(path):
    """全树按文件扩展名聚合（文件数 + 大小）。find 流式输出 → Python 逐行聚合，
    常量内存（不把百万行读进内存）。全 DATA_DIR 约 12s。"""
    agg = {}
    if not path or not os.path.isdir(path):
        return []
    try:
        proc = subprocess.Popen(['find', path, '-type', 'f', '-printf', '%s %f\\n'],
                                stdout=subprocess.PIPE, text=True)
        for line in proc.stdout:
            sp = line.find(' ')
            if sp < 0:
                continue
            try:
                sz = int(line[:sp])
            except ValueError:
                continue
            fn = line[sp + 1:].rstrip('\n')
            dot = fn.rfind('.')
            ext = fn[dot + 1:].lower() if dot > 0 else '(无扩展名)'
            d = agg.setdefault(ext, [0, 0])
            d[0] += 1
            d[1] += sz
        proc.wait()
    except Exception:
        pass
    out = [{'ext': e, 'count': c, 'size': s} for e, (c, s) in agg.items()]
    out.sort(key=lambda x: -x['size'])
    return out


def compute_data_stats():
    """全量统计。du --max-depth=1 拿子目录大小，find 扫最新年目录取同步时间，
    find 流式聚合文件类型；并行 → ~25-30s（瓶颈是最慢的树）。返回 dict。"""
    from concurrent.futures import ThreadPoolExecutor
    from runtime.constant import DATA_DIR, KLINE_DIR
    from finhack.library import market_context as mctx
    from finhack.factor.default.factorManager import factorManager
    from finhack.library.data import get_data_interface
    di = get_data_interface()
    t0 = time.time()

    subdirs = ['market', 'factors', 'cache', 'db', 'logs', 'backtest', 'preds', 'models', 'reports', 'config', 'running']
    cb_root = os.path.join(KLINE_DIR, 'codebased')
    tb_root = os.path.join(KLINE_DIR, 'timebased')
    fac_root = os.path.join(DATA_DIR, 'factors', 'matrix')

    # 先确定市场集合（os.listdir 即时），再并行：4 个 du + 每市场 2 个 find
    try:
        ctx_markets = mctx.list_markets()
    except Exception:
        ctx_markets = []
    cb_markets = [d for d in os.listdir(cb_root)] if os.path.isdir(cb_root) else []
    fac_markets = [d for d in os.listdir(fac_root)] if os.path.isdir(fac_root) else []
    all_markets = sorted(set(cb_markets + fac_markets + ctx_markets))

    with ThreadPoolExecutor(max_workers=12) as ex:
        f_data = ex.submit(_du_depth1, DATA_DIR)
        f_cb = ex.submit(_du_depth1, cb_root)
        f_tb = ex.submit(_du_depth1, tb_root)
        f_fac = ex.submit(_du_depth1, fac_root)
        f_ftype = ex.submit(_file_type_stats, DATA_DIR)
        sync_futs = {mk: ex.submit(_newest_market, os.path.join(cb_root, mk)) for mk in all_markets}
        fsync_futs = {mk: ex.submit(_newest_market, os.path.join(fac_root, mk)) for mk in all_markets}
        data_depth = f_data.result()
        cb_depth = f_cb.result()
        tb_depth = f_tb.result()
        fac_depth = f_fac.result()
        file_types = f_ftype.result()
        sync_map = {mk: sync_futs[mk].result() for mk in all_markets}
        fsync_map = {mk: fsync_futs[mk].result() for mk in all_markets}

    disk = shutil.disk_usage(DATA_DIR)
    data_self_key = os.path.basename(DATA_DIR.rstrip('/'))
    total = data_depth.get(data_self_key, sum(data_depth.values()))

    by_subdir = {s: data_depth.get(s, 0) for s in subdirs}

    markets_out = []
    for mk in all_markets:
        cfg = {}
        try:
            cfg = mctx.get_market_config(mk) or {}
        except Exception:
            cfg = {}
        kcb = cb_depth.get(mk, 0)
        ktb = tb_depth.get(mk, 0)
        fsz = fac_depth.get(mk, 0)
        codes = 0
        try:
            sl = di.get_stock_list(market=mk, use_cache=False)
            codes = int(len(sl)) if sl is not None else 0
        except Exception:
            codes = 0
        freqs = cfg.get('freq_support', ['1d'])
        fac_counts = {}
        for frq in freqs:
            try:
                fac_counts[frq] = len(factorManager.list_factors(market=mk, freq=frq))
            except Exception:
                fac_counts[frq] = 0
        years = []
        for frq in freqs:
            yd = os.path.join(cb_root, mk, frq)
            if os.path.isdir(yd):
                years += [int(b) for b in os.listdir(yd) if b.isdigit() and os.path.isdir(os.path.join(yd, b))]
        markets_out.append({
            'market': mk,
            'kline_size': kcb + ktb, 'kline_cb': kcb, 'kline_tb': ktb,
            'factors_size': fsz, 'codes': codes,
            'factors': fac_counts,
            'date_range': [min(years), max(years)] if years else None,
            'latest_sync': sync_map.get(mk),
            'latest_factors_sync': fsync_map.get(mk),
            'freq_support': freqs, 'benchmark': cfg.get('benchmark'),
            'currency': cfg.get('currency'), 'is_derivatives': cfg.get('is_derivatives', False),
        })

    return {
        'computed_at': time.time(),
        'elapsed': round(time.time() - t0, 1),
        'total': total,
        'by_subdir': by_subdir,
        'file_types': file_types,
        'disk': {'total': disk.total, 'used': disk.used, 'free': disk.free},
        'markets': markets_out,
    }


def get_data_stats():
    """返回缓存统计。若过期/无，标记 stale（调用方可据此触发刷新）。"""
    with _LOCK:
        fresh = _CACHE['stats'] is not None and (time.time() - _CACHE['at']) < _TTL
        return {
            'stats': _CACHE['stats'],
            'stale': not fresh,
            'computing': _CACHE['computing'],
            'age': (time.time() - _CACHE['at']) if _CACHE['at'] else None,
        }


def refresh_data_stats_async():
    """后台刷新（幂等：已在跑则返回 False）。"""
    with _LOCK:
        if _CACHE['computing']:
            return False
        _CACHE['computing'] = True

    def _run():
        try:
            s = compute_data_stats()
            with _LOCK:
                _CACHE['stats'] = s
                _CACHE['at'] = time.time()
                _CACHE['computing'] = False
        except Exception as e:
            with _LOCK:
                _CACHE['computing'] = False
            import finhack.library.log as Log
            Log.logger.warning(f"data_stats refresh 失败: {e}")

    threading.Thread(target=_run, daemon=True).start()
    return True
