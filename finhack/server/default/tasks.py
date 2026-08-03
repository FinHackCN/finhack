# -*- coding: utf-8 -*-
"""异步任务管理：threading 后台执行 finhack API（compute/train/backtest 等）。
dashboard 通过 POST /api/run/* 创建任务，GET /api/task/<id> 轮询进度。
捕获引擎 loguru 日志到 task['log']，解析回测进度（共N天 / [性能]===== / 日总耗时）。
注意：finhack 用 loguru（非 stdlib logging），必须用 loguru.logger.add(sink) 才能接到引擎日志。"""
import re
import time
import threading
import uuid
import traceback

_task_store = {}  # {task_id: {status, result, error, log:[], created_at}}
_lock = threading.Lock()
_LOG_CAP = 400  # 环形 buffer 上限

_DAY_TOTAL_RE = re.compile(r'共\s*(\d+)\s*个交易日')
_EQUITY_RE = re.compile(r'日期:\s*(\S+),\s*总资产:\s*([\d.]+)')


def _make_sink(task_id):
    """loguru sink：追加 record.message 到环形 buffer，并实时维护 total/done 计数器
    （不依赖 buffer，避免滚动后丢总数/少计 done）。"""
    def _sink(message):
        try:
            msg = message.record['message']
            with _lock:
                t = _task_store.get(task_id)
                if t is None:
                    return
                t['log'].append(msg)
                if len(t['log']) > _LOG_CAP:
                    del t['log'][:len(t['log']) - _LOG_CAP]
                if t.get('total') is None:
                    m = _DAY_TOTAL_RE.search(msg)
                    if m:
                        t['total'] = int(m.group(1))
                if '[性能] =====' in msg:
                    t['done'] = t.get('done', 0) + 1
                # 解析每日总资产 → 实时净值曲线
                em = _EQUITY_RE.search(msg)
                if em:
                    t['equity'].append((em.group(1), float(em.group(2))))
        except Exception:
            pass
    return _sink


def _progress_of(t):
    total = t.get('total')
    done = t.get('done', 0)
    pct = round(done / total * 100) if total else 0
    return {'total': total, 'done': done, 'pct': min(pct, 100) if total else 0}


def create_task(func, *args, **kwargs):
    """创建异步任务。返回 task_id。"""
    task_id = str(uuid.uuid4())[:8]
    with _lock:
        _task_store[task_id] = {'status': 'pending', 'result': None, 'error': None,
                                'log': [], 'total': None, 'done': 0, 'equity': [],
                                'created_at': time.time()}

    def _run():
        from loguru import logger as _llog
        import finhack.library.log as _flog
        sink_fn = _make_sink(task_id)

        # patch Log/tLog 类：引擎每次 (re)init logger（会 remove all）后，追加我们的 sink
        _OrigLog, _OrigTLog = _flog.Log, _flog.tLog

        def _wrap(cls):
            def _w(*a, **kw):
                obj = cls(*a, **kw)
                try:
                    _llog.add(sink_fn, level='INFO')
                except Exception:
                    pass
                return obj
            return _w
        _flog.Log = _wrap(_OrigLog)
        _flog.tLog = _wrap(_OrigTLog)
        sid = None
        try:
            sid = _llog.add(sink_fn, level='INFO')   # 立即挂一个（任务内可能不触发 Log.init）
            with _lock:
                _task_store[task_id]['status'] = 'running'
            result = func(*args, **kwargs)
            with _lock:
                _task_store[task_id]['status'] = 'done'
                _task_store[task_id]['result'] = _safe_result(result)
        except Exception as e:
            with _lock:
                _task_store[task_id]['status'] = 'error'
                _task_store[task_id]['error'] = str(e) + '\n' + traceback.format_exc()
        finally:
            _flog.Log = _OrigLog
            _flog.tLog = _OrigTLog
            if sid is not None:
                try:
                    _llog.remove(sid)
                except Exception:
                    pass

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return task_id


def get_task(task_id):
    with _lock:
        t = _task_store.get(task_id)
        if t is None:
            return {'status': 'not_found'}
        log = list(t.get('log', []))
        return {
            'status': t['status'],
            'result': t.get('result'),
            'error': t.get('error'),
            'progress': _progress_of(t),
            'equity': t.get('equity', [])[-500:],  # 最近 500 点（避免过大）
            'log': log[-40:],  # 末 40 行
            'created_at': t.get('created_at'),
        }


def _safe_result(result):
    """把 result 转成 JSON 友好格式"""
    import numpy as np
    import pandas as pd
    if result is None:
        return None
    if isinstance(result, pd.DataFrame):
        return {'type': 'DataFrame', 'shape': list(result.shape), 'columns': list(result.columns)}
    if isinstance(result, tuple):
        return {'type': 'tuple', 'values': [str(x) if isinstance(x, (np.floating, np.integer)) else x for x in result]}
    if isinstance(result, (np.floating, np.integer)):
        return float(result)
    if isinstance(result, str):
        return result
    return str(result)
