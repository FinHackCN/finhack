# -*- coding: utf-8 -*-
"""异步任务管理：threading 后台执行 finhack API（compute/train/backtest 等）。
dashboard 通过 POST /api/run/* 创建任务，GET /api/task/<id> 轮询进度。"""
import threading
import uuid
import traceback

_task_store = {}  # {task_id: {status, result, error}}
_lock = threading.Lock()


def create_task(func, *args, **kwargs):
    """创建异步任务。返回 task_id。"""
    task_id = str(uuid.uuid4())[:8]
    with _lock:
        _task_store[task_id] = {'status': 'pending', 'result': None, 'error': None}

    def _run():
        try:
            with _lock:
                _task_store[task_id]['status'] = 'running'
            result = func(*args, **kwargs)
            # result 可能是 DataFrame/tuple/None — 尝试 JSON 友好化
            with _lock:
                _task_store[task_id]['status'] = 'done'
                _task_store[task_id]['result'] = _safe_result(result)
        except Exception as e:
            with _lock:
                _task_store[task_id]['status'] = 'error'
                _task_store[task_id]['error'] = str(e) + '\n' + traceback.format_exc()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return task_id


def get_task(task_id):
    with _lock:
        return _task_store.get(task_id, {'status': 'not_found'})


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
