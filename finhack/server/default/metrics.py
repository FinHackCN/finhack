# -*- coding: utf-8 -*-
"""富绩效指标（dashboard 回测报告用）。从 pickle 里的 returns/bench_returns/daily_history
服务端现算，零引擎改动，自动回填所有历史回测。empyrical(已装) 出标准比率，
VaR/CVaR/kelly/win_rate/duration 用 numpy（参考 performance_analyzer.py:322-437）。"""
import numpy as np


def _safe(fn, *args, **kw):
    try:
        v = fn(*args, **kw)
        if v is None or (isinstance(v, float) and (np.isnan(v) or np.isinf(v))):
            return None
        return float(v)
    except Exception:
        return None


def _var(returns, cl=0.05):
    if not len(returns):
        return None
    s = np.sort(returns)
    i = int(len(s) * cl)
    if i < len(s):
        return float(-s[i])
    return 0.0


def _cvar(returns, cl=0.05):
    if not len(returns):
        return None
    s = np.sort(returns)
    i = int(len(s) * cl)
    if i > 0:
        tail = s[:i]
        return float(-tail.mean())
    return 0.0


def _win_rate(returns):
    if not len(returns):
        return None
    return float((returns > 0).sum() / len(returns))


def _profit_loss_ratio(returns):
    pos = returns[returns > 0]
    neg = returns[returns < 0]
    if len(pos) == 0 or len(neg) == 0:
        return None
    ap, al = pos.mean(), neg.mean()
    return float(ap / abs(al)) if al != 0 else None


def _kelly(returns):
    wr = _win_rate(returns)
    pl = _profit_loss_ratio(returns)
    if wr is None or pl is None or pl <= 0:
        return None
    k = (wr * pl - (1 - wr)) / pl
    return float(max(0, k))


def _max_dd_duration(equity):
    """最长回撤持续期（从峰值到恢复新高的天数）。equity=累计净值序列。"""
    if equity is None or len(equity) < 2:
        return None
    peak = equity[0]
    max_dur = 0
    cur = 0
    for v in equity:
        if v >= peak:
            peak = v
            max_dur = max(max_dur, cur)
            cur = 0
        else:
            cur += 1
    max_dur = max(max_dur, cur)
    return int(max_dur)


def compute_rich_indicators(returns, bench_returns=None, daily_history=None, period='daily', trades=None):
    """返回分组富指标 dict。returns/bench_returns 为日收益率 list/np。"""
    import empyrical as ep
    r = np.asarray(returns, dtype=float)
    r = r[np.isfinite(r)]
    b = np.asarray(bench_returns, dtype=float) if bench_returns is not None else np.array([])
    if len(b):
        b = b[np.isfinite(b)]
    n = min(len(r), len(b))
    rb, bb = (r[:n], b[:n]) if n > 1 else (r, np.array([]))

    out = {'return': {}, 'risk': {}, 'benchmark': {}, 'trades': {}}
    P = dict(period=period)

    # —— 收益性 ——
    out['return']['total_return'] = _safe(lambda: ep.cum_returns_final(r)) if len(r) else None
    out['return']['annual_return'] = _safe(ep.annual_return, r, **P) if len(r) else None
    out['return']['cagr'] = _safe(lambda: (1 + ep.cum_returns_final(r)) ** (252.0 / max(len(r), 1)) - 1) if len(r) else None

    # —— 风险 ——
    if len(r):
        out['risk']['annual_volatility'] = _safe(ep.annual_volatility, r, **P)
        out['risk']['downside_risk'] = _safe(ep.downside_risk, r, **P)
        out['risk']['sharpe'] = _safe(ep.sharpe_ratio, r, **P)
        out['risk']['sortino'] = _safe(ep.sortino_ratio, r, **P)
        out['risk']['calmar'] = _safe(ep.calmar_ratio, r, **P)
        out['risk']['omega'] = _safe(ep.omega_ratio, r)
        out['risk']['max_drawdown'] = _safe(ep.max_drawdown, r)
        out['risk']['tail_ratio'] = _safe(ep.tail_ratio, r)
        out['risk']['stability_r2'] = _safe(ep.stability_of_timeseries, r)
        out['risk']['var_95'] = _var(r, 0.05)
        out['risk']['var_99'] = _var(r, 0.01)
        out['risk']['cvar_95'] = _cvar(r, 0.05)
        out['risk']['cvar_99'] = _cvar(r, 0.01)
        out['risk']['win_rate'] = _win_rate(r)
        out['risk']['profit_loss_ratio'] = _profit_loss_ratio(r)
        out['risk']['kelly'] = _kelly(r)
    # 回撤持续期（自 daily_history 总资产）
    if daily_history:
        try:
            eq = np.array([float(d.get('total_assets', 0)) for d in daily_history], dtype=float)
            eq = eq / eq[0] if len(eq) and eq[0] else eq
            out['risk']['max_dd_duration_days'] = _max_dd_duration(eq)
        except Exception:
            pass

    # —— 基准相对 ——
    if len(bb) > 1:
        ab = _safe(lambda: ep.alpha_beta(rb, bb))
        if isinstance(ab, tuple) and len(ab) == 2:
            out['benchmark']['alpha'] = _safe(lambda: float(ab[0]))
            out['benchmark']['beta'] = _safe(lambda: float(ab[1]))
        out['benchmark']['information_ratio'] = _safe(ep.excess_sharpe, rb, bb)
        out['benchmark']['excess_return'] = _safe(lambda: ep.cum_returns_final(rb) - ep.cum_returns_final(bb))
        # tracking_error = 年化 std(rb-bb)
        try:
            diff = rb - bb
            out['benchmark']['tracking_error'] = float(np.std(diff, ddof=1) * np.sqrt(252)) if len(diff) > 1 else None
        except Exception:
            pass
        out['benchmark']['correlation'] = _safe(lambda: float(np.corrcoef(rb, bb)[0, 1])) if len(rb) > 1 else None

    # —— 交易（粗粒度，自交易笔数/方向）——
    if trades is not None:
        try:
            out['trades']['trade_count'] = len(trades)
            buys = [t for t in trades if (t.get('side') or '') == 'buy']
            sells = [t for t in trades if (t.get('side') or '') == 'sell']
            out['trades']['buys'] = len(buys)
            out['trades']['sells'] = len(sells)
        except Exception:
            pass

    # 去掉 None 值，JSON 友好
    for grp in out:
        out[grp] = {k: v for k, v in out[grp].items() if v is not None}
    return out
