"""
撮合引擎模块（历史占位）

注意：撮合逻辑实际实现于 engine/backtest_engine.py 的 TradeCenter.try_match_orders_sync。
本目录历史上的 base_matcher/limit_matcher/market_matcher/slippage 四个模块已迁出且文件不存在，
原先的 `from .xxx import` 会在被 import 时触发 ModuleNotFoundError。
现保留为空占位，避免任何 `import matching` 调用崩溃。如需撮合，请使用 backtest_engine 中的实现。
"""

__all__ = []
