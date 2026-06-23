# -*- coding: utf-8 -*-
"""
浮点数验证工具
==============

提供浮点安全的数量/价格验证，避免 IEEE 754 精度导致的取模问题。

例如 volume=0.01, step=1e-8 时，0.01 % 1e-8 在浮点运算中
可能产生非零余数，导致正确的订单被拒绝。
"""


def is_valid_volume(volume: float, step: float, tolerance: float = 1e-6) -> bool:
    """检查 volume 是否是 step 的整数倍（浮点安全）

    Args:
        volume: 订单数量
        step: 最小数量步长 (lot_size / min_order_quantity)
        tolerance: 允许的浮点误差

    Returns:
        bool: True 表示有效
    """
    if step <= 0 or volume <= 0:
        return True
    remainder = volume % step
    return remainder <= tolerance or abs(remainder - step) <= tolerance


def is_valid_price(price: float, tick_size: float, tolerance: float = 1e-6) -> bool:
    """检查 price 是否是 tick_size 的整数倍（浮点安全）

    Args:
        price: 订单价格
        tick_size: 最小价格变动单位
        tolerance: 允许的浮点误差

    Returns:
        bool: True 表示有效
    """
    if tick_size <= 0 or price <= 0:
        return True
    remainder = price % tick_size
    return remainder <= tolerance or abs(remainder - tick_size) <= tolerance
