"""
工具类模块

包含：
- FloatValidation: 浮点安全验证
"""

from .float_validation import is_valid_volume, is_valid_price

__all__ = [
    "is_valid_volume",
    "is_valid_price",
]
