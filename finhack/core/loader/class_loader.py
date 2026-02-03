"""
类加载器 - 用户模块优先

注意：此文件避免导入 runtime，以防止循环导入问题
"""

import os
import sys
import traceback
import importlib
import importlib.util


class ClassLoader:
    """类加载器 - 用户模块优先"""

    @staticmethod
    def get_module(module_path='', user_module_path=''):
        """
        加载模块：用户优先 → 系统 → 报错

        Args:
            module_path: 系统模块路径，如 'finhack.core.loader.trader_loader'
            user_module_path: 用户模块路径，如 '/project/loader/trader_loader.py'

        Returns:
            module: 加载的模块对象

        Raises:
            ImportError: 模块加载失败
        """
        # 延迟导入 Log，避免循环导入
        try:
            import finhack.library.log as Log
            logger = Log.logger
        except Exception:
            logger = None

        # 1. 优先加载用户自定义模块
        if user_module_path and os.path.exists(user_module_path):
            filename = os.path.basename(user_module_path)
            module_name = "user_module"
            if filename.endswith(".py"):
                module_name = filename[:-3]
            try:
                module_spec = importlib.util.spec_from_file_location(module_name, user_module_path)
                module = importlib.util.module_from_spec(module_spec)
                module_spec.loader.exec_module(module)
                if logger:
                    logger.debug(f"成功加载用户模块: {user_module_path}")
                return module
            except Exception as e:
                if logger:
                    logger.error(f"加载用户模块失败: {user_module_path}, 错误: {e}")
                traceback.print_exc()
                raise

        # 2. 加载系统模块
        elif module_path:
            try:
                module = importlib.import_module(module_path)
                if logger:
                    logger.debug(f"成功加载系统模块: {module_path}")
                return module
            except ModuleNotFoundError as e:
                if logger:
                    logger.critical(f"模块不存在: 用户路径={user_module_path}, 系统路径={module_path}")
                traceback.print_exc()
                raise

        # 3. 两者都不存在
        else:
            if logger:
                logger.critical("module_path 和 user_module_path 均未提供")
            raise ValueError("必须提供 module_path 或 user_module_path")
