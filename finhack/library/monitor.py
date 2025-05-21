import time
from finhack.library.alert import alert
import traceback
import finhack.library.log as Log

class tsMonitor:
    """
    Tushare API调用监视器
    处理限流、权限和异常重试
    """
    def __init__(self, func):
        self.func = func
 
    def __get__(self, instance, owner):
        def wrapper(*args, **kwargs):
            res = False
            try_times = 0
            while True:
                try:
                    res = self.func(*args, **kwargs)
                    break
                except Exception as e:
                    if "每分钟最多访问" in str(e):
                        Log.logger.warning(f"{self.func.__name__}:触发限流，等待重试。\n{str(e)}")
                        time.sleep(15)
                        continue
                    
                    elif "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                        Log.logger.warning(f"{self.func.__name__}:今日权限用完。\n{str(e)}")
                        break
                   
                    elif "您没有访问该接口的权限" in str(e):
                        Log.logger.warning(f"{self.func.__name__}:没有访问该接口的权限。\n{str(e)}")
                        break
                
                    else:
                        if try_times < 10:
                            try_times = try_times + 1
                            Log.logger.error(f"{self.func.__name__}:未知异常，等待重试。\n{str(e)}")
                            alert.send(self.func.__name__, '未知异常，等待重试。\n', str(e))
                            time.sleep(15)
                            continue
                        else:
                            info = traceback.format_exc()
                            Log.logger.error(f"{self.func.__name__}:同步异常，{str(info)}")
                            alert.send(self.func.__name__, '同步异常', str(info))
                            break
            Log.logger.info(f"{self.func.__name__}:同步完毕")
            return res
        return wrapper
        
        
class dbMonitor:
    """
    数据库操作监视器
    处理数据库连接异常和重试
    """
    def __init__(self, func):
        self.func = func
 
    def __get__(self, instance, owner):
        def wrapper(*args, **kwargs):
            res = None
            try_times = 0
            
            # 设置最大重试次数和延迟
            max_retries = 5
            retry_delay = 1
            
            # 获取方法名，用于日志记录
            func_name = self.func.__name__
            
            while try_times <= max_retries:
                try:
                    # 如果是类方法调用（instance存在），确保正确传递self参数
                    if instance is not None:
                        res = self.func(instance, *args, **kwargs)
                    else:
                        res = self.func(*args, **kwargs)
                    break
                except Exception as e:
                    error_msg = str(e).lower()
                    
                    # 特定错误不需要重试
                    if "exist" in error_msg or 'duplicate' in error_msg:
                        Log.logger.info(f"{func_name}: {str(e)}")
                        return False
                    
                    # 数据库锁定或连接错误需要重试
                    retry_needed = (
                        "database is locked" in error_msg or 
                        "database is busy" in error_msg or
                        "connect" in error_msg or
                        "timeout" in error_msg
                    )
                    
                    if retry_needed and try_times < max_retries:
                        try_times += 1
                        Log.logger.warning(f"{func_name}: 数据库操作异常，等待重试 ({try_times}/{max_retries})。\n{str(e)}")
                        
                        # 记录调用参数，有助于调试
                        if args:
                            arg_str = ", ".join([f"{a}" for a in args])
                            Log.logger.warning(f"参数: {arg_str}")
                        if kwargs:
                            kwarg_str = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
                            Log.logger.warning(f"关键字参数: {kwarg_str}")
                            
                        # 指数退避，每次重试延迟增加
                        sleep_time = retry_delay * (2 ** (try_times - 1))
                        time.sleep(sleep_time)
                        continue
                    else:
                        # 记录详细错误信息
                        info = traceback.format_exc()
                        Log.logger.error(f"{func_name}: 数据库操作失败，{str(e)}\n{info}")
                        
                        # 如果是非重试错误或已达到最大重试次数，则停止重试
                        if try_times >= max_retries:
                            Log.logger.error(f"{func_name}: 达到最大重试次数 {max_retries}")
                        
                        # 根据方法返回类型，提供适当的默认值
                        if func_name.startswith('select'):
                            if func_name == 'select_to_df':
                                import pandas as pd
                                return pd.DataFrame()
                            elif func_name == 'select_one':
                                return None
                            else:
                                return []
                        elif func_name in ['table_exists', 'truncate_table']:
                            return False
                        elif func_name in ['to_sql', 'safe_to_sql']:
                            return 0
                        elif func_name == 'replace_table':
                            # 获取源表名参数（通常是第二个参数）
                            source_table = args[1] if len(args) > 1 else None
                            return (False, source_table)
                        else:
                            return None
            return res
        return wrapper