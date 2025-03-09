#!/usr/bin/env python
import os
import sys
import multiprocessing
import atexit
from finhack.core.core import Core

def child_process_action(core):
    """子进程执行函数
    
    Args:
        core: Core实例
    """
    # 注册子进程退出时的清理函数，如果有的话
    # atexit.register(Utils.child_cleanup)
    core.do_action()
    # 子进程完成任务后退出
    os._exit(0)

def main():
    """主函数，处理命令行参数并启动框架"""
    # 初始化核心类
    core = Core()
    
    # 加载模块
    core.load_module()

    # 处理后台运行
    from finhack.library.utils import Utils
    if core.args.background:
        # 创建子进程
        pid = os.fork()  # 创建一个新进程
        if pid == -1:  # 创建进程失败
            print("创建后台进程失败！")
            sys.exit(1)
        elif pid == 0:  # 子进程中执行
            child_process_action(core)
        else:  # 父进程中继续执行
            Utils.write_pids(pid)
            print("启动后台任务！")
    else:
        # 注册父进程退出时的清理函数
        # atexit.register(Utils.auto_exit)
        core.do_action()

if __name__ == '__main__':
    main()