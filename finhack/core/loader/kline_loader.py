import os
import sys
from runtime.constant import *
import runtime.global_var as global_var
import finhack.library.log as Log
from finhack.core.loader.class_loader import ClassLoader
import importlib

class KlineLoader():
    def __init__(self, args):
        if args.background:
            self.background(args)

        self.args = args
        self.module_name = args.module
        
        if self.args.vendor == None:
            self.vendor = "default"
        else:
            self.vendor = self.args.vendor
            
        if args.action == "stop":
            return
            
        self.module_path = "finhack." + self.module_name + "." + self.vendor + "." + self.vendor + '_' + self.module_name
        self.user_module_path = BASE_DIR + "/" + self.module_name + "/" + self.vendor + "/" + self.vendor + '_' + self.module_name + ".py"
        self.module = ClassLoader.get_module(module_path=self.module_path, user_module_path=self.user_module_path)
        Log.logger.debug(f"module_path:{self.module_path}")
        Log.logger.debug(f"user_module_path:{self.user_module_path}")
        
        try:
            # 分步处理：先获取类，再实例化
            class_name = self.vendor.capitalize() + self.module_name.capitalize()
            
            # 第一步：检查类是否存在
            try:
                klass_type = getattr(self.module, class_name)
                Log.logger.debug(f"成功找到类: {class_name}")
            except AttributeError as e:
                Log.logger.error(f"找不到类 '{class_name}' 在模块 {self.module}")
                Log.logger.error(f"模块中可用的类和函数: {[name for name in dir(self.module) if not name.startswith('_')]}")
                raise AttributeError(f"模块 {self.module.__name__} 中不存在类 '{class_name}'") from e
            
            # 第二步：尝试实例化类
            try:
                self.klass = klass_type(self.args)
                Log.logger.debug(f"成功实例化类: {class_name}")
            except Exception as e:
                Log.logger.error(f"实例化类 '{class_name}' 时出错: {str(e)}")
                Log.logger.error(f"错误类型: {type(e).__name__}")
                Log.logger.error("类实例化失败，可能的原因:")
                Log.logger.error("1. 类的__init__方法有错误")
                Log.logger.error("2. 类依赖的模块或包未正确导入")
                Log.logger.error("3. 类初始化需要必要的参数但未提供")
                Log.logger.error("完整错误信息:")
                import traceback
                traceback.print_exc()
                raise Exception(f"类 '{class_name}' 实例化失败: {str(e)}") from e
                
            self.klass.args = self.args
            Log.logger.debug(f"klass object:{self.klass}")
            
        except AttributeError as e:
            # 处理类不存在的情况
            class_name = self.vendor.capitalize() + self.module_name.capitalize()
            Log.logger.error(f"类 '{class_name}' 不存在")
            Log.logger.error(f"模块路径: {self.module_path}")
            Log.logger.error(f"用户模块路径: {self.user_module_path}")
            
            if hasattr(self.module, '__file__'):
                Log.logger.error(f"实际加载的模块文件: {self.module.__file__}")
            
            # 检查文件是否存在
            module_file_path = self.module_path.replace('.', '/') + ".py"
            if not os.path.exists(module_file_path) and not os.path.exists(self.user_module_path):
                Log.logger.error(f"模块文件均不存在:")
                Log.logger.error(f"  - 系统模块: {module_file_path}")
                Log.logger.error(f"  - 用户模块: {self.user_module_path}")
            
            Log.logger.error(f"请检查:")
            Log.logger.error(f"1. 类名是否正确: {class_name}")
            Log.logger.error(f"2. 模块文件是否存在且语法正确")
            Log.logger.error(f"3. 类是否正确定义并导出")
            Log.logger.error(f"4. 包是否正确安装")
            
            print("完整错误追踪:", file=sys.stderr)
            import traceback
            traceback.print_exc()
            exit()
            
        except Exception as e:
            # 处理其他类型的异常
            class_name = self.vendor.capitalize() + self.module_name.capitalize()
            Log.logger.error(f"加载类 '{class_name}' 时发生未知错误: {str(e)}")
            Log.logger.error(f"错误类型: {type(e).__name__}")
            Log.logger.error("请检查:")
            Log.logger.error("1. 模块文件语法是否正确")
            Log.logger.error("2. 模块依赖是否完整")
            Log.logger.error("3. 类定义是否正确")
            
            print("完整错误追踪:", file=sys.stderr)
            import traceback
            traceback.print_exc()
            exit()
        
    def background(self, args):
        pass
    
    def run(self):
        klass = self.klass
        klass.args = self.args
        klass.run()
        
    def cache(self):
        klass = self.klass
        klass.args = self.args
        klass.cache()
        
    def stop(self):
        if global_var.args.vendor != None:
            pids_path = os.path.join(BASE_DIR, "data/cache/runtime", f"{global_var.module_name}_{global_var.args.vendor}.pids")
            process_name_pattern = f"{global_var.module_name}_{global_var.args.vendor}"
        else:
            pids_path = os.path.join(BASE_DIR, "data/cache/runtime", f"{global_var.module_name}.pids")
            process_name_pattern = f"{global_var.module_name}"
        
        # 通过PID文件终止进程
        fall_list = ""
        pid_terminated = False
        
        try:
            if os.path.exists(pids_path):
                with open(pids_path, "r") as f:
                    # 逐行读取文件内容
                    line = f.readline()
                    while line:
                        # 处理每一行的内容
                        try:
                            pid = int(line.strip())
                            import psutil
                            if psutil.pid_exists(pid):
                                try:
                                    parent = psutil.Process(pid)
                                    children = parent.children(recursive=True)
                                    for child in children:
                                        try:
                                            child.terminate()  # 终止子进程
                                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                                            pass
                                    parent.terminate()  # 终止父进程
                                    pid_terminated = True
                                except psutil.NoSuchProcess:
                                    pass
                                except psutil.AccessDenied:
                                    Log.logger.warning(f"无权限终止进程 PID: {pid}")
                                    fall_list = fall_list + str(pid) + "\n"
                                    
                            # 等待进程终止
                            import time
                            time.sleep(1)
                            if psutil.pid_exists(pid):
                                try:
                                    # 如果进程仍然存在，尝试强制终止
                                    parent = psutil.Process(pid)
                                    parent.kill()
                                    time.sleep(0.5)
                                except (psutil.NoSuchProcess, psutil.AccessDenied):
                                    pass
                                    
                            # 再次检查进程是否存在
                            if psutil.pid_exists(pid):
                                fall_list = fall_list + str(pid) + "\n"
                        except ValueError:
                            Log.logger.warning(f"PID文件中包含无效数据: {line.strip()}")
                        # 读取下一行内容
                        line = f.readline()
                
                with open(pids_path, "w") as f:
                    f.write(fall_list)
        except Exception as e:
            Log.logger.error(f"通过PID终止进程时出错: {str(e)}")
            import traceback
            traceback.print_exc()
        
        # 输出结果
        if fall_list == "" and pid_terminated:
            Log.logger.info("停止任务成功！")
        elif fall_list != "":
            Log.logger.warning(f"部分进程停止失败！pid列表：{fall_list}")
        elif not pid_terminated:
            Log.logger.info("未找到需要停止的进程")
