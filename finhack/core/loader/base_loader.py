import psutil
import time
import os
import traceback
from runtime.constant import *
import runtime.global_var as global_var
import finhack.library.log as Log
from finhack.library.class_loader import ClassLoader
import sys
import importlib
class ClassLoader:
    @staticmethod
    def get_module(module_path, user_module_path):
        print(f"Attempting to load module: {module_path}")
        try:
            module = importlib.import_module(module_path)
            print(f"Successfully loaded module: {module_path}")
            return module
        except ImportError as e:
            print(f"Failed to load module {module_path}: {e}")
            if os.path.exists(user_module_path):
                print(f"Attempting to load user module: {user_module_path}")
                spec = importlib.util.spec_from_file_location(module_path, user_module_path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                print(f"Successfully loaded user module: {user_module_path}")
                return module
            else:
                print(f"User module path does not exist: {user_module_path}")
                raise

class BaseLoader():
    def __init__(self,args):
        if args.background:
            self.background(args)

        self.args=args
        self.module_name=args.module
        
        if self.args.vendor==None:
            self.vendor="default"
        else:
            self.vendor=self.args.vendor
            
        if args.action=="stop":
            return
            
        self.module_path="finhack."+self.module_name+"."+self.vendor+"."+self.vendor+'_'+self.module_name
        self.user_module_path=BASE_DIR+"/"+self.module_name+"/"+self.vendor+"/"+self.vendor+'_'+self.module_name+".py"
        self.module = ClassLoader.get_module(module_path=self.module_path,user_module_path=self.user_module_path)
        try:
            self.klass = getattr(self.module, self.vendor.capitalize()+self.module_name.capitalize())()
            self.klass.args=self.args
        except Exception as e:
            if "has no attribute" in str(e) and self.vendor.capitalize()+self.module_name.capitalize() in str(e):
                if os.path.exists(self.module_path) or os.path.exists(self.user_module_path):
                    Log.logger.error( self.vendor.capitalize()+self.module_name.capitalize()+"类不存在")
                elif os.path.exists(self.module_path) and os.path.exists(self.user_module_path):
                    Log.logger.error(self.module_path.replace('.','/')+".py，"+self.user_module_path+"均不存在")
                else:
                    class_name=self.vendor.capitalize()+self.module_name.capitalize()
                    Log.logger.error(str(e))
                    Log.logger.error("self.module_path:"+self.module_path+"不存在")
                    Log.logger.error("self.user_module_path:"+self.user_module_path+"不存在")

                    Log.logger.error(f"请检查是否存在{class_name}相关文件，是否在包名错误、包未使用pip安装、包中有错误语法或引用等问题")
                    Log.logger.error(f"提示：可重点关注{self.user_module_path}，以及对应类名{class_name}")
                    print("Traceback:", file=sys.stderr)
                    traceback.print_tb(e.__traceback__)
                exit()
        
        
    def background(self,args):
        pass

        
    
    def run(self):
        klass=self.klass
        klass.args=self.args
        klass.run()


    def stop(self):
        if global_var.args.vendor!=None:
            pids_path=os.path.join(BASE_DIR, "data/cache/runtime", f"{global_var.module_name}_{global_var.args.vendor}.pids")
            process_name_pattern = f"{global_var.module_name}_{global_var.args.vendor}"
        else:
            pids_path=os.path.join(BASE_DIR, "data/cache/runtime", f"{global_var.module_name}.pids")
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
            traceback.print_exc()
        
        # 通过进程名兜底终止进程 - 优化匹配逻辑
        try:
            terminated_by_name = False
            module_pattern = global_var.module_name
            vendor_pattern = global_var.args.vendor if global_var.args.vendor else ""
            
            Log.logger.info(f"尝试通过进程名查找进程，模块名: {module_pattern}, 供应商: {vendor_pattern}")
            
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    # 检查进程名或命令行参数是否包含模块名和vendor
                    proc_info = proc.info
                    cmdline = proc_info.get('cmdline', [])
                    cmdline_str = " ".join(cmdline) if cmdline else ""
                    proc_name = proc_info.get('name', "")
                    
                    # 更精确的匹配逻辑
                    is_python = "python" in proc_name.lower() or "python" in cmdline_str.lower()
                    has_module = module_pattern in cmdline_str
                    has_vendor = vendor_pattern == "" or vendor_pattern in cmdline_str
                    has_finhack = "finhack" in cmdline_str
                    
                    # 调试信息
                    if is_python and has_finhack:
                        Log.logger.debug(f"发现Python进程: PID={proc.pid}, 命令行={cmdline_str}")
                    
                    if is_python and has_module and has_vendor and has_finhack:
                        Log.logger.info(f"找到匹配的进程: PID={proc.pid}, 命令行={cmdline_str}")
                        try:
                            # 终止子进程
                            children = proc.children(recursive=True)
                            for child in children:
                                try:
                                    child.terminate()
                                except (psutil.NoSuchProcess, psutil.AccessDenied):
                                    pass
                            
                            # 终止主进程
                            proc.terminate()
                            terminated_by_name = True
                            Log.logger.info(f"通过进程名终止进程: {proc.pid} ({proc_name})")
                            
                            # 等待进程终止
                            time.sleep(1)
                            if proc.is_running():
                                try:
                                    proc.kill()
                                except (psutil.NoSuchProcess, psutil.AccessDenied):
                                    pass
                        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
                            Log.logger.warning(f"尝试通过进程名终止进程 {proc.pid} 时出错: {str(e)}")
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
                except Exception as e:
                    Log.logger.warning(f"处理进程 {proc.pid} 时出错: {str(e)}")
                    continue
                    
            if not terminated_by_name:
                Log.logger.info("未通过进程名找到匹配的进程")
        except Exception as e:
            Log.logger.error(f"通过进程名兜底终止进程时出错: {str(e)}")
            traceback.print_exc()
        
        # 输出结果
        if fall_list == "" and (pid_terminated or terminated_by_name):
            Log.logger.info("停止任务成功！")
        elif fall_list != "":
            Log.logger.warning(f"部分进程停止失败！pid列表：{fall_list}")
        elif not pid_terminated and not terminated_by_name:
            Log.logger.info("未找到需要停止的进程")
            # 提供更多调试信息
            Log.logger.info(f"请确认是否有运行中的 finhack {module_pattern} 进程")
            Log.logger.info("可以尝试使用系统命令查看进程: ps aux | grep finhack")
    
            
            
            
            
            
            
            
            

