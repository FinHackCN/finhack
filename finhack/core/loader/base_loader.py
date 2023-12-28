import psutil
import time
import os
import traceback
from runtime.constant import *
import runtime.global_var as global_var
import finhack.library.log as Log
from finhack.library.class_loader import ClassLoader
import sys
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
                    Log.logger.error(f"请检查是否存在{class_name}相关文件，是否在包名错误、包未使用pip安装、包中有错误语法或引用等问题")
                    Log.logger.error(f"提示：可重点关注{self.user_module_path}，以及对应类名{class_name}")
                    print("Traceback:", file=sys.stderr)
                    traceback.print_tb(e.__traceback__)
                exit()
        
        
    def background(self,args):
        pass

        
    
    def run(self):
        klass=self.klass
        klass.run()

    
    def stop(self):
        if global_var.args.vendor!=None:
            pids_path=BASE_DIR+"/data/cache/runtime/"+global_var.module_name+"_"+global_var.args.vendor+".pids"
        else:
            pids_path=BASE_DIR+"/data/cache/runtime/"+global_var.module_name+".pids"
        fall_list=""
        with open(pids_path, "r") as f:
            # 逐行读取文件内容
            line = f.readline()
            while line:
                # 处理每一行的内容
                pid=int(line.strip())
                if psutil.pid_exists(pid):
                    try:
                        parent = psutil.Process(pid)
                        children = parent.children(recursive=True)
                        for child in children:
                            child.terminate()  # 终止子进程
                        parent.terminate()  # 终止父进程
                    except psutil.NoSuchProcess:
                        pass
                time.sleep(1)
                if psutil.pid_exists(pid):
                    fall_list=fall_list+str(pid)+"\n"
                # 读取下一行内容
                line = f.readline()
        with open(pids_path, "w") as f:
            f.write(fall_list)   
            
        if fall_list=="":
            Log.logger.info("停止任务成功！")
        else:
            Log.logger.warning("停止任务失败！pid列表："+fall_list)
            
            
            
            
            
            
            
            
            
            

