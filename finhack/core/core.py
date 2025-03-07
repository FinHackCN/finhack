import os
import time
import shutil
import argparse
import importlib
import inspect
import finhack.library.log as Log
from finhack.library.utils import Utils
from finhack.library.class_loader import ClassLoader
from finhack.core.version import get_version
import sys


class Core:
    def __init__(self,project_path='',args=True):
        self.project_path=project_path
        print(project_path)
        self.usage=self.load_help_text()
        if args:
            self.generate_args()
            self.check_project()
        self.check_env()
        self.refresh_runtime()
        if args:
            self.append_args()
            self.generate_global()
            self.init_logger()
        else:
            sys.path.append(self.project_path)
            sys.path.append(self.project_path+'/data/cache/')


    def load_help_text(self):
        """从配置文件加载帮助文本"""
        # 首先尝试从项目路径读取
        help_file_path = os.path.join(self.project_path, 'data', 'config', 'help.conf')
        
        # 如果项目路径下不存在help.conf，尝试从模板目录读取
        if not self.project_path or not os.path.exists(help_file_path):
            template_path = Utils.get_template_path()
            help_file_path = os.path.join(template_path, 'data', 'config', 'help.conf')
            
            # 如果模板目录下也不存在help.conf，返回空字符串
            if not os.path.exists(help_file_path):
                return ""
        
        try:
            with open(help_file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            print(f"读取帮助文件失败: {e}")
            # 读取失败时返回空字符串
            return ""

    def init_logger(self):
        from runtime.constant import LOGS_DIR
        vendor="default" if self.args.vendor is None else self.args.vendor
        action="run" if self.args.action is None else self.args.action
        Log.logger=Log.Log(module=self.args.module,vendor=vendor,action=action,logs_dir=LOGS_DIR,background=self.args.background).logger
        
    #追加配置文件中的参数
    def append_args(self):
        sys.path.append(self.project_path)
        sys.path.append(self.project_path+'/data/cache/')
        from finhack.library.config import Config 

        
        #待添加的args列表
        args_list={

        }

        #读取args文件下所有分区
        my_args_group_list=Config.get_section_list('args')
        args, unknown = self.parser.parse_known_args()

        #[global]
        for my_args_group in my_args_group_list:
            if my_args_group=='global':
                my_args_list=Config.get_config('args',my_args_group)
                for arg,default in my_args_list.items():
                    args_list[arg]=default

        vendor="default" if args.vendor is None else args.vendor
        #[model]
        for my_args_group in my_args_group_list:
            if my_args_group==args.module:
                my_args_list=Config.get_config('args',my_args_group)
                for arg,default in my_args_list.items():
                    args_list[arg]=default

        #[model-vendor]
        for my_args_group in my_args_group_list:
            if my_args_group==args.module+'-'+vendor:
                my_args_list=Config.get_config('args',my_args_group)
                for arg,default in my_args_list.items():
                    args_list[arg]=default

        #[model-vendor-action]
        for my_args_group in my_args_group_list:
            if my_args_group==args.module+'-'+vendor+'-'+args.action:
                my_args_list=Config.get_config('args',my_args_group)
                for arg,default in my_args_list.items():
                    args_list[arg]=default

        #model.conf [args]
        my_args_list=Config.get_config(args.module,'args')
        for arg,default in my_args_list.items():
            args_list[arg]=default


        #model.conf [args.section]
        if args.section!=None and args.section!='':
            my_args_list=Config.get_config(args.module,args.section)
            for arg,default in my_args_list.items():
                args_list[arg]=default


        for arg,default in args_list.items():
                group = self.parser.add_argument_group(my_args_group)
                group.add_argument('--'+arg,metavar='', default=default)
        args=self.parse_args()
        self.args=args
        
        import runtime.global_var as global_var 
        global_var.args=args
        
        
    def generate_global(self):
        import runtime.global_var as global_var 
        global_var.pid = os.getpid()
        arg_vars=vars(global_var.args).copy()
        arg_vars['background']=None
        hash=Utils.md5(str(arg_vars))
        global_var.hash=hash
        global_var.module_name=global_var.args.module
        global_var.action_name=global_var.args.action
        

        
        
    def refresh_runtime(self):
        project_path=self.project_path
        constant=''
        with open(project_path+"/data/config/constant.conf", 'r') as f:
            constant = f.read()
            constant=constant.replace('{BASE_DIR}','"'+project_path+'"')
            constant=constant.replace('{FRAMEWORK_DIR}','"'+Utils.get_framework_path()+'"')
        with open(project_path+"/data/cache/runtime/constant.py", 'w') as f:
            f.write(constant)
        
        global_var=''
        with open(project_path+"/data/config/global_var.conf", 'r') as f:
            global_var = f.read()
            global_var=global_var.replace('{BASE_DIR}','"'+project_path+'"')
            global_var=global_var.replace('{FRAMEWORK_DIR}','"'+Utils.get_framework_path()+'"')
        with open(project_path+"/data/cache/runtime/global_var.py", 'w') as f:
            f.write(global_var)               
        
    
    def check_project(self):
        args, unknown = self.parser.parse_known_args()
        module=args.module
        action=args.action
        if module=="project":
            project_path=args.project_path
            if project_path==None:
                project_path=os.getcwd()+"/project_"+str(int(time.time()))
                self.project_path=project_path

            template_path=Utils.get_template_path()
            if action=="create":
                shutil.copytree(template_path, project_path)
                if os.path.exists(project_path):
                    print("创建完毕！项目路径为 %s 请切换到该目录或使用-p参数指定目录并执行相应操作！" % (project_path).replace('//','/'))
                else:
                    print("创建失败！")
                exit()
            elif action=="renew":
                #todo
                pass

    
    def check_env(self):
        if self.project_path!='':
            return True
        args, unknown = self.parser.parse_known_args() 
        project_path=args.project_path
        if project_path==None:
            project_path=os.getcwd()
        project_file=project_path+"/.proj"
        if not os.path.exists(project_file):
            print("当前目录非项目目录，请使用-p参数指定项目路径，或使用create命令创建项目")
        self.project_path=project_path
    
    
    #生成参数
    def generate_args(self):
        parser = argparse.ArgumentParser(description='',usage=self.usage)
        parser.add_argument('module', help='需要调用的模块')
        parser.add_argument('action', help='需要执行的动作')
        parser.add_argument("--background",  default=False, action='store_true', help="是否在后台运行")
        parser.add_argument('--project_path',metavar='', help='项目路径')
        parser.add_argument("--vendor",  metavar='',  help="模块的供给侧")
        parser.add_argument("--section",  metavar='',  help="配置文件section")
        self.parser=parser
        return parser
        
    
    #生成参数
    def parse_args(self):
        args, unknown = self.parser.parse_known_args() 
        self.args=args
        return args
        
        
    def load_module(self):
        module=self.args.module
        module_loader_path="finhack.core.loader."+self.args.module+"_loader"
        class_name=module.capitalize()+"Loader"
        action=self.args.action
        user_module_path=self.project_path+'/loader/'+self.args.module+"_loader.py"
        module = ClassLoader.get_module(module_path=module_loader_path,user_module_path=user_module_path)
        self.module=module
        
        if "base_loader.py" == os.path.basename(inspect.getfile(module)):
            self.class_name='BaseLoader'
        else:
            self.class_name=class_name
        self.action=action
        
    def do_action(self):    
        loader = getattr(self.module, self.class_name)
        loader_obj = loader(self.args)


        if hasattr(loader_obj, self.action):
            method = getattr(loader_obj, self.action)
        else:
            klass=loader_obj.klass
            klass.args=self.args
            method = getattr(klass, self.action)
            
        method()  