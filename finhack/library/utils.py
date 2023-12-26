import os
import hashlib
import signal
class Utils:
    #获取目录中的文件列表
    def get_file_list(path):
        filelist=[]
        for subfile in os.listdir(path):
            if not '__' in subfile:
                filelist.append(subfile)
        return filelist
        
        
    #获取框架安装路径
    def get_framework_path():
        framework_path=os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        return framework_path
        
        
    #获取框架的模板路径
    def get_template_path():
        framework_path=Utils.get_framework_path()
        template_path=framework_path+"/widgets/templates/empty_project"
        return template_path
        
    
    #计算字符串的md5值
    def md5(s):
        m = hashlib.md5()
        m.update(s.encode('utf-8'))
        return m.hexdigest()
        
        
    def write_pids(pid="",module_name="",vendor_name=""):
        import runtime.global_var as global_var
        from runtime.constant import BASE_DIR
        if module_name=="":
            module_name=global_var.module_name
        if vendor_name=="":
            vendor_name=global_var.args.vendor
        if vendor_name==None:
            vendor_name="default"
            
        pids_path1=BASE_DIR+"/data/cache/runtime/"+module_name+".pids"
        with open(pids_path1, "a") as f:
            f.write(str(pid)+"\n")    
            
        pids_path2=BASE_DIR+"/data/cache/runtime/"+module_name+"_"+vendor_name+".pids"
        with open(pids_path2, "a") as f:
            f.write(str(pid)+"\n")  
        
    def auto_exit():
        pgid = os.getpgid(0)
        os.killpg(pgid, signal.SIGTERM)