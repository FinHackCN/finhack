import time
from library.alert import alert
import traceback
class tsMonitor:
    def __init__(self, func):
        self.func = func
 
    def __get__(self, instance, owner):
        def wrapper(*args, **kwargs):
            res=False
            try_times=0
            while True:
                try:
                    res=self.func(*args,**kwargs)
                    break
                except Exception as e:
                    if "每分钟最多访问" in str(e):
                        print(self.func.__name__+":触发限流，等待重试。\n"+str(e))
                        time.sleep(15)
                        continue
                    
                    elif "每天最多访问" in str(e) or "每小时最多访问" in str(e):
                        print(self.func.__name__+":今日权限用完。\n"+str(e))
                        break
                        continue
                   
                    elif "您没有访问该接口的权限" in str(e):
                        print(self.func.__name__+":没有访问该接口的权限。\n"+str(e))
                        break
                
                    else:
                        if try_times<10:
                            try_times=try_times+1;
                            print(self.func.__name__+":未知异常，等待重试。\n"+str(e))
                            time.sleep(15)
                            continue
                        else:
                            info = traceback.format_exc()
                            print(self.func.__name__+":同步异常，"+str(info))
                            alert.send(self.func.__name__,'同步异常',str(info))
                            break
            print(self.func.__name__+":同步完毕")
            return res
        return wrapper
        
        
class dbMonitor:
    def __init__(self, func):
        self.func = func
 
    def __get__(self, instance, owner):
        def wrapper(*args, **kwargs):
            res=False
            try_times=0
            while True:
                try:
                    res=self.func(*args,**kwargs)
                    break
                except Exception as e:
                    if "exist" in str(e) or 'Duplicate' in str(e):
                        print("INFO:"+str(e)) 
                        # print(str(*args))
                        return False
                        
                    if "connect" in str(e)  and try_times<10:
                        try_times=try_times+1;
                        print(self.func.__name__+":mysql连接异常，等待重试。\n"+str(e))
                        print(*args)
                        print(**kwargs)
                        time.sleep(1)
                        continue
                    else:
                        info = traceback.format_exc()
                        print(args)
                        print(self.func.__name__+":mysql异常，"+str(info))
                        alert.send(self.func.__name__,'mysql异常',str(info))
                        break
            return res
        return wrapper