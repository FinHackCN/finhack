import threading
import time
import finhack.library.log as Log

class collectThread(threading.Thread):
    def __init__(self, className, functionName, pro, db):
        threading.Thread.__init__(self)
        self.className = className
        self.functionName = functionName
        self.pro = pro
        self.db = db
        self.max_runtime = None  # 默认无限制
        
    def set_max_runtime(self, seconds):
        """设置线程最大运行时间（秒）"""
        self.max_runtime = seconds
        
    def run(self):
        start_time = time.time()
        
        # 创建一个守护线程来监控运行时间
        if self.max_runtime:
            def check_timeout():
                while True:
                    if time.time() - start_time > self.max_runtime:
                        Log.logger.warning(f"线程 {self.functionName} 已超过最大运行时间 {self.max_runtime/3600} 小时，强制终止")
                        # 在Python中无法直接终止线程，但可以设置一个标志
                        self.timeout = True
                        break
                    time.sleep(10)  # 每10秒检查一次
            
            self.timeout = False
            monitor = threading.Thread(target=check_timeout)
            monitor.daemon = True
            monitor.start()
        
        try:
            # 获取类中的方法并执行
            method = getattr(self.className, self.functionName)
            
            # 如果设置了超时，则定期检查是否已超时
            if self.max_runtime:
                # 这里需要修改原方法的实现，使其能够定期检查timeout标志
                # 由于我们无法直接修改原方法，建议在类中实现一个包装方法
                method_with_timeout = self.wrap_method_with_timeout(method)
                method_with_timeout(pro=self.pro, db=self.db)
            else:
                method(pro=self.pro, db=self.db)
                
            end_time = time.time()
            Log.logger.info(f"线程 {self.functionName} 完成，运行时间: {(end_time-start_time)/60:.2f} 分钟")
        except Exception as e:
            Log.logger.error(f"线程 {self.functionName} 执行出错: {str(e)}")
    
    def wrap_method_with_timeout(self, method):
        """包装原方法，使其能够定期检查是否超时"""
        def wrapped_method(*args, **kwargs):
            # 这里的实现取决于原方法的具体结构
            # 如果原方法是一个长时间运行的循环，可以在循环的每次迭代中检查timeout标志
            # 如果原方法是一个单次调用，则可能需要修改原方法的实现
            try:
                return method(*args, **kwargs)
            except Exception as e:
                if hasattr(self, 'timeout') and self.timeout:
                    Log.logger.warning(f"线程 {self.functionName} 因超时被终止")
                else:
                    raise e
        return wrapped_method
        
        


def start_thread_to_terminate_when_parent_process_dies(ppid):
    pid = os.getpid()

    def f():
        while True:
            try:
                os.kill(ppid, 0)
            except OSError:
                os.kill(pid, signal.SIGTERM)
            time.sleep(1)

    thread = threading.Thread(target=f, daemon=True)
    thread.start()


from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED
import time
 

