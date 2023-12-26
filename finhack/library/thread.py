import threading
class collectThread (threading.Thread):
    def __init__(self,className,functionName,pro,db):
        threading.Thread.__init__(self)
        self.className=className
        self.functionName=functionName
        self.pro=pro
        self.db=db
    def run(self):
        f = getattr(self.className, self.functionName)
        f(self.pro,self.db)
        
        


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
 

