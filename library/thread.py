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
        
        





from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED
import time
 

