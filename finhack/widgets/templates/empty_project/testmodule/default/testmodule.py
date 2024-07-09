import finhack.library.log as Log
from runtime.constant import *
import runtime.global_var as global_var

import time
class Testmodule():
    def run(self):
        Log.logger.debug("----%s----" % (__file__))
        Log.logger.debug("this is Testmodule")
        Log.logger.debug("vendor is default")
        while True:
            time.sleep(1)
            Log.logger.debug(".")
        
        
