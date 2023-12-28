import finhack.library.log as Log
from finhack.core.loader.base_loader import BaseLoader
class TestmoduleLoader(BaseLoader):
    def testaction(self):
        Log.logger.debug("----%s----" % (__file__))
        Log.logger.debug("this is TestmoduleLoader")
        Log.logger.debug("loading "+self.module_name)
        Log.logger.debug("testarg1 is:"+str(self.args.testarg1))
        Log.logger.debug("testarg2 is:"+str(self.args.testarg2))
        obj=self.klass()
        obj.args=self.args
        obj.run()
        
    