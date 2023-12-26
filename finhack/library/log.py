import sys
from loguru import logger
class Log():
    def __init__(self,module,logs_dir="",background=False):

        log_path=logs_dir+module+'.log'
        
        #print(log_path)

        logger.remove(handler_id=None) 
        fmt = "{time} - {name} - {level} - {message}"
        logger.add(log_path, level="DEBUG", format=fmt, retention='7 days', filter=lambda record: record["extra"].get("name") == "core")
        
        if background==False:
            logger.add(sys.stderr, level="DEBUG", format=fmt, filter=lambda record: record["extra"].get("name") == "core")
        self.logger=logger.bind(name="core")
    
class tLog():
    def __init__(self,id,logs_dir="",background=False,level='INFO'):
        log_path=logs_dir+"/trader/"+id+'.log'
        #logger.remove(handler_id=None) 
        fmt = "{message}"
        logger.add(log_path, level=level, format=fmt, filter=lambda record: record["extra"].get("trader") == "core")
        
        if background==False:
            logger.add(sys.stderr, level=level, format=fmt, filter=lambda record: record["extra"].get("name") == "trader")
        self.logger=logger.bind(name="trader")       

