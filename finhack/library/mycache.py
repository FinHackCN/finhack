from finhack.library.config import Config
from runtime.constant import *
import pickle
import os
import redis

class FileCache:
    def __init__(self, cache_dir=None):
        if cache_dir==None:
            cache_dir=KV_CACHE_DIR
        self.cache_dir = cache_dir
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
    
    def _get_file_path(self, key):
        safe_key = str(key).replace('/', '_').replace('..', '_')
        return os.path.join(self.cache_dir, safe_key)
    
    def set(self, key, value, expire=None):
        file_path = self._get_file_path(key)
        data_to_store = {
            'expire_at': time.time() + expire if expire else None,
            'value': value
        }
        with open(file_path, 'wb') as f:
            pickle.dump(data_to_store, f)
        return True
    
    def get(self, key):
        file_path = self._get_file_path(key)
        if not os.path.exists(file_path):
            return None
        
        with open(file_path, 'rb') as f:
            data_stored = pickle.load(f)
        
        # 检查是否过期
        if data_stored['expire_at'] is not None and data_stored['expire_at'] < time.time():
            os.remove(file_path)  # 删除过期文件
            return None
        
        return data_stored['value']
        
class RedisCache:
    def __init__(self):
        cfg=Config.get_config('db','redis')
        redisPool = redis.ConnectionPool(host=cfg['host'],port=int(cfg['port']),password=cfg['password'],db=int(cfg['db']))
        self.client = redis.Redis(connection_pool=redisPool)    

    def set(self,key,value,expire=0):
        serialized_value = pickle.dumps(value)
        if expire>0:
            self.client.set(key,serialized_value,ex=expire)
        else:
            self.client.set(key,serialized_value)
        return True
    
    def get(self,key):
        serialized_value=self.client.get(key)
        if serialized_value is None:
            return None
        # 使用pickle反序列化数据
        return pickle.loads(serialized_value)    
    
    
cfgCache=Config.get_config('core','cache')
ctype=cfgCache['type']
mycache=None
if ctype=='file':
    mycache=FileCache()
else:
    mycache=RedisCache()