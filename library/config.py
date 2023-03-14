import configparser
import os
from library.globalvar import *

class config:
    @staticmethod
    def getConfig(configName,sections='default'):
        filename=configName+".conf"
        filepath = CONFIG_DIR+filename 
        config = configparser.ConfigParser()
        config.read(filepath)
        config=config.items(sections)
        config= {key: value for key, value in config for kv in config}
        return config
        
        
    @staticmethod
    def getSectionList(configName):
        filename=configName+".conf"
        filepath = CONFIG_DIR+filename 
        config = configparser.ConfigParser()
        config.read(filepath)
        return config.sections()