import configparser
import os
from runtime.constant import *

class Config:
    def get_config(config_name,sections='default'):
        file_name=config_name+".conf"
        file_path = CONFIG_DIR+file_name 
        config = configparser.ConfigParser()
        config.read(file_path)
        config=config.items(sections)
        config= {key: value for key, value in config for kv in config}
        return config
        
    def get_section_list(config_name):
        file_name=config_name+".conf"
        file_path = CONFIG_DIR+file_name 
        config = configparser.ConfigParser()
        config.read(file_path)
        return config.sections()