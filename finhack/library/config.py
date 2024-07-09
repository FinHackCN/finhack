import configparser
import os
from runtime.constant import *

class Config:
    def get_config(config_name,section='default'):
        file_name=config_name+".conf"
        file_path = CONFIG_DIR+file_name 
        config = configparser.ConfigParser()
        config.read(file_path)

        section_list=Config.get_section_list(config_name)
        if section not in section_list:
            return {}

        config=config.items(section)
        config= {key: value for key, value in config for kv in config}
        return config
        
    def get_section_list(config_name):
        file_name=config_name+".conf"
        file_path = CONFIG_DIR+file_name 
        config = configparser.ConfigParser()
        config.read(file_path)
        return config.sections()