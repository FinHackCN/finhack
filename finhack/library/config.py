import configparser
import os

# 尝试从正确位置导入常量，如果失败则使用默认路径
try:
    from runtime.constant import CONFIG_DIR
except ImportError:
    try:
        # 尝试从demo_project导入
        import sys
        sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../demo_project/data/cache')))
        from runtime.constant import CONFIG_DIR
    except ImportError:
        # 如果都失败，使用默认路径
        CONFIG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../demo_project/data/config/'))
        if not CONFIG_DIR.endswith('/'):
            CONFIG_DIR += '/'

class Config:
    @staticmethod
    def get_config(config_name, section='default'):
        file_name = config_name + ".conf"
        file_path = CONFIG_DIR + file_name 
        config = configparser.ConfigParser()
        config.read(file_path)

        section_list = Config.get_section_list(config_name)
        if section not in section_list:
            return {}

        config_items = config.items(section)
        config_dict = {key: value for key, value in config_items}
        return config_dict
        
    @staticmethod
    def get_section_list(config_name):
        file_name = config_name + ".conf"
        file_path = CONFIG_DIR + file_name 
        config = configparser.ConfigParser()
        config.read(file_path)
        return config.sections()