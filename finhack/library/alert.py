import json
import datetime
import requests
from finhack.library.config import Config
class alert:
    def send(module,title,content):
        cfgAlert=Config.get_config('alert')
        title=cfgAlert['key']+module+":"+title
        if(cfgAlert['enable']=='True'):
            if 'api' in cfgAlert['dingtalk_webhook']:
                alert.sendDingTalk(cfgAlert['dingtalk_webhook'],title,content)
            if 'api' in cfgAlert['feishu_webhook']:
                alert.sendFeishu(cfgAlert['feishu_webhook'],title,content)
    
    
    def sendFeishu(url,title,content):
        data_json = json.dumps({
            "msg_type": "post",
            "content":
                {"post":
                     {"zh_cn":
                          {"title": title,
                           "content": [[
                               {"tag": "text",
                                "text": content
                                },
                           ]]}}}})
        r = requests.post(url, data_json)
        print(r)
    
    
    def sendDingTalk(url,title,content):
        msg=title+"\n"+content
        headers = {'Content-Type': 'application/json;charset=utf-8'}
        data = {
            "msgtype": "text", # 发送消息类型为文本
            "text": {
                "content": msg, # 消息正文
            }
        }
        r = requests.post(url, data=json.dumps(data), headers=headers)
        print(r)
