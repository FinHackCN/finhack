import os
from library.globalvar import *

class util:
    #获取目录中的文件列表
    def getFileList(path):
        filelist=[]
        for subfile in os.listdir(path):
            if not '__' in subfile:
                filelist.append(subfile)
        return filelist