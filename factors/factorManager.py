import os
import re
from library.mydb import mydb

class factorManager:
    def getFactorsList(valid=True):
        factorslist=[]
        path = os.path.dirname(__file__)+"/../data/single_factors"
        ignore=['close','vol','volume','open','low','high','pct_chg','amount','pre_close','vwap']
        for subfile in os.listdir(path):
            if not '__' in subfile and not subfile.replace('.csv','') in ignore:
                factorslist.append(subfile.replace('.csv',''))
                
        if valid==True:
            flist=mydb.selectToDf('select * from factors_list','finhack')
            for factor in factorslist:
                if 'alpha' in factor:
                    factor_name=factor
                    pass
                else:
                    factor_name=factor.split('_')[0]
                factor_df=flist[flist.factor_name==factor_name]
                check_type=factor_df['check_type'].values
                
                if len(check_type)==0 or check_type[0]!=11:
                    factorslist.remove(factor)
            
                
        return factorslist
    
    
    
    #获取alpha列表的列表
    def getAlphaLists():
        alphalists=[]
        path = os.path.dirname(__file__)+"/../lists/alphalist"
        for subfile in os.listdir(path):
            if not '__' in subfile:
                listname=subfile
                alphalists.append(subfile)
                
                

 
        return alphalists
    
    #根据alpha列表获取alpha
    def getAlphaList(listname):
        path = os.path.dirname(__file__)+"/../lists/alphalist/"+listname
        with open(path, 'r', encoding='utf-8') as f:
            return f.readlines()
            

    def getIndicatorsList():
        return_fileds=[]
        path = os.path.dirname(__file__)+"/indicators/"
        for subfile in os.listdir(path):
            if not '__' in subfile:
                indicators=subfile.split('.py')
                indicators=indicators[0]
                function_name=''
                code=''
                find=False
                with open(path+subfile) as filecontent:
                    for line in filecontent:
                        if(line.strip()[0:1]=='#'):
                            code=code+"\n"+line
                            continue
                        #提取当前函数名
                        if('def ' in line):
                            function_name=line.split('def ')
                            function_name=function_name[1]
                            function_name=function_name.split('(')
                            function_name=function_name[0]
                            function_name=function_name.strip()
                            code=line
                        else:
                            code=code+"\n"+line
                        left=line.split('=')
                        left=left[0]
                        
                        pattern = re.compile(r"df\[\'([A-Za-z0-9_\-]*?)\'\]")   # 查找数字
                        
                        flist = pattern.findall(left)
                        
                        for f in flist.copy():
                            #前缀是tmp_的都是临时因子，不管
                            if f[:4]=='tmp_':
                                flist.remove(f)
                        return_fileds=return_fileds+flist
                        
         
        path = os.path.dirname(__file__)+"/../lists/factorlist/"
        with open(path+'all','w') as file_object:
            file_object.write("\n".join(return_fileds))  
        
        #print("\n".join(return_fileds))  
        return return_fileds