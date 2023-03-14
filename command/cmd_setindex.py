import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from library.config import config
from library.mydb import mydb
from collect.ts.helper import tsSHelper

cfgTS=config.getConfig('ts')
db=cfgTS['db']

tables_list=mydb.selectToList('show tables',db)
for v in tables_list:
    table=list(v.values())[0]
    tsSHelper.setIndex(table,db)

 
