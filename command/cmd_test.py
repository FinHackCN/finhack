import sys
import os
import time
import random
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from library.astock import AStock
from factors.indicatorCompute import indicatorCompute
from library.mydb import mydb
md5='d5fae5c77fa76ab243b329f40d303f50'
has=mydb.selectToDf('select * from auto_train where  hash="%s"' % (md5),'finhack')

print(not has.empty)

print(has)