from django.http import JsonResponse
import sys
import os
#from rest_framework.decorators import api_view, permission_classes
#from rest_framework.permissions import IsAuthenticated
finhack_path=os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
sys.path.append(finhack_path)
from running.running import running
from library.mydb import mydb

#@api_view(['GET'])
#@permission_classes([IsAuthenticated])
def signal(request):
    btid = request.GET.get('btid', '')
    date = request.GET.get('date', '')
    n = request.GET.get('n', '100')
    response = {'message': 'Hello, World!'}
    
    pred=running.pred_bt(btid,date)
    pred=pred.head(int(n))
    pred=pred['ts_code'].values.tolist()
    
    st_code=mydb.selectToDf('select ts_code from astock_basic where name like "%ST%"','tushare')
    st_code=st_code['ts_code'].values.tolist()
    
    result=[]
    for code in pred:
        if '688' in code[:3] or '300' in code[:3] or 'BJ' in code or code in st_code:
            continue
        else:
            result.append(code)
    
    
    return JsonResponse(result,safe=False)
 