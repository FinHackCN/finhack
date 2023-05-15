from django.shortcuts import render


from finhack.models import BacktestModel
from finhack.serializers import *
from dvadmin.utils.viewset import CustomModelViewSet
from dvadmin.utils.json_response import DetailResponse, SuccessResponse





class BacktestModelViewSet(CustomModelViewSet):
    """
    list:查询
    create:新增
    update:修改
    retrieve:单例
    destroy:删除
    """
    queryset = BacktestModel.objects.all()
    #serializer_class = BacktestModelSerializer
    
    def get_serializer_class(self):
        if self.action == 'list':
            return BacktestModelListSerializer
        else:
            return BacktestModelInfoSerializer
    
    create_serializer_class = BacktestModelCreateUpdateSerializer
    update_serializer_class = BacktestModelCreateUpdateSerializer
    #filter_fields = ['strategy','returns','benchreturns']
    search_fields = ['strategy']
    
    
    def list(self, request, *args, **kwargs):
        # 如果懒加载，则只返回父级
        order=''
        if request.GET.get('orderAsc','0')=='0':
            order='-'
        queryset = self.filter_queryset(self.get_queryset()).order_by(order+'sharpe').all()
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True, request=request)
            return self.get_paginated_response(serializer.data)
        serializer = self.get_serializer(queryset, many=True, request=request)
        return SuccessResponse(data=serializer.data, msg="获取成功")
        
