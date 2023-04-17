from django.shortcuts import render


from finhack.models import BacktestModel
from finhack.serializers import BacktestModelSerializer, BacktestModelCreateUpdateSerializer
from dvadmin.utils.viewset import CustomModelViewSet


class BacktestModelViewSet(CustomModelViewSet):
    """
    list:查询
    create:新增
    update:修改
    retrieve:单例
    destroy:删除
    """
    queryset = BacktestModel.objects.all()
    serializer_class = BacktestModelSerializer
    create_serializer_class = BacktestModelCreateUpdateSerializer
    update_serializer_class = BacktestModelCreateUpdateSerializer
    filter_fields = ['strategy']
    search_fields = ['features_list']