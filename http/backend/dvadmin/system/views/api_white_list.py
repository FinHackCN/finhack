# -*- coding: utf-8 -*-

"""
@author: 猿小天
@contact: QQ:1638245306
@Created on: 2022/1/1 001 9:34
@Remark:
"""
from dvadmin.system.models import ApiWhiteList
from dvadmin.utils.serializers import CustomModelSerializer
from dvadmin.utils.viewset import CustomModelViewSet


class ApiWhiteListSerializer(CustomModelSerializer):
    """
    接口白名单-序列化器
    """

    class Meta:
        model = ApiWhiteList
        fields = "__all__"
        read_only_fields = ["id"]


class ApiWhiteListInitSerializer(CustomModelSerializer):
    """
    初始化获取数信息(用于生成初始化json文件)
    """

    class Meta:
        model = ApiWhiteList
        fields = ['url', 'method', 'enable_datasource', 'creator', 'dept_belong_id']
        read_only_fields = ["id"]
        extra_kwargs = {
            'creator': {'write_only': True},
            'dept_belong_id': {'write_only': True}
        }


class ApiWhiteListViewSet(CustomModelViewSet):
    """
    接口白名单
    list:查询
    create:新增
    update:修改
    retrieve:单例
    destroy:删除
    """
    queryset = ApiWhiteList.objects.all()
    serializer_class = ApiWhiteListSerializer
    # permission_classes = []
