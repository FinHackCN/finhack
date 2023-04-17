# -*- coding: utf-8 -*-
from django.db.models import Q
from rest_framework import serializers

from dvadmin.system.models import Area
from dvadmin.utils.json_response import SuccessResponse
from dvadmin.utils.serializers import CustomModelSerializer
from dvadmin.utils.viewset import CustomModelViewSet


class AreaSerializer(CustomModelSerializer):
    """
    地区-序列化器
    """
    pcode_count = serializers.SerializerMethodField(read_only=True)

    def get_pcode_count(self, instance: Area):
        return Area.objects.filter(pcode=instance).count()

    class Meta:
        model = Area
        fields = "__all__"
        read_only_fields = ["id"]


class AreaCreateUpdateSerializer(CustomModelSerializer):
    """
    地区管理 创建/更新时的列化器
    """

    class Meta:
        model = Area
        fields = '__all__'


class AreaViewSet(CustomModelViewSet):
    """
    地区管理接口
    list:查询
    create:新增
    update:修改
    retrieve:单例
    destroy:删除
    """
    queryset = Area.objects.all()
    serializer_class = AreaSerializer
    extra_filter_backends = []
