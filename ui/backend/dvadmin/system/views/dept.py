# -*- coding: utf-8 -*-

"""
@author: H0nGzA1
@contact: QQ:2505811377
@Remark: 部门管理
"""
from rest_framework import serializers
from rest_framework.decorators import action

from dvadmin.system.models import Dept
from dvadmin.utils.json_response import DetailResponse, SuccessResponse
from dvadmin.utils.permission import AnonymousUserPermission
from dvadmin.utils.serializers import CustomModelSerializer
from dvadmin.utils.viewset import CustomModelViewSet


class DeptSerializer(CustomModelSerializer):
    """
    部门-序列化器
    """
    parent_name = serializers.CharField(read_only=True, source='parent.name')
    status_label = serializers.SerializerMethodField()
    has_children = serializers.SerializerMethodField()
    hasChild = serializers.SerializerMethodField()

    def get_hasChild(self, instance):
        hasChild = Dept.objects.filter(parent=instance.id)
        if hasChild:
            return True
        return False

    def get_status_label(self, obj: Dept):
        if obj.status:
            return "启用"
        return "禁用"

    def get_has_children(self, obj: Dept):
        return Dept.objects.filter(parent_id=obj.id).count()

    class Meta:
        model = Dept
        fields = '__all__'
        read_only_fields = ["id"]


class DeptImportSerializer(CustomModelSerializer):
    """
    部门-导入-序列化器
    """

    class Meta:
        model = Dept
        fields = '__all__'
        read_only_fields = ["id"]


class DeptInitSerializer(CustomModelSerializer):
    """
    递归深度获取数信息(用于生成初始化json文件)
    """
    children = serializers.SerializerMethodField()

    def get_children(self, obj: Dept):
        data = []
        instance = Dept.objects.filter(parent_id=obj.id)
        if instance:
            serializer = DeptInitSerializer(instance=instance, many=True)
            data = serializer.data
        return data

    def save(self, **kwargs):
        instance = super().save(**kwargs)
        children = self.initial_data.get('children')
        if children:
            for menu_data in children:
                menu_data['parent'] = instance.id
                filter_data = {
                    "name": menu_data['name'],
                    "parent": menu_data['parent'],
                    "key": menu_data['key']
                }
                instance_obj = Dept.objects.filter(**filter_data).first()
                if instance_obj and not self.initial_data.get('reset'):
                    continue
                serializer = DeptInitSerializer(instance_obj, data=menu_data, request=self.request)
                serializer.is_valid(raise_exception=True)
                serializer.save()

        return instance

    class Meta:
        model = Dept
        fields = ['name', 'sort', 'owner', 'phone', 'email', 'status', 'parent', 'creator', 'dept_belong_id',
                  'children', 'key']
        extra_kwargs = {
            'creator': {'write_only': True},
            'dept_belong_id': {'write_only': True}
        }
        read_only_fields = ['id', 'children']


class DeptCreateUpdateSerializer(CustomModelSerializer):
    """
    部门管理 创建/更新时的列化器
    """

    def create(self, validated_data):
        value = validated_data.get('parent',None)
        if value is None:
            validated_data['parent'] = self.request.user.dept
        instance = super().create(validated_data)
        instance.dept_belong_id = instance.id
        instance.save()
        return instance

    class Meta:
        model = Dept
        fields = '__all__'


class DeptViewSet(CustomModelViewSet):
    """
    部门管理接口
    list:查询
    create:新增
    update:修改
    retrieve:单例
    destroy:删除
    """
    queryset = Dept.objects.all()
    serializer_class = DeptSerializer
    create_serializer_class = DeptCreateUpdateSerializer
    update_serializer_class = DeptCreateUpdateSerializer
    filter_fields = ['name', 'id', 'parent']
    search_fields = []
    # extra_filter_backends = []
    import_serializer_class = DeptImportSerializer
    import_field_dict = {
        "name": "部门名称",
        "key": "部门标识",
    }

    def list(self, request, *args, **kwargs):
        # 如果懒加载，则只返回父级
        params = request.query_params
        parent = params.get('parent', None)
        if params:
            if parent:
                queryset = self.queryset.filter(status=True, parent=parent)
            else:
                queryset = self.queryset.filter(status=True)
        else:
            queryset = self.queryset.filter(status=True, parent__isnull=True)
        queryset = self.filter_queryset(queryset)
        serializer = DeptSerializer(queryset, many=True, request=request)
        data = serializer.data
        return SuccessResponse(data=data)

    def dept_lazy_tree(self, request, *args, **kwargs):
        parent = self.request.query_params.get('parent')
        is_superuser = request.user.is_superuser
        if is_superuser:
            queryset = Dept.objects.values('id', 'name', 'parent')
        else:
            data_range = request.user.role.values_list('data_range', flat=True)
            user_dept_id = request.user.dept.id
            dept_list = [user_dept_id]
            data_range_list = list(set(data_range))
            for item in data_range_list:
                if item in [0,2]:
                    dept_list = [user_dept_id]
                elif item == 1:
                    dept_list = Dept.recursion_dept_info(dept_id=user_dept_id)
                elif item == 3:
                    dept_list = Dept.objects.values_list('id',flat=True)
                elif item == 4:
                    dept_list = request.user.role.values_list('dept',flat=True)
                else:
                    dept_list = []
            queryset = Dept.objects.filter(id__in=dept_list).values('id', 'name', 'parent')
        return DetailResponse(data=queryset, msg="获取成功")


    @action(methods=["GET"], detail=False, permission_classes=[AnonymousUserPermission])
    def all_dept(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())
        data = queryset.filter(status=True).order_by('sort').values('name', 'id', 'parent')
        return DetailResponse(data=data, msg="获取成功")
