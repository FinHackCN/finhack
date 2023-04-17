# -*- coding: utf-8 -*-
import json

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django_restql.fields import DynamicSerializerMethodField
from rest_framework import serializers
from rest_framework.decorators import action, permission_classes
from rest_framework.permissions import IsAuthenticated, AllowAny

from dvadmin.system.models import MessageCenter, Users, MessageCenterTargetUser
from dvadmin.utils.json_response import SuccessResponse, DetailResponse
from dvadmin.utils.serializers import CustomModelSerializer
from dvadmin.utils.viewset import CustomModelViewSet


class MessageCenterSerializer(CustomModelSerializer):
    """
    消息中心-序列化器
    """
    role_info = DynamicSerializerMethodField()
    user_info = DynamicSerializerMethodField()
    dept_info = DynamicSerializerMethodField()
    is_read = serializers.BooleanField(read_only=True, source='target_user__is_read')

    def get_role_info(self, instance, parsed_query):
        roles = instance.target_role.all()
        # You can do what ever you want in here
        # `parsed_query` param is passed to BookSerializer to allow further querying
        from dvadmin.system.views.role import RoleSerializer
        serializer = RoleSerializer(
            roles,
            many=True,
            parsed_query=parsed_query
        )
        return serializer.data

    def get_user_info(self, instance, parsed_query):
        users = instance.target_user.all()
        # You can do what ever you want in here
        # `parsed_query` param is passed to BookSerializer to allow further querying
        from dvadmin.system.views.user import UserSerializer
        serializer = UserSerializer(
            users,
            many=True,
            parsed_query=parsed_query
        )
        return serializer.data

    def get_dept_info(self, instance, parsed_query):
        dept = instance.target_dept.all()
        # You can do what ever you want in here
        # `parsed_query` param is passed to BookSerializer to allow further querying
        from dvadmin.system.views.dept import DeptSerializer
        serializer = DeptSerializer(
            dept,
            many=True,
            parsed_query=parsed_query
        )
        return serializer.data

    class Meta:
        model = MessageCenter
        fields = "__all__"
        read_only_fields = ["id"]


class MessageCenterTargetUserSerializer(CustomModelSerializer):
    """
    目标用户序列化器-序列化器
    """

    class Meta:
        model = MessageCenterTargetUser
        fields = "__all__"
        read_only_fields = ["id"]


class MessageCenterTargetUserListSerializer(CustomModelSerializer):
    """
    目标用户序列化器-序列化器
    """
    is_read = serializers.SerializerMethodField()

    def get_is_read(self, instance):
        user_id = self.request.user.id
        message_center_id = instance.id
        queryset = MessageCenterTargetUser.objects.filter(messagecenter__id=message_center_id,users_id=user_id).first()
        return queryset.is_read

    class Meta:
        model = MessageCenter
        fields = "__all__"
        read_only_fields = ["id"]

def websocket_push(user_id, message):
    """
    主动推送消息
    """
    username = "user_"+str(user_id)
    print(103,message)
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
    username,
    {
      "type": "push.message",
      "json": message
    }
    )

class MessageCenterCreateSerializer(CustomModelSerializer):
    """
    消息中心-新增-序列化器
    """

    def save(self, **kwargs):
        data = super().save(**kwargs)
        initial_data = self.initial_data
        target_type = initial_data.get('target_type')
        # 在保存之前,根据目标类型,把目标用户查询出来并保存
        users = initial_data.get('target_user', [])
        if target_type in [1]:  # 按角色
            target_role = initial_data.get('target_role',[])
            users = Users.objects.exclude(is_deleted=True).filter(role__id__in=target_role).values_list('id', flat=True)
        if target_type in [2]:  # 按部门
            target_dept = initial_data.get('target_dept',[])
            users = Users.objects.exclude(is_deleted=True).filter(dept__id__in=target_dept).values_list('id', flat=True)
        if target_type in [3]:  # 系统通知
            users = Users.objects.exclude(is_deleted=True).values_list('id', flat=True)
        targetuser_data = []
        for user in users:
            targetuser_data.append({
                "messagecenter": data.id,
                "users": user
            })
        targetuser_instance = MessageCenterTargetUserSerializer(data=targetuser_data, many=True, request=self.request)
        targetuser_instance.is_valid(raise_exception=True)
        targetuser_instance.save()
        for user in users:
            unread_count = MessageCenterTargetUser.objects.filter(users__id=user, is_read=False).count()
            websocket_push(user, message={"sender": 'system', "contentType": 'SYSTEM',
                                  "content": '您有一条新消息~', "unread": unread_count})
        return data

    class Meta:
        model = MessageCenter
        fields = "__all__"
        read_only_fields = ["id"]


class MessageCenterViewSet(CustomModelViewSet):
    """
    消息中心接口
    list:查询
    create:新增
    update:修改
    retrieve:单例
    destroy:删除
    """
    queryset = MessageCenter.objects.order_by('create_datetime')
    serializer_class = MessageCenterSerializer
    create_serializer_class = MessageCenterCreateSerializer
    extra_filter_backends = []

    def get_queryset(self):
        if self.action == 'list':
            return MessageCenter.objects.filter(creator=self.request.user.id).all()
        return MessageCenter.objects.all()

    def retrieve(self, request, *args, **kwargs):
        """
        重写查看
        """
        pk = kwargs.get('pk')
        user_id = self.request.user.id
        queryset = MessageCenterTargetUser.objects.filter(users__id=user_id, messagecenter__id=pk).first()
        if queryset:
            queryset.is_read = True
            queryset.save()
        instance = self.get_object()
        serializer = self.get_serializer(instance)
        # 主动推送消息
        unread_count = MessageCenterTargetUser.objects.filter(users__id=user_id, is_read=False).count()
        websocket_push(user_id, message={"sender": 'system', "contentType": 'TEXT',
                                 "content": '您查看了一条消息~', "unread": unread_count})
        return DetailResponse(data=serializer.data, msg="获取成功")

    @action(methods=['GET'], detail=False, permission_classes=[IsAuthenticated])
    def get_self_receive(self, request):
        """
        获取接收到的消息
        """
        self_user_id = self.request.user.id
        # queryset = MessageCenterTargetUser.objects.filter(users__id=self_user_id).order_by('-create_datetime')
        queryset = MessageCenter.objects.filter(target_user__id=self_user_id)
        print(queryset)
        # queryset = self.filter_queryset(queryset)
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = MessageCenterTargetUserListSerializer(page, many=True, request=request)
            return self.get_paginated_response(serializer.data)
        serializer = MessageCenterTargetUserListSerializer(queryset, many=True, request=request)
        return SuccessResponse(data=serializer.data, msg="获取成功")

    @action(methods=['GET'], detail=False, permission_classes=[IsAuthenticated])
    def get_newest_msg(self, request):
        """
        获取最新的一条消息
        """
        self_user_id = self.request.user.id
        queryset = MessageCenterTargetUser.objects.filter(users__id=self_user_id).order_by('create_datetime').last()
        data = None
        if queryset:
            serializer = MessageCenterTargetUserListSerializer(queryset, many=False, request=request)
            data = serializer.data
        return DetailResponse(data=data, msg="获取成功")
