# -*- coding: utf-8 -*-

"""
@author: 猿小天
@contact: QQ:1638245306
@Created on: 2021/6/6 006 10:30
@Remark: 自定义权限
"""
import re

from django.contrib.auth.models import AnonymousUser
from django.db.models import F
from rest_framework.permissions import BasePermission

from dvadmin.system.models import ApiWhiteList


def ValidationApi(reqApi, validApi):
    """
    验证当前用户是否有接口权限
    :param reqApi: 当前请求的接口
    :param validApi: 用于验证的接口
    :return: True或者False
    """
    if validApi is not None:
        valid_api = validApi.replace('{id}', '.*?')
        matchObj = re.match(valid_api, reqApi, re.M | re.I)
        if matchObj:
            return True
        else:
            return False
    else:
        return False


class AnonymousUserPermission(BasePermission):
    """
    匿名用户权限
    """

    def has_permission(self, request, view):
        if isinstance(request.user, AnonymousUser):
            return False
        return True


def ReUUID(api):
    """
    将接口的uuid替换掉
    :param api:
    :return:
    """
    pattern = re.compile(r'[a-f\d]{4}(?:[a-f\d]{4}-){4}[a-f\d]{12}/$')
    m = pattern.search(api)
    if m:
        res = api.replace(m.group(0), ".*/")
        return res
    else:
        return None


class CustomPermission(BasePermission):
    """自定义权限"""

    def has_permission(self, request, view):
        if isinstance(request.user, AnonymousUser):
            return False
        # 判断是否是超级管理员
        if request.user.is_superuser:
            return True
        else:
            api = request.path  # 当前请求接口
            method = request.method  # 当前请求方法
            methodList = ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'PATCH']
            method = methodList.index(method)
            # ***接口白名单***
            api_white_list = ApiWhiteList.objects.values(permission__api=F('url'), permission__method=F('method'))
            api_white_list = [
                str(item.get('permission__api').replace('{id}', '([a-zA-Z0-9-]+)')) + ":" + str(
                    item.get('permission__method')) + '$' for item in api_white_list if item.get('permission__api')]
            # ********#
            if not hasattr(request.user, "role"):
                return False
            userApiList = request.user.role.values('permission__api', 'permission__method')  # 获取当前用户的角色拥有的所有接口
            ApiList = [
                str(item.get('permission__api').replace('{id}', '([a-zA-Z0-9-]+)')) + ":" + str(
                    item.get('permission__method')) + '$' for item in userApiList if item.get('permission__api')]
            new_api_ist = api_white_list + ApiList
            new_api = api + ":" + str(method)
            for item in new_api_ist:
                matchObj = re.match(item, new_api, re.M | re.I)
                if matchObj is None:
                    continue
                else:
                    return True
            else:
                return False
