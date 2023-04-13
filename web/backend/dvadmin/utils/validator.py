# -*- coding: utf-8 -*-

"""
@author: 猿小天
@contact: QQ:1638245306
@Created on: 2021/6/2 002 17:03
@Remark: 自定义验证器
"""

from django.db import DataError
from rest_framework.exceptions import APIException
from rest_framework.validators import UniqueValidator


class CustomValidationError(APIException):
    """
    继承并重写验证器返回的结果,避免暴露字段
    """

    def __init__(self, detail):
        self.detail = detail


def qs_exists(queryset):
    try:
        return queryset.exists()
    except (TypeError, ValueError, DataError):
        return False


def qs_filter(queryset, **kwargs):
    try:
        return queryset.filter(**kwargs)
    except (TypeError, ValueError, DataError):
        return queryset.none()


class CustomUniqueValidator(UniqueValidator):
    """
    继承,重写必填字段的验证器结果,防止字段暴露
    """

    def filter_queryset(self, value, queryset, field_name):
        """
        Filter the queryset to all instances matching the given attribute.
        """
        filter_kwargs = {'%s__%s' % (field_name, self.lookup): value}
        return qs_filter(queryset, **filter_kwargs)

    def exclude_current_instance(self, queryset, instance):
        """
        If an instance is being updated, then do not include
        that instance itself as a uniqueness conflict.
        """
        if instance is not None:
            return queryset.exclude(pk=instance.pk)
        return queryset

    def __call__(self, value, serializer_field):
        # Determine the underlying model field name. This may not be the
        # same as the serializer field name if `source=<>` is set.
        field_name = serializer_field.source_attrs[-1]
        # Determine the existing instance, if this is an update operation.
        instance = getattr(serializer_field.parent, 'instance', None)

        queryset = self.queryset
        queryset = self.filter_queryset(value, queryset, field_name)
        queryset = self.exclude_current_instance(queryset, instance)
        if qs_exists(queryset):
            raise CustomValidationError(self.message)

    def __repr__(self):
        return super().__repr__()
