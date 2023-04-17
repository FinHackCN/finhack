# -*- coding: utf-8 -*-

"""
@author: 猿小天
@contact: QQ:1638245306
@Created on: 2021/8/12 012 10:25
@Remark: swagger配置
"""
from drf_yasg.generators import OpenAPISchemaGenerator
from drf_yasg.inspectors import SwaggerAutoSchema

from application.settings import SWAGGER_SETTINGS


def get_summary(string):
    if string is not None:
        result = string.strip().replace(" ","").split("\n")
        return result[0]

class CustomSwaggerAutoSchema(SwaggerAutoSchema):
    def get_tags(self, operation_keys=None):
        tags = super().get_tags(operation_keys)
        if "api" in tags and operation_keys:
            #  `operation_keys` 内容像这样 ['v1', 'prize_join_log', 'create']
            tags[0] = operation_keys[SWAGGER_SETTINGS.get('AUTO_SCHEMA_TYPE', 2)]
        return tags

    def get_summary_and_description(self):
        summary_and_description = super().get_summary_and_description()
        summary = get_summary(self.__dict__.get('view').__doc__)
        description = summary_and_description[1]
        return summary,description


class CustomOpenAPISchemaGenerator(OpenAPISchemaGenerator):
    def get_schema(self, request=None, public=False):
        """Generate a :class:`.Swagger` object with custom tags"""

        swagger = super().get_schema(request, public)
        swagger.tags = [
            {
                "name": "token",
                "description": "认证相关"
            },
        ]
        return swagger
