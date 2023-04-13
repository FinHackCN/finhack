# 初始化基类
import json
import os

from django.apps import apps
from rest_framework import request

from application import settings
from dvadmin.system.models import Users


class CoreInitialize:
    """
    使用方法：继承此类，重写 run方法，在 run 中调用 save 进行数据初始化
    """
    creator_id = None
    reset = False
    request = request
    file_path = None

    def __init__(self, reset=False, creator_id=None, app=None):
        """
        reset: 是否重置初始化数据
        creator_id: 创建人id
        """
        self.reset = reset or self.reset
        self.creator_id = creator_id or self.creator_id
        self.app = app or ''
        self.request.user = Users.objects.order_by('create_datetime').first()

    def init_base(self, Serializer, unique_fields=None):
        model = Serializer.Meta.model
        path_file = os.path.join(apps.get_app_config(self.app.split('.')[-1]).path, 'fixtures',
                                 f'init_{Serializer.Meta.model._meta.model_name}.json')
        if not os.path.isfile(path_file):
            return
        with open(path_file,encoding="utf-8") as f:
            for data in json.load(f):
                filter_data = {}
                # 配置过滤条件,如果有唯一标识字段则使用唯一标识字段，否则使用全部字段
                if unique_fields:
                    for field in unique_fields:
                        if field in data:
                            filter_data[field] = data[field]
                else:
                    for key, value in data.items():
                        if isinstance(value, list) or value == None or value == '':
                            continue
                        filter_data[key] = value
                instance = model.objects.filter(**filter_data).first()
                data["reset"] = self.reset
                serializer = Serializer(instance, data=data, request=self.request)
                serializer.is_valid(raise_exception=True)
                serializer.save()
        print(f"[{self.app}][{model._meta.model_name}]初始化完成")

    def save(self, obj, data: list, name=None, no_reset=False):
        name = name or obj._meta.verbose_name
        print(f"正在初始化[{obj._meta.label} => {name}]")
        if not no_reset and self.reset and obj not in settings.INITIALIZE_RESET_LIST:
            try:
                obj.objects.all().delete()
                settings.INITIALIZE_RESET_LIST.append(obj)
            except Exception:
                pass
        for ele in data:
            m2m_dict = {}
            new_data = {}
            for key, value in ele.items():
                # 判断传的 value 为 list 的多对多进行抽离，使用set 进行更新
                if isinstance(value, list) and value and isinstance(value[0], int):
                    m2m_dict[key] = value
                else:
                    new_data[key] = value
            object, _ = obj.objects.get_or_create(id=ele.get("id"), defaults=new_data)
            for key, m2m in m2m_dict.items():
                m2m = list(set(m2m))
                if m2m and len(m2m) > 0 and m2m[0]:
                    exec(f"""
if object.{key}:
    values_list = object.{key}.all().values_list('id', flat=True)
    values_list = list(set(list(values_list) + {m2m}))
    object.{key}.set(values_list)
""")
        print(f"初始化完成[{obj._meta.label} => {name}]")

    def run(self):
        raise NotImplementedError('.run() must be overridden')
