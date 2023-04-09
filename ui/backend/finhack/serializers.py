from finhack.models import BacktestModel
from dvadmin.utils.serializers import CustomModelSerializer


class BacktestModelSerializer(CustomModelSerializer):
    """
    序列化器
    """

    class Meta:
        model = BacktestModel
        fields = "__all__"


class BacktestModelCreateUpdateSerializer(CustomModelSerializer):
    """
    创建/更新时的列化器
    """

    class Meta:
        model = BacktestModel
        fields = '__all__'