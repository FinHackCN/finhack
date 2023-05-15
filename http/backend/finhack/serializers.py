from finhack.models import BacktestModel
from dvadmin.utils.serializers import CustomModelSerializer
from rest_framework import serializers

class BacktestModelListSerializer(CustomModelSerializer):
    """
    序列化器
    """
    p_max_down = serializers.SerializerMethodField()
    class Meta:
        model = BacktestModel
        fields = ["id",'init_cash','total_value','sharpe','strategy','max_down','p_max_down']
        
    def get_p_max_down(self, obj):
        # 在这里你可以做任何你想做的处理，比如四舍五入
        return str(round(obj.max_down*100, 2))+'%'



class BacktestModelInfoSerializer(CustomModelSerializer):
    """
    序列化器
    """
    p_max_down = serializers.SerializerMethodField()
    class Meta:
        model = BacktestModel
        fields = '__all__'
        
    def get_p_max_down(self, obj):
        # 在这里你可以做任何你想做的处理，比如四舍五入
        return str(round(obj.max_down*100, 2))+'%'


class BacktestModelCreateUpdateSerializer(CustomModelSerializer):
    """
    创建/更新时的列化器
    """

    class Meta:
        model = BacktestModel
        fields = '__all__'