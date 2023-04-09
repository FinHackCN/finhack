# python manage.py inspectdb --database finhack >./model.py
# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class AutoTrainModel(models.Model):
    start_date = models.CharField(max_length=10, blank=True, null=True)
    valid_date = models.CharField(max_length=10, blank=True, null=True)
    end_date = models.CharField(max_length=10, blank=True, null=True)
    features = models.TextField(blank=True, null=True)
    label = models.CharField(max_length=255, blank=True, null=True)
    shift = models.IntegerField(blank=True, null=True)
    param = models.TextField(blank=True, null=True)
    hash = models.CharField(max_length=32, blank=True, null=True)
    created_at = models.DateTimeField(blank=True, null=True)
    loss = models.CharField(max_length=255, blank=True, null=True)
    algorithm = models.CharField(max_length=255, blank=True, null=True)
    filter = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'auto_train'


class BacktestModel(models.Model):
    instance_id = models.CharField(unique=True, max_length=32)
    features_list = models.TextField(blank=True, null=True)
    train = models.CharField(max_length=255, blank=True, null=True)
    model = models.CharField(max_length=255, blank=True, null=True)
    strategy = models.CharField(max_length=255, blank=True, null=True)
    start_date = models.CharField(max_length=10, blank=True, null=True)
    end_date = models.CharField(max_length=10, blank=True, null=True)
    init_cash = models.FloatField(blank=True, null=True)
    args = models.TextField(blank=True, null=True)
    history = models.TextField(blank=True, null=True)
    returns = models.TextField(blank=True, null=True)
    logs = models.TextField(blank=True, null=True)
    total_value = models.FloatField(blank=True, null=True)
    alpha = models.FloatField(blank=True, null=True)
    beta = models.FloatField(blank=True, null=True)
    annual_return = models.FloatField(blank=True, null=True)
    cagr = models.FloatField(blank=True, null=True)
    annual_volatility = models.FloatField(blank=True, null=True)
    info_ratio = models.FloatField(blank=True, null=True)
    downside_risk = models.FloatField(blank=True, null=True)
    r2 = models.FloatField(db_column='R2', blank=True, null=True)  # Field name made lowercase.
    sharpe = models.FloatField(blank=True, null=True)
    sortino = models.FloatField(blank=True, null=True)
    calmar = models.FloatField(blank=True, null=True)
    omega = models.FloatField(blank=True, null=True)
    max_down = models.FloatField(blank=True, null=True)
    sqn = models.FloatField(db_column='SQN', blank=True, null=True)  # Field name made lowercase.
    created_at = models.DateTimeField(blank=True, null=True)
    filter = models.CharField(max_length=255, blank=True, null=True)
    win = models.FloatField(blank=True, null=True)
    server = models.CharField(max_length=255, blank=True, null=True)
    trade_num = models.IntegerField(blank=True, null=True)
    runtime = models.CharField(max_length=255, blank=True, null=True)
    starttime = models.CharField(max_length=100, blank=True, null=True)
    endtime = models.CharField(max_length=100, blank=True, null=True)
    benchreturns = models.TextField(db_column='benchReturns', blank=True, null=True)  # Field name made lowercase.
    roto = models.FloatField(blank=True, null=True)
    simulate = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'backtest'


class FactorsAnalysisModel(models.Model):
    factor_name = models.CharField(max_length=255, blank=True, null=True)
    days = models.CharField(max_length=255, blank=True, null=True)
    pool = models.CharField(max_length=255, blank=True, null=True)
    start_date = models.CharField(max_length=10, blank=True, null=True)
    end_date = models.CharField(max_length=10, blank=True, null=True)
    formula = models.TextField(blank=True, null=True)
    ic = models.FloatField(db_column='IC', blank=True, null=True)  # Field name made lowercase.
    ir = models.FloatField(db_column='IR', blank=True, null=True)  # Field name made lowercase.
    irr = models.FloatField(db_column='IRR', blank=True, null=True)  # Field name made lowercase.
    score = models.FloatField(blank=True, null=True)
    hash = models.CharField(max_length=255, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'factors_analysis'


class FactorsListModel(models.Model):
    factor_name = models.CharField(max_length=255, blank=True, null=True)
    indicators = models.CharField(max_length=255, blank=True, null=True)
    func_name = models.CharField(max_length=255, blank=True, null=True)
    code = models.TextField(blank=True, null=True)
    return_fileds = models.TextField(blank=True, null=True)
    md5 = models.CharField(max_length=255, blank=True, null=True)
    check_type = models.IntegerField(blank=True, null=True)
    status = models.CharField(max_length=20, blank=True, null=True)
    created_at = models.DateTimeField(blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'factors_list'


class FactorsMiningModel(models.Model):
    factor_name = models.CharField(max_length=255, blank=True, null=True)
    days = models.CharField(max_length=255, blank=True, null=True)
    pool = models.CharField(max_length=255, blank=True, null=True)
    start_date = models.CharField(max_length=10, blank=True, null=True)
    end_date = models.CharField(max_length=10, blank=True, null=True)
    formula = models.TextField(blank=True, null=True)
    ic = models.FloatField(db_column='IC', blank=True, null=True)  # Field name made lowercase.
    ir = models.FloatField(db_column='IR', blank=True, null=True)  # Field name made lowercase.
    irr = models.FloatField(db_column='IRR', blank=True, null=True)  # Field name made lowercase.
    score = models.FloatField(blank=True, null=True)
    hash = models.CharField(unique=True, max_length=255, blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'factors_mining'


class NicknameModelModel(models.Model):
    id = models.IntegerField(primary_key=True)
    model_id = models.CharField(max_length=32, blank=True, null=True)
    name = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'nickname_model'


class NicknameStrageyModel(models.Model):
    id = models.IntegerField(primary_key=True)
    bt_id = models.CharField(max_length=32, blank=True, null=True)
    name = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'nickname_stragey'


class StockFinhackRimModel(models.Model):
    code = models.CharField(max_length=10, blank=True, null=True)
    ts_code = models.CharField(max_length=15, blank=True, null=True)
    name = models.CharField(max_length=20, blank=True, null=True)
    industry = models.CharField(max_length=100, blank=True, null=True)
    price = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    value = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    value_end = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    value_max = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    vp = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    vep = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    vmp = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    date = models.DateField(blank=True, null=True)
    rcount = models.IntegerField(blank=True, null=True)
    vlist = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'stock_finhack_rim'
