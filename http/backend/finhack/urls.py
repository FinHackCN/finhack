from rest_framework.routers import SimpleRouter
from django.urls import path
from .views import BacktestModelViewSet
from .tests import hello

router = SimpleRouter()
router.register("backtest", BacktestModelViewSet)

#path('my-view/', my_view, {'http_method': 'get'}, name='my-view-get'),
urlpatterns = [
    path('test/', hello)
]
urlpatterns += router.urls