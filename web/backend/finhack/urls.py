from rest_framework.routers import SimpleRouter

from .views import BacktestModelViewSet

router = SimpleRouter()
router.register("backtest", BacktestModelViewSet)

urlpatterns = [
]
urlpatterns += router.urls