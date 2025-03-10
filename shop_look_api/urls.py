from unicodedata import name
from django.conf.urls import url
from shop_look_api import views

app_name = 'shop_look_api'

urlpatterns = [
    url(r'^python/shop/the/look$', views.ShoopLook.as_view(), name="ShoopLook"),
    url(r'^python/shop-the-look/product-detail-page$', views.ShopTheLookPDP.as_view(), name="ShopTheLookPDP"),
]
