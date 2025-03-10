from django.conf.urls import url
from product_details_apis import views
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")

app_name = 'product_details_apis'

urlpatterns = [
    url(r'^python/product/details$', views.ProductDetails.as_view(), name='ProductDetails'),
    url(r'^python/sizeChart$', views.SizeChart.as_view(), name='SizeChart'),
    url(r'^python/size/chart$', views.ProductSizeChart.as_view(), name='ProductSizeChart'),
    url(r'^python/seller/list$', views.SellerList.as_view(), name='SellerList'),
    url(r'^python/substitute/products$', views.SubstituteProduct.as_view(), name='SubstituteProduct'),
    url(r'^python/combo/products$', views.ComboProductList.as_view(), name='ComboProductList'),
    url(r'^python/product/portion$', views.ProductPortionData.as_view(), name='ProductPortionData')
]
