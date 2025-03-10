from django.conf.urls import url
from meola_apis import views
app_name = 'meola_apis'


urlpatterns = [
    url(r'^python/mega/menu/offer/list$', views.MegaMenuOfferList.as_view(), name='MegaMenuOfferList'),
    url(r'^python/brand$', views.PythonBrand.as_view(), name='PythonBrand'),
    url(r'^python/stores$', views.PythonStores.as_view(), name='PythonStores'),
    url(r'^python/product/variant$', views.PythonProductVariant.as_view(), name='PythonProductVariant'),
    url(r'^python/category/seo$', views.PythonCategorySeo.as_view(), name='PythonCategorySeo'),
    url(r'^python/get/all/sales$', views.PythonAllSales.as_view(), name='PythonAllSales'),
    url(r'^python/blogs/product/count$', views.BlogGet.as_view(), name='blogGet'),
]