from django.conf.urls import url
from epic_api import views

app_name = 'epic_api'

urlpatterns = [
    url(r'^python/social/banners$', views.SocialBannersList.as_view(), name='SocialBannersList'),
    url(r'^python/tagged/product/list$', views.TaggedProductList.as_view(), name='TaggedProductList'),
    url(r'^python/get/product/list$', views.ProductList.as_view(), name='ProductList'),
    url(r'^python/recommended/products/list$', views.RecommendedProductList.as_view(), name='RecommendedProductList'),
    url(r'^python/product/search$', views.ProductSearch.as_view(), name='ProductSearch'),
    url(r'^python/get/nearest/stores$', views.GetNearestStores.as_view(), name='GetNearestStores'),
    url(r'^python/nearest/searchFilter$', views.NearestStoreFilter.as_view(), name='NearestStoreFilter'),
    url(r'^python/tagged/post$', views.TaggedPostDetails.as_view(), name='TaggedPostDetails'),
    url(r'^python/ecommerce/store/list$', views.AllEcommerceStoreList.as_view(), name='AllEcommerceStoreList'),
    url(r'^python/add/combo/products$', views.AddComboProducts.as_view(), name='AddComboProducts'),
    url(r'^python/bevvy/best/sellers$', views.BevvyBestSellers.as_view(), name='BevvyBestSellers'),
]