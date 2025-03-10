from django.conf.urls import url
from home_page_api import views
app_name = 'home_page_api'


urlpatterns = [
    url(r'^python/home/page$', views.HomePage.as_view(), name='HomePage'),
    url(r'^python/clubmart/home/page$', views.ClubMartHomePage.as_view(), name='ClubMartHomePage'),
    url(r'^python/clubmart/stores$', views.ClubMartStores.as_view(), name='ClubMartStores'),
    url(r'^python/recent/view$', views.UserRecentView.as_view(), name='UserRecentView'),
    url(r'^python/recent/bought$', views.UserRecentBought.as_view(), name='UserRecentBought'),
    url(r'^python/popular/items$', views.PopularItems.as_view(), name='PopularItems'),
    url(r'^python/best/deals$', views.BestDeals.as_view(), name='BestDeals'),
    url(r'^python/subCategoryList$', views.SubCategoryList.as_view(), name='subCategoryList'),
    url(r'^python/subCategoryProducts/$', views.SubCategoryProducts.as_view(), name='SubCategoryProducts'),
    url(r'^python/delivery/slot$', views.DeliverySlot.as_view(), name='DeliverySlot'),
    url(r'^python/all/product/list$', views.AllProductList.as_view(), name='AllProductList'),
    url(r'^python/product/list$', views.PostAddProductsList.as_view(), name='PostAddProductsList'),
    url(r'^python/banner/list$', views.BannerList.as_view(), name='BannerList'),
    url(r'^python/product/seller/list$', views.ProductSellerList.as_view(), name='ProductSellerList'),
    url(r'^python/v2/home/page$', views.HomePageNew2.as_view(), name='HomePageNew2'),
    url(r'^python/v3/home/page$', views.HomePageNew3.as_view(), name='HomePageNew3'),
    url(r'^python/category/products$', views.CategoryProducts.as_view(), name='CategoryProducts'),
    url(r'^python/mega/menu$', views.MegaMenu.as_view(), name='MegaMenu'),
    url(r'^python/mega/menu/column$', views.MegaMenuColumn.as_view(), name='MegaMenuColumn'),
    url(r'^python/v4/home/page$', views.HomePageV4.as_view(), name='HomePageV4'),
    url(r'^python/get/zone/alerts$', views.ZoneAlerts.as_view(), name='ZoneAlerts'),
    url(r'^python/variant/list$', views.VariantList.as_view(), name='VariantList'),
    url(r'^python/seo/details$', views.SeoDetails.as_view(), name='SeoDetails'),
    url(r'^python/mega/menu/offer/list$', views.MegaMenuOfferList.as_view(), name='MegaMenuOfferList'),
    url(r'^python/v5/home/page$', views.HomePageIngredienta.as_view(), name='HomePageIngredienta'),
    url(r'^python/service/home/page', views.ServiceHomePage.as_view(), name="serviceHomePage"),
    url(r'^python/ricevan/stores', views.RicevanStores.as_view(), name="ricevanHomepage")
]