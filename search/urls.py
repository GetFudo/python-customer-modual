from django.conf.urls import url
from search import views
from rest_framework import permissions
from rest_framework import permissions
from drf_yasg.views import get_schema_view
from drf_yasg import openapi
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "searchApi.settings")
from search_api.settings import SWAGGER_URL

app_name = 'search'

schema_view = get_schema_view(
	openapi.Info(
		title="Search API",
		default_version='python',
	),
	url=SWAGGER_URL,
	# url="http://localhost:8000/",
	public=False,
	permission_classes=(permissions.AllowAny,),
)


urlpatterns = [
	url(r'^documentation/$', schema_view.with_ui('swagger', cache_timeout=0), name='schema-swagger-ui'),

    url(r'^python/product/review$', views.ProductReviewList.as_view(), name='ProductReviewList'),
    url(r'^python/zones$', views.ZoneDetails.as_view(), name='ZoneDetails'),
    url(r'^python/offer/products$', views.OfferProducts.as_view(), name='OfferList'),
    url(r'^python/meat/offer/products$', views.MeatOfferProducts.as_view(), name='MeatOfferProducts'),
    url(r'^python/favourite/product/$', views.FavouriteProduct.as_view(), name='AddFavouriteProduct'),
    url(r'^python/favourite/product/stores$', views.FavouriteProductAndStores.as_view(), name='FavouriteProductAndStores'),
    url(r'^python/wishList/$', views.WishList.as_view(), name='WishList'),
    url(r'^python/local/foodies$', views.LocalFoodies.as_view(), name='LocalFoodies'),
	url(r'^storeProductSearch/$', views.StoreProductSearch.as_view(), name='StoreProductSearch'),
    url(r'^storeProductDetails/$', views.StoreProductDetails.as_view(), name='StoreProductDetails'),
    # # ===================================product review and rating =================================================
    url(r'^python/productReviewRating/$', views.ProductReviewRating.as_view(), name='ProductReviewRating'),
    url(r'^python/projectReviewRating/$', views.ProjectReviewRating.as_view(), name='ProjectReviewRating'),
    url(r'^python/likeDislikeReview/$', views.LikesDislikeReview.as_view(), name='LikesDislikeReview'),
    url(r'^python/reportReview/$', views.ReportReview.as_view(), name='ReportReview'),
	url(r'^python/ratableAttribute$', views.RatableAttribute.as_view(), name='RatableAttribute'),
    url(r'^python/brand/list$', views.BrandList.as_view(), name='BrandList'),
    url(r'^python/symptoms/list$', views.SymptomsList.as_view(), name='SymptomsList'),
    url(r'^python/brand/products$', views.BrandProducts.as_view(), name='BrandProducts'),
    url(r'^python/newsLetter/$', views.NewsLetter.as_view(), name='NewsLetter'),
    url(r'^python/updateInventory/$', views.UpdateInventory.as_view(), name='UpdateInventory'),
    url(r'^python/variants$', views.Variants.as_view(), name='GetVariants'),
    url(r'^python/child/variants$', views.ChildVariants.as_view(), name='GetVariants'),
    url(r'^python/preferences$', views.UserPreferences.as_view(), name='UserPreferences'),
	url(r'^python/search$', views.AdminSearch.as_view(), name='AdminSearch'),
    url(r'^python/redemption/$', views.redemption.as_view(), name='redemption'),
    url(r'^python/scan/products$', views.QrScanProducts.as_view(), name='QrScanProducts'),
    url(r'^python/product/notify$', views.ProductNotify.as_view(), name='ProductNotify'),
    url(r'^python/user/address$', views.UserAddress.as_view(), name='QrScanProducts'),
	url(r'^sellerFilter/$', views.SellerFilter.as_view(), name='SellerFilter'),
	url(r'^python/user/recentview$', views.UserRecentView.as_view(), name='UserRecentView'),
	url(r'^python/recent/search$', views.UserRecentSearch.as_view(), name='UserRecentSearch'),
	url(r'^python/stores/products/$', views.SimilarStoreProducts.as_view(), name='SimilarStoreProducts'),
	url(r'^python/rating/products/$', views.RatingProductsList.as_view(), name='RatingProductsList'),
	url(r'^python/shopping/list$', views.ShoppingList.as_view(), name='ShoppingList'),
	url(r'^python/product/shopping/list$', views.ProductShoppingList.as_view(), name='ProductShoppingList'),
	url(r'^python/user/shopping/list$', views.ShoppingListData.as_view(), name='ShoppingListData'),
	url(r'^python/shopping/list/products$', views.ShoppingListProducts.as_view(), name='ShoppingListProducts'),
	url(r'^python/store/list$', views.StoreList.as_view(), name='StoreList'),
	url(r'^python/store/lastCheckins$', views.StoreLastCheckins.as_view(), name='LastCheckins'),
	url(r'^python/store/storedetails$', views.StoreDetails.as_view(), name='StoreDetails'),
    url(r'^python/review/details$', views.ProductReviewDetails.as_view(), name='ProductReviewDetails'),
    url(r'^python/user/notification$', views.UserNotificationDetails.as_view(), name='UserNotificationDetails'),
    url(r'^python/user/notification/count$', views.UserNotificationCount.as_view(), name='UserNotificationCount'),
    url(r'^python/user/favourite/stores$', views.UserFavouriteStores.as_view(), name='UserFavouriteStores'),
    url(r'^python/seller/rating$', views.SellerRatingRating.as_view(), name='SellerRatingRating'),
    url(r'^python/seller/review$', views.SellerRatingReview.as_view(), name='SellerRatingReview'),
    url(r'^python/get/store/products$', views.StoreSearchProduct.as_view(), name='StoreSearchProduct'),
    url(r'^python/store/filter$', views.StoreFilter.as_view(), name='StoreFilter'),
    # # ============================food=================================================
    url(r'^python/get/food/products/$', views.FoodProductList.as_view(), name='FoodProductList'),
    url(r'^python/get/products/$', views.FoodProducts.as_view(), name='FoodProducts'),
    url(r'^python/get/store$', views.AllStoreList.as_view(), name='AllStoreList'),
    url(r'^python/suggestion/food/products/$', views.FoodProductSuggestion.as_view(), name='FoodProductSuggestion'),
    url(r'^python/product/addons$', views.ProductAddOnsData.as_view(), name='ProductAddOnsData'),
    url(r'^python/get/food/products/indispatcher$', views.FoodProductsInDispatcher.as_view(), name='FoodProductsInDispatcher'),
    url(r'^python/food/product/search$', views.FoodSuggestion.as_view(), name='FoodSuggestion'),
    url(r'^python/get/store/filters$', views.StoreSpecialies.as_view(), name='StoreSpecialies'),
    url(r'^python/food/products$', views.FoodProductSearch.as_view(), name='FoodProductSearch'),
    url(r'^python/food/filter/products$', views.FoodProductMeatFilter.as_view(), name='FoodProductMeatFilter'),
    #==================================================================================================
    url(r'^python/store/category/list$', views.StoreCategoryDetails.as_view(), name='StoreCategoryDetails'),
    url(r'^python/get/services$', views.ServiceDetails.as_view(), name='ServiceDetails'),
    url(r'^python/product/customizable/attributes$', views.ProductCustomizableAttributes.as_view(), name='ProductCustomizableAttributes'),
    url(r'^python/category/list$', views.ProductCategoryList.as_view(), name='CategoryList'), # done
    url(r'^python/customer/rating$', views.CustomerRating.as_view(), name='CustomerRating'),
    url(r'^python/get/hyperLocalStores$', views.HyperLocalStores.as_view(), name='HyperLocalStores'),
    url(r'^python/get/product/tax$', views.ProductTaxes.as_view(), name='ProductTaxes'),
    url(r'^python/get/store/category/setting$', views.StoreCategorySetting.as_view(), name='StoreCategorySetting'),
    url(r'^python/update/product/$', views.UpdateProductDetails.as_view(), name='UpdateProductDetails'),
    url(r'^python/store/details$', views.StoreDeatails.as_view(), name='StoreDeatails'),
    url(r'^python/get/cuisine$', views.CuisineList.as_view(), name='CuisineList'),
    url(r'^python/get/more/stores$', views.GetMoreStores.as_view(), name='GetMoreStores'),
    url(r'^python/get/storeCategoryConfig$', views.StoreCategoryConfig.as_view(), name='StoreCategoryConfig'),
    url(r'^python/get/static/pages$', views.StaticPageData.as_view(), name='StaticPageData'),
    url(r'^python/driver/rating/$', views.DriverRating.as_view(), name='DriverRating'),
    # ========================================store app api=================================================
    url(r'^python/store/product/list$', views.StoreProductList.as_view(), name='StoreProductList'),
    url(r'^python/product/set/availability$', views.PrdocuctSetAvailability.as_view(), name='PrdocuctSetAvailability'),
    url(r'^python/get/portion$', views.PrdocuctGetPortion.as_view(), name='PrdocuctGetPortion'),
    url(r'^python/get/stock/details$', views.PrdocuctGetProductStockDetails.as_view(), name='PrdocuctGetProductStockDetails'),
    url(r'^python/stock/update$', views.PrdocuctStockUpdate.as_view(), name='PrdocuctStockUpdate'),
    url(r'^python/get/link/stores$', views.ProductLinkStores.as_view(), name='ProductLinkStores'),
    url(r'^python/get/product/batch$', views.ProductBatch.as_view(), name='ProductBatch'),
    #===============================================global search===================================
    url(r'^python/all/store/search$', views.AllStoreSearch.as_view(), name='AllStoreSearch'),
    url(r'^python/all/store/business$', views.AllStoreBusiness.as_view(), name='AllStoreSearch'),
    url(r'^python/global/search$', views.PrdocuctGlobalSearch.as_view(), name='PrdocuctGlobalSearch'),
    url(r'^python/validate/batch$', views.ValidateBatch.as_view(), name='ValidateBatch'),
    #===============================================supporting api for pdp page===================================
    url(r'^python/get/pdpdata$', views.SlugDetails.as_view(), name='SlugDetails'),
    #=========================== add store detail API in category WISE ============
    url(r'^python/category/store/detail', views.CategoryStoreDetail.as_view(), name="categoryStoreDetail"),
    url(r'^python/heatmap$', views.HeatMap.as_view(), name='HeatmapDetail'),
    url(r'^UnauthorizedProductCategoryList', views.UnauthorizedProductCategoryList.as_view(), name="unauthorizedProductCategoryList"),

    url(r'^get/storeReviews$', views.StoreReviewDetails.as_view(), name='storeReviewDetails'), # Api for get store review
    url(r'^get/storeMenu$', views.FoodProductSearch.as_view(), name='FoodProductSearch'), # api for get store product data
    url(r'^get/moreDetailsAboutStore$', views.StoreMoreDetails.as_view(), name='storeMoreDetails'), # api for get storeTime , about and etc
    url(r'^get/storeMenuItemDetails$', views.StoreMenuItemDetails.as_view(), name='storeMoreDetails'), # api for get all product portion and addons details
    url(r'^get/nearbyStores', views.GetNearByStore.as_view(), name='nearby_stores'),
    url(r'^get/closeStoresByZone$', views.CloseStoresByZone.as_view(), name='CloseStoresByZone'), # api for get store list base on rating
    url(r'^get/recomdedStoresByZone$', views.RecomdedStoresByZone.as_view(), name='RecomdedStoresByZone'), # api for get store list base on rating
    url(r'^get/newStoresByZone$', views.NewStoresByZone.as_view(), name='NewStoresByZone'), # api for get store list which are created within 60 days
    url(r'^get/menuItems$', views.MenuItems.as_view(), name='MenuItems'), # api for get store list which are created within 60 days
    url(r'^get/popularStoresByZone$', views.PopularStoresByZone.as_view(), name='PopularStoresByZone'), # api for get store list baseed on store order 
    url(r'^get/bookTableNearYouByZone$', views.BookTableNearYouByZone.as_view(), name='BookTableNearYouByZone'), # api for get store list whihc have table booking available 
    url(r'^get/storeProductsWithVariants$', views.StoreProductsVariantsDetails.as_view(), name='StoreProductsVariantsDetails'), # api for get all product portion and addons details
    url(r'^get/home/page/product/search$', views.HomePageProductSearch.as_view(), name='HomePageProductSearch'), # api for get store list base on rating
    url(r'^get/banner/list$', views.GetBannerList.as_view(), name='HomePageProductSearch'), # api for get store list base on rating
]