from django.conf.urls import url
from mettamuse_api import views

app_name = 'mettamuse_api'


urlpatterns = [
    url(r'^python/search/filter$', views.Search.as_view(), name='Search'),  # get api for the filter and search
    url(r'^python/searchFilter$', views.Search.as_view(), name='SearchOLD'), # get api for the filter and search
    url(r'^python/mettamuse/search/filter$', views.MettamuseSearch.as_view(), name='MettamuseSearch'),  # get api for the filter and search
    url(r'^python/filter/list$', views.FilterList.as_view(), name='FilterList'),  # get api for the filter and search
    url(r'^python/mettamuse/filter/list$', views.MettamuseFilterList.as_view(), name='MettamuseFilterList'),  # get api for the filter and search
    url(r'^python/product/suggestions$', views.ProductSuggestionsMettamuse.as_view(), name='ProductSuggestionsMettamuse'),
    url(r'^python/get/mettamuse/homepage$', views.MettamuseHomePage.as_view(), name='MettamuseHomePage'), # apt to get mettamuse homepage
    url(r'^python/currency/list$', views.CurrencyList.as_view(), name='CurrencyList'),
]
