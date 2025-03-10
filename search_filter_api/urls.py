from django.conf.urls import url
from search_filter_api import views
from rest_framework import permissions
import os
import sys


urlpatterns = [
    url(r'^python/v1/new/filter$', views.ProductFilterNew.as_view(), name='ProductFilterNew'),
    url(r'^python/v1/filter$', views.ProductFilter.as_view(), name='ProductFilter'),
    url(r'^python/v1/new/filter/value$', views.ProductFilterValueNew.as_view(), name='ProductFilterValueNew'),
    url(r'^python/v1/filter/value$', views.ProductFilterValue.as_view(), name='ProductFilterValue'),
    url(r'^python/suggestions/new$', views.ProductSuggestionsNew.as_view(), name='ProductSuggestionsNew'),
    url(r'^python/search/filter/new$', views.SearchNew.as_view(), name='SearchNew'), # get api for the filter and search
    url(r'^python/filter$', views.Filter.as_view(), name='Filter'), # get api for the filters(left side for website)
    url(r'^python/suggestions$', views.ProductSuggestions.as_view(), name='ProductSuggestions'),
]