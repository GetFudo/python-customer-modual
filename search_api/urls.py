"""searchApi URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.8/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Add an import:  from blog import urls as blog_urls
    2. Add a URL to urlpatterns:  url(r'^blog/', include(blog_urls))
"""
from django.conf.urls import include, url
from django.contrib import admin

urlpatterns = [
    url('admin/', admin.site.urls),
    url(r'^', include('search.urls')),
    url(r'^', include('mettamuse_api.urls')),
    url(r'^', include('home_page_api.urls')),
    url(r'^', include('epic_api.urls')),
    url(r'^', include('search_filter_api.urls')),
    url(r'^', include('inventory_validation_app.urls')),
    url(r'^', include('bookstars_api.urls')),
    url(r'^', include('meola_apis.urls')),
    url(r'^', include('product_details_apis.urls')),
    url(r'^', include('product_rating_review.urls')),
    url(r'^', include('product_question_answer.urls')),
    url(r'^', include('shop_look_api.urls'))
]
