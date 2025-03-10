from unicodedata import name
from django.conf.urls import url
from product_rating_review import views

app_name = 'product_rating_review'

urlpatterns = [
    url(r'^python/product/review/reply$', views.ProductReviewReply.as_view(), name="ProductReviewReply")
]
