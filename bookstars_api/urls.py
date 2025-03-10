from django.conf.urls import url
from bookstars_api import views

app_name = 'bookstars_api'

urlpatterns = [
    url(r'^get/more/details$', views.ArtNiche.as_view(), name='ArtNiche'),
    url(r'^get/linked/user/list$', views.NicheRelatedArtists.as_view(), name='NicheRelatedArtists'),
]