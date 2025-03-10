from django.conf.urls import url
from inventory_validation_app import views

app_name = 'inventory_validation_app'

urlpatterns = [
    url(r'^python/validateInventory/$', views.ValidateInventory.as_view(), name='ValidateInventory'),
    url(r'^zone/wise/validate/inventory$', views.ZoneWiseValidateInventory.as_view(), name='ZoneWiseValidateInventory'),
    url(r'^unit/measurement/list$', views.UnitMeasurementList.as_view(), name='UnitMeasurementList'),
]