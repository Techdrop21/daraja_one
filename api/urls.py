from django.urls import path
from . import views
from . import admin

app_name = 'api'

urlpatterns = [
    path('daraja/c2b/', views.daraja_c2b_callback, name='c2b_callback'),
    path('daraja/validation/', views.daraja_validation_endpoint, name='validation'),
    path('daraja/test-sheet-write/', views.daraja_test_sheet_write, name='test_sheet_write'),
    path('config/status/', admin.config_status, name='config_status'),
]
