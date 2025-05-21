# billing/webapp/bill_review/urls.py
from django.urls import path
from . import views

app_name = 'bill_review'

urlpatterns = [
    path('', views.dashboard, name='dashboard'),
    path('bill/<str:bill_id>/', views.bill_detail, name='bill_detail'),
    path('bill/<str:bill_id>/update/', views.update_bill, name='update_bill'),
    path('bill/<str:bill_id>/reset/', views.reset_bill, name='reset_bill'),
    path('bill/<str:bill_id>/pdf/', views.view_bill_pdf, name='view_bill_pdf'),
    path('line-item/<str:line_item_id>/update/', views.line_item_update, name='line_item_update'),
    path('line-item/<str:line_item_id>/delete/', views.line_item_delete, name='line_item_delete'),
    path('provider/<str:provider_id>/update/<str:bill_id>/', views.update_provider, name='update_provider'),
]