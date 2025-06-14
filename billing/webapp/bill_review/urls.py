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
    path('bill/<str:bill_id>/map/<str:order_id>/', views.map_bill_to_order, name='map_bill_to_order'),
    path('line-item/<str:line_item_id>/update/', views.line_item_update, name='line_item_update'),
    path('line-item/<str:line_item_id>/delete/', views.line_item_delete, name='line_item_delete'),
    path('provider/<str:provider_id>/update/<str:bill_id>/', views.update_provider, name='update_provider'),
    path('bill/<str:bill_id>/line-item/<int:line_item_id>/add-ota-rate/', views.add_ota_rate, name='add_ota_rate'),
    path('bill/<str:bill_id>/line-item/<int:line_item_id>/add-ppo-rate/', views.add_ppo_rate, name='add_ppo_rate'),
    path('instructions/', views.instructions, name='instructions'),
    path('bill/<str:bill_id>/line-item/add/', views.add_line_item, name='add_line_item'),
]