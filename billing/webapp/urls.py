from django.urls import path
from . import views

urlpatterns = [
    path("", views.DashboardView.as_view(), name="dashboard"),
    path("bill/<int:bill_id>/", views.bill_detail, name="bill_detail"),
]
