# monolith/referrals/webapp/referrals/urls.py
"""URL configuration for the referrals app."""
from django.urls import path
from . import views

app_name = 'referrals'

urlpatterns = [
    path('', views.dashboard, name='dashboard'),
    path('referrals/', views.referral_list, name='referral_list'),
    path('referrals/<int:referral_id>/', views.referral_detail, name='referral_detail'),
    path('referrals/<int:referral_id>/complete/', views.mark_referral_complete, name='mark_complete'),
    path('queue/pending-reviews/', views.pending_reviews, name='pending_reviews'),
]