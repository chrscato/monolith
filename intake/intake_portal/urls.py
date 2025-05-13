"""
URL configuration for intake_portal project.
"""
from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('providers/', include('providers.urls')),
    path('referrals/', include('referrals.urls')),
] 