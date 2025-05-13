# billing/webapp/cdx_ehr/urls.py
from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('bill_review.urls')),
]