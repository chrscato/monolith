"""
Admin configuration for providers app.
"""
from django.contrib import admin
from .models import Provider

@admin.register(Provider)
class ProviderAdmin(admin.ModelAdmin):
    list_display = ('name', 'phone', 'email', 'created_at', 'updated_at')
    search_fields = ('name', 'address', 'phone', 'email')
    list_filter = ('created_at', 'updated_at')
    readonly_fields = ('created_at', 'updated_at') 