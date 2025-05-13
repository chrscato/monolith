"""
Provider models for the intake portal.
"""
from django.db import models
from django.contrib.gis.db import models as gis_models

class Provider(models.Model):
    """Model representing a healthcare provider."""
    name = models.CharField(max_length=255)
    address = models.TextField()
    phone = models.CharField(max_length=20)
    email = models.EmailField(blank=True)
    specialties = models.JSONField(default=list)
    location = gis_models.PointField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    class Meta:
        ordering = ['name'] 