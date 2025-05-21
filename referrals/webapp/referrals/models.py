# monolith/referrals/webapp/referrals/models.py
"""Django models for the referrals app."""
from django.db import models

# These are proxy models for the SQLAlchemy models in the database
# They allow Django to work with the existing database without migrations

class Referral(models.Model):
    """Referral model - proxy for SQLAlchemy Referral."""
    class Meta:
        managed = False
        db_table = 'referrals'

    id = models.AutoField(primary_key=True)
    email_id = models.CharField(max_length=100, unique=True)
    subject = models.CharField(max_length=255, blank=True, null=True)
    sender = models.CharField(max_length=100, blank=True, null=True)
    received_date = models.DateTimeField(blank=True, null=True)
    body_text = models.TextField(blank=True, null=True)
    status = models.CharField(max_length=50, blank=True, null=True)
    created_at = models.DateTimeField(blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)
    
    def __str__(self):
        return f"Referral {self.id}: {self.subject}"

class Attachment(models.Model):
    """Attachment model - proxy for SQLAlchemy Attachment."""
    class Meta:
        managed = False
        db_table = 'attachments'
    
    id = models.AutoField(primary_key=True)
    referral_id = models.IntegerField()
    filename = models.CharField(max_length=255, blank=True, null=True)
    s3_key = models.CharField(max_length=255, blank=True, null=True)
    content_type = models.CharField(max_length=100, blank=True, null=True)
    size = models.IntegerField(blank=True, null=True)
    uploaded = models.BooleanField(default=False)
    created_at = models.DateTimeField(blank=True, null=True)
    
    def __str__(self):
        return f"Attachment {self.id}: {self.filename}"

class ExtractedData(models.Model):
    """ExtractedData model - proxy for SQLAlchemy ExtractedData."""
    class Meta:
        managed = False
        db_table = 'extracted_data'
    
    id = models.AutoField(primary_key=True)
    referral_id = models.IntegerField()
    patient_first_name = models.CharField(max_length=100, blank=True, null=True)
    patient_last_name = models.CharField(max_length=100, blank=True, null=True)
    patient_dob = models.CharField(max_length=20, blank=True, null=True)
    patient_phone = models.CharField(max_length=20, blank=True, null=True)
    patient_address = models.TextField(blank=True, null=True)
    patient_city = models.CharField(max_length=100, blank=True, null=True)
    patient_state = models.CharField(max_length=2, blank=True, null=True)
    patient_zip = models.CharField(max_length=10, blank=True, null=True)
    insurance_provider = models.CharField(max_length=100, blank=True, null=True)
    insurance_id = models.CharField(max_length=50, blank=True, null=True)
    referring_physician = models.CharField(max_length=100, blank=True, null=True)
    physician_npi = models.CharField(max_length=20, blank=True, null=True)
    service_requested = models.CharField(max_length=255, blank=True, null=True)
    status = models.CharField(max_length=50, blank=True, null=True)
    created_at = models.DateTimeField(blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)
    
    def __str__(self):
        return f"ExtractedData {self.id} for Referral {self.referral_id}"