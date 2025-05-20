from django.db import models

# Create your models here.

class ProviderBill(models.Model):
    id = models.TextField(primary_key=True)
    claim_id = models.TextField(null=True, blank=True)
    status = models.TextField(null=True, blank=True)
    action = models.TextField(null=True, blank=True)
    last_error = models.TextField(null=True, blank=True)
    created_at = models.TextField(null=True, blank=True)
    patient_name = models.TextField(null=True, blank=True)
    patient_dob = models.TextField(null=True, blank=True)
    patient_zip = models.TextField(null=True, blank=True)
    billing_provider_name = models.TextField(null=True, blank=True)
    billing_provider_address = models.TextField(null=True, blank=True)
    billing_provider_tin = models.TextField(null=True, blank=True)
    billing_provider_npi = models.TextField(null=True, blank=True)
    total_charge = models.FloatField(null=True, blank=True)
    patient_account_no = models.TextField(null=True, blank=True)
    
    class Meta:
        db_table = 'ProviderBill'
        managed = False
