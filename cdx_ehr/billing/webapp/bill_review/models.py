# billing/webapp/bill_review/models.py
from django.db import models

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
        db_table = 'ProviderBill'  # Specify the existing table
        managed = False  # Tell Django not to manage this table

class BillLineItem(models.Model):
    id = models.AutoField(primary_key=True)
    provider_bill = models.ForeignKey(ProviderBill, models.DO_NOTHING, db_column='provider_bill_id')
    cpt_code = models.TextField(null=True, blank=True)
    modifier = models.TextField(null=True, blank=True)
    units = models.IntegerField(null=True, blank=True)
    charge_amount = models.FloatField(null=True, blank=True)
    allowed_amount = models.FloatField(null=True, blank=True)
    decision = models.TextField(null=True, blank=True)
    reason_code = models.TextField(null=True, blank=True)
    date_of_service = models.TextField(null=True, blank=True)
    place_of_service = models.TextField(null=True, blank=True)
    diagnosis_pointer = models.TextField(null=True, blank=True)
    
    class Meta:
        db_table = 'BillLineItem'
        managed = False
        
class Order(models.Model):
    order_id = models.TextField(db_column='Order_ID', primary_key=True)
    patient_last_name = models.TextField(db_column='Patient_Last_Name', null=True, blank=True)
    patient_first_name = models.TextField(db_column='Patient_First_Name', null=True, blank=True)
    patient_dob = models.TextField(db_column='Patient_DOB', null=True, blank=True)
    bundle_type = models.TextField(null=True, blank=True)
    provider_id = models.TextField(null=True, blank=True)
    
    class Meta:
        db_table = 'orders'
        managed = False

class Provider(models.Model):
    primary_key = models.TextField(db_column='PrimaryKey', primary_key=True)
    dba_name = models.TextField(db_column='DBA Name Billing Name', null=True, blank=True)
    billing_name = models.TextField(db_column='Billing Name', null=True, blank=True)
    tin = models.TextField(db_column='TIN', null=True, blank=True)
    provider_network = models.TextField(db_column='Provider Network', null=True, blank=True)
    
    class Meta:
        db_table = 'providers'
        managed = False