
# billing/models/bill.py

from django.db import models
import uuid

class ProviderBill(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    claim = models.ForeignKey('core.Claim', on_delete=models.CASCADE)
    uploaded_by = models.ForeignKey('core.User', on_delete=models.SET_NULL, null=True)
    source_file = models.CharField(max_length=255)  # S3 or other ref
    status = models.CharField(max_length=30, choices=[
        ('RECEIVED', 'Received'),
        ('REVIEWED', 'Reviewed'),
        ('FLAGGED', 'Flagged'),
        ('COMPLETED', 'Completed'),
    ])
    last_error = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Bill {self.id} for claim {self.claim_id}"

class BillLineItem(models.Model):
    provider_bill = models.ForeignKey(ProviderBill, on_delete=models.CASCADE, related_name="line_items")
    cpt_code = models.CharField(max_length=10)
    modifier = models.CharField(max_length=5, blank=True)
    units = models.PositiveIntegerField()
    charge_amount = models.DecimalField(max_digits=10, decimal_places=2)
    allowed_amount = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    decision = models.CharField(max_length=20, choices=[
        ('approved', 'Approved'),
        ('reduced', 'Reduced'),
        ('denied', 'Denied'),
        ('pending', 'Pending')
    ], default='pending')
    reason_code = models.CharField(max_length=20, blank=True)
    date_of_service = models.DateField()

    def __str__(self):
        return f"{self.cpt_code} - {self.charge_amount}"


