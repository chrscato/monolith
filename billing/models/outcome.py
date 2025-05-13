# billing/models/outcome.py

from django.db import models

class EOBR(models.Model):
    provider_bill = models.OneToOneField('ProviderBill', on_delete=models.CASCADE)
    summary_notes = models.TextField(blank=True)
    file_path = models.CharField(max_length=255, null=True)  # S3 or file ref
    generated_by = models.ForeignKey('core.User', on_delete=models.SET_NULL, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"EOB for Bill {self.provider_bill.id}"

class ReimbursementLog(models.Model):
    bill_line = models.ForeignKey('BillLineItem', on_delete=models.CASCADE)
    paid_amount = models.DecimalField(max_digits=10, decimal_places=2)
    paid_date = models.DateField()
    method = models.CharField(max_length=50)  # Check, ACH, etc.

    def __str__(self):
        return f"Reimbursed {self.paid_amount} on {self.paid_date}"