# billing/models/reference.py

from django.db import models

class FeeSchedule(models.Model):
    state = models.CharField(max_length=2)
    cpt_code = models.CharField(max_length=10)
    modifier = models.CharField(max_length=5, blank=True)
    rate = models.DecimalField(max_digits=10, decimal_places=2)
    effective_date = models.DateField()

    def __str__(self):
        return f"{self.state} - {self.cpt_code}/{self.modifier} - {self.rate}"

class AdjustmentReason(models.Model):
    code = models.CharField(max_length=10, unique=True)
    label = models.CharField(max_length=100)
    description = models.TextField()

    def __str__(self):
        return f"{self.code} - {self.label}"

