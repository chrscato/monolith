# billing/webapp/bill_review/forms.py
from django import forms

class BillUpdateForm(forms.Form):
    STATUS_CHOICES = [
        ('MAPPED', 'Mapped'),
        ('REVIEWED', 'Reviewed'),
        ('FLAGGED', 'Flagged'),
        ('ERROR', 'Error'),
        ('ARTHROGRAM', 'Arthrogram'),
    ]
    
    ACTION_CHOICES = [
        ('', 'None'),
        ('to_review', 'To Review'),
        ('apply_rate', 'Apply Rate'),
        ('update_prov_info', 'Update Provider Info'),
        ('review_rates', 'Review Rates'),
        ('address_line_item_mismatch', 'Address Line Item Mismatch'),
        ('complete_line_item_mismatch', 'Complete Line Item Mismatch'),
    ]
    
    status = forms.ChoiceField(choices=STATUS_CHOICES)
    action = forms.ChoiceField(choices=ACTION_CHOICES, required=False)
    last_error = forms.CharField(widget=forms.Textarea(attrs={'rows': 3}), required=False)

class LineItemUpdateForm(forms.Form):
    DECISION_CHOICES = [
        ('pending', 'Pending'),
        ('approved', 'Approved'),
        ('reduced', 'Reduced'),
        ('denied', 'Denied'),
    ]
    
    cpt_code = forms.CharField(max_length=10)
    modifier = forms.CharField(max_length=10, required=False)
    units = forms.IntegerField(min_value=1)
    charge_amount = forms.DecimalField(max_digits=10, decimal_places=2)
    allowed_amount = forms.DecimalField(max_digits=10, decimal_places=2, required=False)
    decision = forms.ChoiceField(choices=DECISION_CHOICES)
    reason_code = forms.CharField(max_length=20, required=False)