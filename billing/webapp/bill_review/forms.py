# billing/webapp/bill_review/forms.py
from django import forms

class BillUpdateForm(forms.Form):
    STATUS_CHOICES = [
        ('MAPPED', 'Mapped'),
        ('REVIEWED', 'Reviewed'),
        ('FLAGGED', 'Flagged'),
        ('ERROR', 'Error'),
        ('ARTHROGRAM', 'Arthrogram'),
        ('ESCALATE', 'Escalate'),
        ('DENIED', 'Denied'),
    ]
    
    ACTION_CHOICES = [
        ('', 'None'),
        ('to_review', 'To Review'),
        ('apply_rate', 'Apply Rate'),
        ('update_prov_info', 'Update Provider Info'),
        ('review_rates', 'Review Rates'),
        ('address_line_item_mismatch', 'Address Line Item Mismatch'),
        ('complete_line_item_mismatch', 'Complete Line Item Mismatch'),
        ('resolve_escalation', 'Resolve Escalation'),
        ('deny-not a cdx claim', 'Deny - Not a CDX Claim'),
        ('deny-CO-50', 'Deny - CO-50'),
        ('review_rate', 'Review Rate'),
        ('apply_rate', 'Apply Rate'),
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
    place_of_service = forms.CharField(
        max_length=10,
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'e.g., 11 (Office)'
        })
    )
    date_of_service = forms.DateField(
        required=True,
        widget=forms.DateInput(attrs={
            'class': 'form-control',
            'type': 'date'
        })
    )

class OTARateForm(forms.Form):
    cpt_code = forms.CharField(
        max_length=10,
        widget=forms.TextInput(attrs={'readonly': 'readonly'})
    )
    modifier = forms.CharField(
        max_length=10,
        required=False,
        widget=forms.TextInput(attrs={'readonly': 'readonly'})
    )
    rate = forms.DecimalField(max_digits=10, decimal_places=2)

class PPORateForm(forms.Form):
    cpt_code = forms.CharField(
        max_length=10,
        widget=forms.TextInput(attrs={'readonly': 'readonly'})
    )
    modifier = forms.CharField(
        max_length=10,
        required=False,
        widget=forms.TextInput(attrs={'readonly': 'readonly'})
    )
    proc_desc = forms.CharField(
        max_length=255,
        required=False,
        widget=forms.TextInput(attrs={'class': 'form-control'})
    )
    proc_category = forms.CharField(
        max_length=100,
        required=False,
        widget=forms.TextInput(attrs={'class': 'form-control'})
    )
    rate = forms.DecimalField(
        max_digits=10,
        decimal_places=2,
        widget=forms.NumberInput(attrs={'class': 'form-control'})
    )

class BillMappingForm(forms.Form):
    patient_last_name = forms.CharField(
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Enter patient last name',
            'autocomplete': 'off'
        }),
        help_text='Enter the patient\'s last name to search for matching orders'
    )
    patient_first_name = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Enter patient first name (optional)',
            'autocomplete': 'off'
        }),
        help_text='Optionally enter the patient\'s first name to narrow the search'
    )
    date_from = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={
            'class': 'form-control',
            'type': 'date',
            'placeholder': 'Start date (optional)'
        }),
        help_text='Optional start date for order search'
    )
    date_to = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={
            'class': 'form-control',
            'type': 'date',
            'placeholder': 'End date (optional)'
        }),
        help_text='Optional end date for order search'
    )

class AddLineItemForm(forms.Form):
    DECISION_CHOICES = [
        ('pending', 'Pending'),
        ('approved', 'Approved'),
        ('reduced', 'Reduced'),
        ('denied', 'Denied'),
    ]
    
    cpt_code = forms.CharField(
        max_length=10,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Enter CPT code'
        })
    )
    modifier = forms.CharField(
        max_length=10, 
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Optional modifier'
        })
    )
    units = forms.IntegerField(
        min_value=1,
        initial=1,
        widget=forms.NumberInput(attrs={
            'class': 'form-control',
            'min': '1'
        })
    )
    charge_amount = forms.DecimalField(
        max_digits=10, 
        decimal_places=2,
        widget=forms.NumberInput(attrs={
            'class': 'form-control',
            'step': '0.01',
            'placeholder': '0.00'
        })
    )
    allowed_amount = forms.DecimalField(
        max_digits=10, 
        decimal_places=2, 
        required=False,
        widget=forms.NumberInput(attrs={
            'class': 'form-control',
            'step': '0.01',
            'placeholder': '0.00'
        })
    )
    decision = forms.ChoiceField(
        choices=DECISION_CHOICES,
        initial='pending',
        widget=forms.Select(attrs={'class': 'form-select'})
    )
    reason_code = forms.CharField(
        max_length=20, 
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Optional reason code'
        })
    )
    place_of_service = forms.CharField(
        max_length=10,
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'e.g., 11 (Office)'
        })
    )
    date_of_service = forms.DateField(
        required=True,
        widget=forms.DateInput(attrs={
            'class': 'form-control',
            'type': 'date'
        })
    )