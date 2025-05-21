# monolith/referrals/webapp/referrals/forms.py
"""Forms for the referrals app."""
from django import forms
from .models import ExtractedData

class ReferralSearchForm(forms.Form):
    """Form for searching referrals."""
    search_query = forms.CharField(required=False, label='Search',
                                  widget=forms.TextInput(attrs={'placeholder': 'Search by patient name, email, etc.'}))
    status = forms.ChoiceField(required=False, choices=[
        ('', 'All Statuses'),
        ('new', 'New'),
        ('processing', 'Processing'),
        ('reviewed', 'Reviewed'),
        ('completed', 'Completed'),
    ])
    date_from = forms.DateField(required=False, label='From Date',
                               widget=forms.DateInput(attrs={'type': 'date'}))
    date_to = forms.DateField(required=False, label='To Date',
                             widget=forms.DateInput(attrs={'type': 'date'}))

class ExtractedDataForm(forms.ModelForm):
    """Form for editing extracted data."""
    class Meta:
        model = ExtractedData
        fields = [
            'patient_first_name', 'patient_last_name', 'patient_dob', 'patient_phone',
            'patient_address', 'patient_city', 'patient_state', 'patient_zip',
            'insurance_provider', 'insurance_id', 'referring_physician', 
            'physician_npi', 'service_requested', 'status'
        ]
        widgets = {
            'patient_dob': forms.DateInput(attrs={'type': 'date'}),
            'status': forms.Select(choices=[
                ('extracted', 'AI Extracted'),
                ('verified', 'Verified'),
                ('invalid', 'Invalid/Incomplete'),
            ]),
        }