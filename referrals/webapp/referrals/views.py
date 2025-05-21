# monolith/referrals/webapp/referrals/views.py
"""Views for the referrals app."""
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.db.models import Q
from django.contrib import messages
from django.http import HttpResponseRedirect
from django.urls import reverse
import boto3
from botocore.client import Config
import os
from datetime import datetime, timedelta

from .models import Referral, Attachment, ExtractedData
from .forms import ReferralSearchForm, ExtractedDataForm

@login_required
def dashboard(request):
    """Dashboard view showing referral queues."""
    # Get counts for each status
    new_count = Referral.objects.filter(status='new').count()
    processing_count = Referral.objects.filter(status='processing').count()
    reviewed_count = Referral.objects.filter(status='reviewed').count()
    completed_count = Referral.objects.filter(status='completed').count()
    
    # Get recent referrals
    recent_referrals = Referral.objects.all().order_by('-received_date')[:10]
    
    # Get pending reviews
    pending_reviews = Referral.objects.filter(
        status='processing'
    ).exclude(
        extracteddata__status='verified'
    ).order_by('received_date')[:10]
    
    context = {
        'new_count': new_count,
        'processing_count': processing_count,
        'reviewed_count': reviewed_count,
        'completed_count': completed_count,
        'recent_referrals': recent_referrals,
        'pending_reviews': pending_reviews,
    }
    
    return render(request, 'referrals/dashboard.html', context)

@login_required
def referral_list(request):
    """View showing a list of all referrals with search and filtering."""
    # Process search form
    form = ReferralSearchForm(request.GET)
    referrals = Referral.objects.all().order_by('-received_date')
    
    if form.is_valid():
        # Apply search filters
        search_query = form.cleaned_data.get('search_query')
        status = form.cleaned_data.get('status')
        date_from = form.cleaned_data.get('date_from')
        date_to = form.cleaned_data.get('date_to')
        
        if search_query:
            referrals = referrals.filter(
                Q(subject__icontains=search_query) |
                Q(sender__icontains=search_query) |
                Q(extracteddata__patient_first_name__icontains=search_query) |
                Q(extracteddata__patient_last_name__icontains=search_query) |
                Q(extracteddata__patient_phone__icontains=search_query)
            ).distinct()
        
        if status:
            referrals = referrals.filter(status=status)
        
        if date_from:
            referrals = referrals.filter(received_date__gte=date_from)
        
        if date_to:
            # Include the entire day
            date_to = datetime.combine(date_to, datetime.max.time())
            referrals = referrals.filter(received_date__lte=date_to)
    
    # Paginate results
    paginator = Paginator(referrals, 25)  # 25 referrals per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    context = {
        'form': form,
        'page_obj': page_obj,
    }
    
    return render(request, 'referrals/referral_list.html', context)

@login_required
def referral_detail(request, referral_id):
    """View showing details of a specific referral with options to edit extracted data."""
    referral = get_object_or_404(Referral, id=referral_id)
    
    # Get attachments and extracted data
    attachments = Attachment.objects.filter(referral_id=referral.id)
    
    try:
        extracted_data = ExtractedData.objects.get(referral_id=referral.id)
        form = ExtractedDataForm(instance=extracted_data)
    except ExtractedData.DoesNotExist:
        extracted_data = None
        form = ExtractedDataForm()
    
    # Process form submission
    if request.method == 'POST':
        if extracted_data:
            form = ExtractedDataForm(request.POST, instance=extracted_data)
        else:
            form = ExtractedDataForm(request.POST)
            
        if form.is_valid():
            # If this is a new extracted data record, set the referral ID
            if not extracted_data:
                extracted_data = form.save(commit=False)
                extracted_data.referral_id = referral.id
                extracted_data.created_at = datetime.now()
            
            # Update the record
            extracted_data = form.save(commit=False)
            extracted_data.updated_at = datetime.now()
            extracted_data.save()
            
            # If status is verified and referral is still processing, update it
            if extracted_data.status == 'verified' and referral.status == 'processing':
                referral.status = 'reviewed'
                referral.updated_at = datetime.now()
                referral.save()
            
            messages.success(request, 'Referral data updated successfully.')
            return redirect('referrals:referral_detail', referral_id=referral.id)
    
    # Generate presigned URLs for attachments
    s3_client = boto3.client('s3',
                         aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                         aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
                         region_name=os.environ.get('AWS_REGION', 'us-east-1'),
                         config=Config(signature_version='s3v4'))
    
    for attachment in attachments:
        if attachment.uploaded and attachment.s3_key:
            try:
                # Generate presigned URL for 1 hour
                url = s3_client.generate_presigned_url(
                    'get_object',
                    Params={
                        'Bucket': os.environ.get('S3_BUCKET_NAME'),
                        'Key': attachment.s3_key
                    },
                    ExpiresIn=3600
                )
                attachment.presigned_url = url
                
                # Set preview flag for PDFs and images
                if attachment.content_type in ['application/pdf', 'image/jpeg', 'image/png', 'image/gif']:
                    attachment.can_preview = True
                else:
                    attachment.can_preview = False
                    
            except Exception as e:
                print(f"Error generating presigned URL: {str(e)}")
                attachment.presigned_url = None
    
    context = {
        'referral': referral,
        'attachments': attachments,
        'extracted_data': extracted_data,
        'form': form,
    }
    
    return render(request, 'referrals/referral_detail.html', context)

@login_required
def mark_referral_complete(request, referral_id):
    """Mark a referral as complete."""
    if request.method == 'POST':
        referral = get_object_or_404(Referral, id=referral_id)
        
        if referral.status == 'reviewed':
            referral.status = 'completed'
            referral.updated_at = datetime.now()
            referral.save()
            messages.success(request, 'Referral marked as completed.')
        else:
            messages.error(request, 'Referral must be reviewed before it can be completed.')
            
    return redirect('referrals:referral_detail', referral_id=referral_id)

@login_required
def pending_reviews(request):
    """View showing referrals that need review."""
    referrals = Referral.objects.filter(
        status='processing'
    ).filter(
        Q(extracteddata__status='extracted') | 
        Q(extracteddata__status='invalid')
    ).order_by('received_date')
    
    # Paginate results
    paginator = Paginator(referrals, 25)  # 25 referrals per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    context = {
        'page_obj': page_obj,
        'queue_name': 'Pending Reviews',
    }
    
    return render(request, 'referrals/queue_list.html', context)