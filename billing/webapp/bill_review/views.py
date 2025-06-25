# billing/webapp/bill_review/views.py
from django.shortcuts import render, redirect, get_object_or_404
from django.db import connection
from django.urls import reverse
from django.http import HttpResponseRedirect, HttpResponse, Http404
from django.contrib import messages
import logging
from .forms import BillUpdateForm, LineItemUpdateForm, OTARateForm, PPORateForm, BillMappingForm, AddLineItemForm
from .utils import extract_last_name, normalize_date, similar
from django.contrib.auth.decorators import login_required
import boto3
import os
import tempfile
from botocore.exceptions import ClientError
from django.views.decorators.http import require_GET, require_http_methods
import uuid
from datetime import date, timedelta, datetime

logger = logging.getLogger(__name__)

def generate_comparison_data(bill_items, order_items, cpt_categories, ancillary_codes):
    """
    Generate comparison data structure for side-by-side display.
    
    Args:
        bill_items: List of bill line items
        order_items: List of order line items
        cpt_categories: Dict mapping CPT codes to categories
        ancillary_codes: Set of ancillary CPT codes
        
    Returns:
        List of comparison items for template rendering
    """
    comparison_data = []
    processed_bill_items = set()
    processed_order_items = set()
    
    # Create mappings for quick lookup
    bill_cpt_map = {}
    order_cpt_map = {}
    
    for item in bill_items:
        cpt = item.get('cpt_code', '').strip()
        if cpt:
            if cpt not in bill_cpt_map:
                bill_cpt_map[cpt] = []
            bill_cpt_map[cpt].append(item)
    
    for item in order_items:
        cpt = item.get('CPT', '').strip()
        if cpt:
            if cpt not in order_cpt_map:
                order_cpt_map[cpt] = []
            order_cpt_map[cpt].append(item)
    
    # 1. Find exact matches first
    for bill_cpt in bill_cpt_map.keys():
        if bill_cpt in order_cpt_map:
            # Exact match found
            bill_items_for_cpt = bill_cpt_map[bill_cpt]
            order_items_for_cpt = order_cpt_map[bill_cpt]
            
            # Pair them up (if multiple items with same CPT)
            max_items = max(len(bill_items_for_cpt), len(order_items_for_cpt))
            
            for i in range(max_items):
                bill_item = bill_items_for_cpt[i] if i < len(bill_items_for_cpt) else None
                order_item = order_items_for_cpt[i] if i < len(order_items_for_cpt) else None
                
                comparison_data.append({
                    'bill_item': bill_item,
                    'order_item': order_item,
                    'match_type': 'exact',
                    'cpt_code': bill_cpt
                })
                
                if bill_item:
                    processed_bill_items.add(id(bill_item))
                if order_item:
                    processed_order_items.add(id(order_item))
    
    # 2. Find category matches for remaining items
    remaining_bill_items = [item for item in bill_items if id(item) not in processed_bill_items]
    remaining_order_items = [item for item in order_items if id(item) not in processed_order_items]
    
    # Group remaining items by category
    bill_categories = {}
    order_categories = {}
    
    for item in remaining_bill_items:
        cpt = item.get('cpt_code', '').strip()
        if cpt in cpt_categories:
            category = cpt_categories[cpt].get('category', 'Unknown')
            subcategory = cpt_categories[cpt].get('subcategory', '')
            cat_key = f"{category}_{subcategory}"
            
            if cat_key not in bill_categories:
                bill_categories[cat_key] = []
            bill_categories[cat_key].append(item)
    
    for item in remaining_order_items:
        cpt = item.get('CPT', '').strip()
        if cpt in cpt_categories:
            category = cpt_categories[cpt].get('category', 'Unknown')
            subcategory = cpt_categories[cpt].get('subcategory', '')
            cat_key = f"{category}_{subcategory}"
            
            if cat_key not in order_categories:
                order_categories[cat_key] = []
            order_categories[cat_key].append(item)
    
    # Match by category
    for cat_key in bill_categories.keys():
        if cat_key in order_categories:
            bill_items_for_cat = bill_categories[cat_key]
            order_items_for_cat = order_categories[cat_key]
            
            max_items = max(len(bill_items_for_cat), len(order_items_for_cat))
            
            for i in range(max_items):
                bill_item = bill_items_for_cat[i] if i < len(bill_items_for_cat) else None
                order_item = order_items_for_cat[i] if i < len(order_items_for_cat) else None
                
                comparison_data.append({
                    'bill_item': bill_item,
                    'order_item': order_item,
                    'match_type': 'category',
                    'category': cat_key.split('_')[0]
                })
                
                if bill_item:
                    processed_bill_items.add(id(bill_item))
                if order_item:
                    processed_order_items.add(id(order_item))
    
    # 3. Add remaining bill-only items
    remaining_bill_items = [item for item in bill_items if id(item) not in processed_bill_items]
    for item in remaining_bill_items:
        comparison_data.append({
            'bill_item': item,
            'order_item': None,
            'match_type': 'bill_only',
            'cpt_code': item.get('cpt_code', '')
        })
    
    # 4. Add remaining order-only items  
    remaining_order_items = [item for item in order_items if id(item) not in processed_order_items]
    for item in remaining_order_items:
        comparison_data.append({
            'bill_item': None,
            'order_item': item,
            'match_type': 'order_only',
            'cpt_code': item.get('CPT', '')
        })
    
    # Sort comparison data for better display
    # Priority: exact matches first, then category matches, then mismatches
    sort_priority = {'exact': 1, 'category': 2, 'bill_only': 3, 'order_only': 4}
    comparison_data.sort(key=lambda x: (
        sort_priority.get(x['match_type'], 5),
        x.get('cpt_code', ''),
        x.get('category', '')
    ))
    
    return comparison_data

def add_ota_rate(request, bill_id, line_item_id):
    """Add a new OTA rate for a line item."""
    try:
        # Get the line item details
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT bli.cpt_code, bli.modifier, pb.claim_id
                FROM BillLineItem bli
                JOIN ProviderBill pb ON bli.provider_bill_id = pb.id
                WHERE bli.id = %s AND pb.id = %s
            """, [line_item_id, bill_id])
            row = cursor.fetchone()
            if not row:
                messages.error(request, "Line item not found")
                return redirect('bill_review:bill_detail', bill_id=bill_id)
            
            cpt_code, modifier, claim_id = row

        if request.method == 'POST':
            form = OTARateForm(request.POST)
            if form.is_valid():
                try:
                    with connection.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO current_otas (ID_Order_PrimaryKey, CPT, modifier, rate)
                            VALUES (%s, %s, %s, %s)
                        """, [claim_id, cpt_code, modifier, form.cleaned_data['rate']])
                    messages.success(request, "OTA rate added successfully")
                    return HttpResponseRedirect(reverse('bill_review:bill_detail', args=[bill_id]))
                except Exception as e:
                    logger.error(f"Error inserting OTA rate: {e}")
                    messages.error(request, "Failed to add OTA rate")
        else:
            form = OTARateForm(initial={
                'cpt_code': cpt_code,
                'modifier': modifier
            })

        return render(request, 'bill_review/add_ota_rate.html', {
            'form': form,
            'bill_id': bill_id,
            'line_item_id': line_item_id
        })
    except Exception as e:
        logger.error(f"Error in add_ota_rate: {e}")
        messages.error(request, "An error occurred while processing your request")
        return redirect('bill_review:bill_detail', bill_id=bill_id)

def add_ppo_rate(request, bill_id, line_item_id):
    """Add a new PPO rate for a line item."""
    try:
        # Get the line item and provider details
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    bli.cpt_code, 
                    bli.modifier,
                    p.State as RenderingState,
                    p.TIN,
                    p."DBA Name Billing Name" as provider_name
                FROM BillLineItem bli
                JOIN ProviderBill pb ON bli.provider_bill_id = pb.id
                JOIN orders o ON pb.claim_id = o.Order_ID
                JOIN providers p ON o.provider_id = p.PrimaryKey
                WHERE bli.id = %s AND pb.id = %s
            """, [line_item_id, bill_id])
            row = cursor.fetchone()
            if not row:
                messages.error(request, "Line item not found")
                return redirect('bill_review:bill_detail', bill_id=bill_id)
            
            cpt_code, modifier, rendering_state, tin, provider_name = row

        if request.method == 'POST':
            form = PPORateForm(request.POST)
            logger.info(f"Form data: {request.POST}")
            if form.is_valid():
                try:
                    with connection.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO ppo (
                                id, RenderingState, TIN, provider_name, 
                                proc_cd, modifier, proc_desc, proc_category, rate
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, [
                            str(uuid.uuid4()),
                            rendering_state,
                            tin,
                            provider_name,
                            cpt_code,
                            modifier,
                            form.cleaned_data['proc_desc'],
                            form.cleaned_data['proc_category'],
                            form.cleaned_data['rate']
                        ])
                    messages.success(request, "PPO rate added successfully")
                    return HttpResponseRedirect(reverse('bill_review:bill_detail', args=[bill_id]))
                except Exception as e:
                    logger.error(f"Error inserting PPO rate: {e}")
                    messages.error(request, "Failed to add PPO rate")
            else:
                logger.error(f"Form validation errors: {form.errors}")
                messages.error(request, "Please correct the errors below.")
        else:
            form = PPORateForm(initial={
                'cpt_code': cpt_code,
                'modifier': modifier
            })

        return render(request, 'bill_review/add_ppo_rate.html', {
            'form': form,
            'bill_id': bill_id,
            'line_item_id': line_item_id
        })
    except Exception as e:
        logger.error(f"Error in add_ppo_rate: {e}")
        messages.error(request, "An error occurred while processing your request")
        return redirect('bill_review:bill_detail', bill_id=bill_id)

def get_flagged_bills(failure_category=None):
    """Get all flagged bills."""
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT 
                    pb.id,
                    pb.claim_id,
                    pb.patient_name,
                    pb.status,
                    pb.action,
                    pb.last_error,
                    pb.created_at,
                    p."DBA Name Billing Name" as provider_name
                FROM ProviderBill pb
                LEFT JOIN orders o ON pb.claim_id = o.Order_ID
                LEFT JOIN providers p ON o.provider_id = p.PrimaryKey
                WHERE pb.status IN ('FLAGGED', 'REVIEW_FLAG')
            """
            params = []
            
            if failure_category:
                query += " AND pb.action = %s"
                params.append(failure_category)
                
            query += " ORDER BY pb.created_at DESC"
            
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Error retrieving flagged bills: {e}")
        return []

def get_error_bills():
    """Get all error bills."""
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    pb.id,
                    pb.claim_id,
                    pb.patient_name,
                    pb.status,
                    pb.action,
                    pb.last_error,
                    pb.created_at,
                    p."DBA Name Billing Name" as provider_name
                FROM ProviderBill pb
                LEFT JOIN orders o ON pb.claim_id = o.Order_ID
                LEFT JOIN providers p ON o.provider_id = p.PrimaryKey
                WHERE pb.status = 'ERROR'
                ORDER BY pb.created_at DESC
            """)
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Error retrieving error bills: {e}")
        return []

def get_arthrogram_bills():
    """Get all arthrogram bills."""
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    pb.id,
                    pb.claim_id,
                    pb.patient_name,
                    pb.status,
                    pb.action,
                    pb.last_error,
                    pb.created_at,
                    p."DBA Name Billing Name" as provider_name
                FROM ProviderBill pb
                LEFT JOIN orders o ON pb.claim_id = o.Order_ID
                LEFT JOIN providers p ON o.provider_id = p.PrimaryKey
                WHERE pb.status = 'ARTHROGRAM'
                ORDER BY pb.created_at DESC
            """)
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Error retrieving arthrogram bills: {e}")
        return []

def get_bill_details(bill_id):
    """Get details for a specific bill."""
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    pb.*,
                    o.Order_ID,
                    o.bundle_type,
                    o.created_at,
                    o.PatientName,
                    o.Patient_First_Name,
                    o.Patient_Last_Name,
                    o.Patient_DOB,
                    o.Patient_Address,
                    o.Patient_City,
                    o.Patient_State,
                    o.Patient_Zip,
                    o.PatientPhone,
                    o.Order_Type,
                    o.Jurisdiction_State,
                    o.provider_id,
                    o.provider_name,
                    o.BILLS_PAID,
                    o.FULLY_PAID,
                    o.BILLS_REC,
                    p."DBA Name Billing Name" as provider_name,
                    p.TIN,
                    p.NPI,
                    p."Provider Network"
                FROM ProviderBill pb
                LEFT JOIN orders o ON pb.claim_id = o.Order_ID
                LEFT JOIN providers p ON o.provider_id = p.PrimaryKey
                WHERE pb.id = %s
            """, [bill_id])
            columns = [col[0] for col in cursor.description]
            row = cursor.fetchone()
            return dict(zip(columns, row)) if row else None
    except Exception as e:
        logger.error(f"Error retrieving bill details for {bill_id}: {e}")
        return None

def get_bill_line_items(bill_id):
    """Get line items for a specific bill."""
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    bli.*,
                    oli.CPT as order_cpt,
                    oli.modifier as order_modifier,
                    oli.units as order_units,
                    oli.Charge as order_charge,
                    oli.BILL_REVIEWED
                FROM BillLineItem bli
                LEFT JOIN orders o ON bli.provider_bill_id = o.Order_ID
                LEFT JOIN order_line_items oli ON o.Order_ID = oli.Order_ID 
                    AND bli.cpt_code = oli.CPT
                WHERE bli.provider_bill_id = %s
                ORDER BY bli.date_of_service
            """, [bill_id])
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Error retrieving line items for bill {bill_id}: {e}")
        return []

def get_provider_for_bill(bill_id):
    """Get provider information for a bill."""
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    p.PrimaryKey,
                    p."DBA Name Billing Name",
                    p."Billing Name",
                    p."Address Line 1",
                    p."Address Line 2",
                    p.City,
                    p.State,
                    p."Postal Code",
                    p.TIN,
                    p.NPI,
                    p."Provider Network",
                    p."Provider Type",
                    p."Provider Status",
                    p."Billing Address 1",
                    p."Billing Address 2",
                    p."Billing Address City",
                    p."Billing Address State",
                    p."Billing Address Postal Code",
                    p.Phone,
                    p."Fax Number"
                FROM ProviderBill pb
                LEFT JOIN orders o ON pb.claim_id = o.Order_ID
                LEFT JOIN providers p ON o.provider_id = p.PrimaryKey
                WHERE pb.id = %s
            """, [bill_id])
            columns = [col[0] for col in cursor.description]
            row = cursor.fetchone()
            if row:
                provider_data = dict(zip(columns, row))
                # Debug log to see what data we're getting
                logger.info(f"Provider data for bill {bill_id}: {provider_data}")
                return provider_data
            else:
                logger.warning(f"No provider found for bill {bill_id}")
                return None
    except Exception as e:
        logger.error(f"Error retrieving provider for bill {bill_id}: {e}")
        return None

def update_bill_status(bill_id, status, action, last_error):
    """Update bill status, action, and error message."""
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                UPDATE ProviderBill
                SET status = %s, action = %s, last_error = %s
                WHERE id = %s
            """, [status, action, last_error, bill_id])
            
            # If status is REVIEWED, update order line items
            if status == 'REVIEWED':
                # Get the claim_id (Order_ID) for this bill
                cursor.execute("""
                    SELECT claim_id FROM ProviderBill WHERE id = %s
                """, [bill_id])
                row = cursor.fetchone()
                if row and row[0]:
                    order_id = row[0]
                    # Get all CPT codes for this bill
                    cursor.execute("""
                        SELECT cpt_code FROM BillLineItem WHERE provider_bill_id = %s
                    """, [bill_id])
                    cpt_codes = [row[0] for row in cursor.fetchall()]
                    # Update order line items as reviewed
                    if cpt_codes:
                        placeholders = ', '.join(['%s'] * len(cpt_codes))
                        cursor.execute(f"""
                            UPDATE order_line_items
                            SET BILL_REVIEWED = %s
                            WHERE Order_ID = %s AND CPT IN ({placeholders})
                        """, [bill_id, order_id] + cpt_codes)
            
            return True
    except Exception as e:
        logger.error(f"Error updating bill status for {bill_id}: {e}")
        return False

def get_failure_categories():
    """Get all failure categories."""
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT id, name, code, severity, description
                FROM failure_categories
                ORDER BY severity DESC, name ASC
            """)
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Error retrieving failure categories: {e}")
        return []

def get_status_distribution():
    """Get distribution of bill statuses."""
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    status,
                    COUNT(*) as count,
                    MIN(created_at) as first_occurrence,
                    MAX(created_at) as last_occurrence
                FROM ProviderBill
                WHERE status IS NOT NULL
                GROUP BY status
                ORDER BY count DESC
            """)
            statuses = []
            for row in cursor.fetchall():
                status = row[0] or 'No Status'  # Convert None to 'No Status'
                color = 'secondary'  # default color
                if status == 'FLAGGED':
                    color = 'warning'
                elif status == 'ERROR':
                    color = 'danger'
                elif status == 'REVIEWED':
                    color = 'success'
                elif status == 'MAPPED':
                    color = 'info'
                
                statuses.append({
                    'status': status,
                    'count': row[1],
                    'first_occurrence': row[2] or '',  # Convert None to empty string
                    'last_occurrence': row[3] or '',   # Convert None to empty string
                    'color': color,
                    'description': f'Bills with status {status}'
                })
            return statuses
    except Exception as e:
        logger.error(f"Error retrieving status distribution: {e}")
        return []

def get_action_distribution():
    """Get distribution of bill actions."""
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    action,
                    COUNT(*) as count,
                    MIN(created_at) as first_occurrence,
                    MAX(created_at) as last_occurrence
                FROM ProviderBill
                WHERE action IS NOT NULL
                GROUP BY action
                ORDER BY count DESC
            """)
            actions = []
            for row in cursor.fetchall():
                actions.append({
                    'action': row[0] or 'No Action',  # Convert None to 'No Action'
                    'count': row[1],
                    'first_occurrence': row[2] or '',  # Convert None to empty string
                    'last_occurrence': row[3] or ''    # Convert None to empty string
                })
            return actions
    except Exception as e:
        logger.error(f"Error retrieving action distribution: {e}")
        return []

def get_filtered_bills(status=None, action=None):
    """Get bills filtered by status and/or action."""
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT 
                    pb.id,
                    pb.claim_id,
                    pb.patient_name,
                    pb.status,
                    pb.action,
                    pb.last_error,
                    pb.created_at,
                    p."DBA Name Billing Name" as provider_name
                FROM ProviderBill pb
                LEFT JOIN orders o ON pb.claim_id = o.Order_ID
                LEFT JOIN providers p ON o.provider_id = p.PrimaryKey
                WHERE 1=1
            """
            params = []
            
            if status:
                query += " AND pb.status = %s"
                params.append(status)
            
            if action:
                query += " AND pb.action = %s"
                params.append(action)
                
            query += " ORDER BY pb.created_at DESC"
            
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            bills = []
            for row in cursor.fetchall():
                bill = dict(zip(columns, row))
                # Convert None values to empty strings or appropriate defaults
                bill['status'] = bill['status'] or 'No Status'
                bill['action'] = bill['action'] or 'No Action'
                bill['last_error'] = bill['last_error'] or ''
                bill['provider_name'] = bill['provider_name'] or 'Unknown Provider'
                bills.append(bill)
            return bills
    except Exception as e:
        logger.error(f"Error retrieving filtered bills: {e}")
        return []

def dashboard(request):
    """View for the bill review dashboard."""
    try:
        # Get filter parameters
        status = request.GET.get('status')
        action = request.GET.get('action')
        
        # Get filtered bills
        bills = get_filtered_bills(status, action)
        
        # Get status and action distributions
        status_distribution = get_status_distribution()
        action_distribution = get_action_distribution()
        
        # Get unique statuses and actions for filter dropdowns
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT status 
                FROM ProviderBill 
                WHERE status IS NOT NULL 
                ORDER BY status
            """)
            statuses = [row[0] for row in cursor.fetchall()]
            
            cursor.execute("""
                SELECT DISTINCT action 
                FROM ProviderBill 
                WHERE action IS NOT NULL 
                ORDER BY action
            """)
            actions = [row[0] for row in cursor.fetchall()]
        
        context = {
            'bills': bills,
            'status_distribution': status_distribution,
            'action_distribution': action_distribution,
            'statuses': statuses,
            'actions': actions,
        }
        
        return render(request, 'bill_review/dashboard.html', context)
    except Exception as e:
        logger.error(f"Error in dashboard view: {e}")
        messages.error(request, "An error occurred while loading the dashboard.")
        return render(request, 'bill_review/dashboard.html', {})

def normalize_date(date_str):
    if not date_str:
        return None
    try:
        # Handle date range format (e.g., "04/04/2025-04/04/2025")
        if '-' in date_str:
            date_str = date_str.split('-')[0].strip()
        
        # Try different date formats
        for fmt in ['%m/%d/%Y', '%m/%d/%y', '%Y-%m-%d']:
            try:
                return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
        return None
    except Exception:
        return None

def bill_detail(request, bill_id):
    """Show comprehensive details for a bill with all data needed for manual review."""
    try:
        # Add debug logging at the start
        logger.info(f"bill_detail called for {bill_id}, method: {request.method}")
        if request.method == 'POST':
            logger.info(f"POST data keys: {list(request.POST.keys())}")
            logger.info(f"Full POST data: {dict(request.POST)}")

        # Handle POST requests first (before building context)
        if request.method == 'POST':
            logger.info("Processing POST request")
            
            # Handle bill mapping search
            if 'search_orders' in request.POST:
                logger.info("Processing search_orders POST")
                mapping_form = BillMappingForm(request.POST)
                
                if mapping_form.is_valid():
                    logger.info("Mapping form is valid")
                    logger.info(f"Form cleaned data: {mapping_form.cleaned_data}")
                    
                    try:
                        # Create connection and cursor inside the try block
                        with connection.cursor() as search_cursor:
                            # Get search parameters from form
                            patient_last_name = mapping_form.cleaned_data['patient_last_name'].strip()
                            patient_first_name = mapping_form.cleaned_data.get('patient_first_name', '').strip()
                            date_from = mapping_form.cleaned_data.get('date_from')
                            date_to = mapping_form.cleaned_data.get('date_to')
                            
                            # Build the query based on available date parameters
                            query = """
                                SELECT DISTINCT 
                                    o.Order_ID,
                                    o.Patient_Last_Name,
                                    o.Patient_First_Name,
                                    o.Patient_DOB,
                                    MIN(oli.DOS) as earliest_dos,
                                    MAX(oli.DOS) as latest_dos,
                                    COUNT(oli.CPT) as cpt_count,
                                    GROUP_CONCAT(DISTINCT oli.CPT) as cpt_codes,
                                    MIN(ABS(julianday(oli.DOS) - julianday(%s))) as min_date_diff
                                FROM orders o
                                JOIN order_line_items oli ON o.Order_ID = oli.Order_ID
                                WHERE o.Patient_Last_Name LIKE %s
                            """
                            params = [date.today().strftime('%Y-%m-%d'), f'%{patient_last_name}%']
                            
                            # Add first name condition if provided
                            if patient_first_name:
                                query += " AND o.Patient_First_Name LIKE %s"
                                params.append(f'%{patient_first_name}%')
                                logger.info(f"Adding first name search: {patient_first_name}")
                            
                            # Handle date range logic
                            if date_from and date_to:
                                # Both dates provided - use exact range
                                query += " AND oli.DOS >= %s AND oli.DOS <= %s"
                                params.extend([date_from.strftime('%Y-%m-%d'), date_to.strftime('%Y-%m-%d')])
                                logger.info(f"Using exact date range: {date_from} to {date_to}")
                            elif date_from:
                                # Only start date - search forward 30 days
                                query += " AND oli.DOS >= %s AND oli.DOS <= %s"
                                end_date = date_from + timedelta(days=30)
                                params.extend([date_from.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')])
                                logger.info(f"Using forward date range from {date_from} to {end_date}")
                            elif date_to:
                                # Only end date - search backward 30 days
                                query += " AND oli.DOS >= %s AND oli.DOS <= %s"
                                start_date = date_to - timedelta(days=30)
                                params.extend([start_date.strftime('%Y-%m-%d'), date_to.strftime('%Y-%m-%d')])
                                logger.info(f"Using backward date range from {start_date} to {date_to}")
                            else:
                                # No dates provided - no date restrictions
                                logger.info("No date restrictions applied")
                            
                            query += """
                                GROUP BY o.Order_ID, o.Patient_Last_Name, o.Patient_First_Name, o.Patient_DOB
                                ORDER BY 
                                    CASE 
                                        WHEN min_date_diff IS NOT NULL THEN min_date_diff 
                                        ELSE 999999 
                                    END,
                                    o.Patient_Last_Name, 
                                    o.Patient_First_Name
                                LIMIT 20
                            """
                            
                            logger.info(f"Executing search query with params: {params}")
                            search_cursor.execute(query, params)

                            search_results = []
                            for row in search_cursor.fetchall():
                                search_results.append({
                                    'order_id': row[0],
                                    'patient_last_name': row[1],
                                    'patient_first_name': row[2],
                                    'patient_dob': row[3],
                                    'earliest_dos': row[4],
                                    'latest_dos': row[5],
                                    'cpt_count': row[6],
                                    'cpt_codes': row[7].split(',') if row[7] else [],
                                    'days_difference': f"{row[8]} days" if row[8] is not None else "Unknown"
                                })

                            logger.info(f"Found {len(search_results)} potential matches")
                            if not search_results:
                                logger.info("No matching orders found")
                                messages.info(request, 'No matching orders found. Try adjusting the search criteria.')
                            
                    except Exception as e:
                        logger.error(f"Error searching for matching orders for bill {bill_id}: {str(e)}")
                        messages.error(request, 'An error occurred while searching for matching orders.')
                        search_results = []
                else:
                    logger.error(f"Mapping form validation errors: {mapping_form.errors}")
                    search_results = []
            
            # Handle regular bill update
            elif 'status' in request.POST:
                logger.info("Processing regular bill update POST")
                form = BillUpdateForm(request.POST)
                if form.is_valid():
                    try:
                        with connection.cursor() as cursor:
                            cursor.execute("""
                                UPDATE ProviderBill
                                SET status = %s,
                                    action = %s,
                                    last_error = %s
                                WHERE id = %s
                            """, [
                                form.cleaned_data['status'],
                                form.cleaned_data['action'],
                                form.cleaned_data['last_error'],
                                bill_id
                            ])
                        messages.success(request, 'Bill updated successfully.')
                    except Exception as e:
                        logger.error(f"Error updating bill {bill_id}: {e}")
                        messages.error(request, 'Failed to update bill.')
                else:
                    logger.error(f"Bill update form validation errors: {form.errors}")
                    messages.error(request, 'Please correct the errors below.')

        # Load all bill data using patterns similar to process/utils/loader.py
        with connection.cursor() as cursor:
            # Get bill details with provider info
            cursor.execute("""
                SELECT 
                    pb.*,
                    p.PrimaryKey as provider_id,
                    p."DBA Name Billing Name" as provider_dba_name,
                    p."Billing Name" as provider_billing_name,
                    p."Address Line 1" as provider_address1,
                    p."Address Line 2" as provider_address2,
                    p.City as provider_city,
                    p.State as provider_state,
                    p."Postal Code" as provider_postal_code,
                    p.TIN as provider_tin,
                    p.NPI as provider_npi,
                    p."Provider Network" as provider_network,
                    p."Provider Type" as provider_type,
                    p."Provider Status" as provider_status,
                    p."Billing Address 1" as provider_billing_address1,
                    p."Billing Address 2" as provider_billing_address2,
                    p."Billing Address City" as provider_billing_city,
                    p."Billing Address State" as provider_billing_state,
                    p."Billing Address Postal Code" as provider_billing_postal_code,
                    p.Phone as provider_phone,
                    p."Fax Number" as provider_fax
                FROM ProviderBill pb
                LEFT JOIN orders o ON pb.claim_id = o.Order_ID
                LEFT JOIN providers p ON o.provider_id = p.PrimaryKey
                WHERE pb.id = %s
            """, [bill_id])
            columns = [col[0] for col in cursor.description]
            bill_row = cursor.fetchone()
            
            if not bill_row:
                messages.error(request, 'Bill not found.')
                return redirect('bill_review:dashboard')
                
            bill = dict(zip(columns, bill_row))
            logger.info(f"Retrieved bill data: {bill}")
            logger.info(f"Bill status: {bill.get('status')}")
            
            # Extract provider data from bill
            provider = None
            if bill.get('provider_id'):
                provider = {
                    'PrimaryKey': bill.get('provider_id'),
                    'DBA_Name_Billing_Name': bill.get('provider_dba_name'),
                    'Billing_Name': bill.get('provider_billing_name'),
                    'Address_Line_1': bill.get('provider_address1'),
                    'Address_Line_2': bill.get('provider_address2'),
                    'City': bill.get('provider_city'),
                    'State': bill.get('provider_state'),
                    'Postal_Code': bill.get('provider_postal_code'),
                    'TIN': bill.get('provider_tin'),
                    'NPI': bill.get('provider_npi'),
                    'Provider_Network': bill.get('provider_network'),
                    'Provider_Type': bill.get('provider_type'),
                    'Provider_Status': bill.get('provider_status'),
                    'Billing_Address_1': bill.get('provider_billing_address1'),
                    'Billing_Address_2': bill.get('provider_billing_address2'),
                    'Billing_Address_City': bill.get('provider_billing_city'),
                    'Billing_Address_State': bill.get('provider_billing_state'),
                    'Billing_Address_Postal_Code': bill.get('provider_billing_postal_code'),
                    'Phone': bill.get('provider_phone'),
                    'Fax_Number': bill.get('provider_fax')
                }
                logger.info(f"Provider data: {provider}")
            
            # Initialize form with current bill status
            form = BillUpdateForm(initial={
                'status': bill.get('status'),
                'action': bill.get('action'),
                'last_error': bill.get('last_error')
            })
            logger.info(f"Form initial data: {form.initial}")
            
            # Get bill line items
            cursor.execute("""
                SELECT bli.* 
                FROM BillLineItem bli
                WHERE bli.provider_bill_id = %s
                ORDER BY bli.date_of_service, bli.cpt_code
            """, [bill_id])
            columns = [col[0] for col in cursor.description]
            bill_items = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            # Normalize dates for each line item
            for item in bill_items:
                if 'date_of_service' in item:
                    item['date_of_service'] = normalize_date(item['date_of_service'])
            
            # Get order details if claim_id exists
            order = {}
            order_items = []
            
            if bill.get('claim_id'):
                # Get order details
                cursor.execute("""
                    SELECT o.*
                    FROM orders o
                    WHERE o.Order_ID = %s
                """, [bill.get('claim_id')])
                columns = [col[0] for col in cursor.description]
                order_row = cursor.fetchone()
                
                if order_row:
                    order = dict(zip(columns, order_row))
                    
                    # Get order line items
                    cursor.execute("""
                        SELECT oli.*
                        FROM order_line_items oli
                        WHERE oli.Order_ID = %s
                        ORDER BY oli.line_number
                    """, [bill.get('claim_id')])
                    columns = [col[0] for col in cursor.description]
                    order_items = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            # Normalize dates for each order line item
            for item in order_items:
                if 'date_of_service' in item:
                    item['date_of_service'] = normalize_date(item['date_of_service'])
            
            # Get CPT code categories for comparisons
            cpt_codes = [item['cpt_code'] for item in bill_items if item.get('cpt_code')]
            ordered_cpts = [item['CPT'] for item in order_items if item.get('CPT')]
            all_cpts = list(set(cpt_codes + ordered_cpts))
            
            if all_cpts:
                placeholders = ', '.join(['%s'] * len(all_cpts))
                cursor.execute(f"""
                    SELECT proc_cd, category, subcategory
                    FROM dim_proc
                    WHERE proc_cd IN ({placeholders})
                """, all_cpts)
                
                cpt_categories = {}
                for row in cursor.fetchall():
                    cpt_categories[row[0]] = {
                        'category': row[1],
                        'subcategory': row[2]
                    }
            else:
                cpt_categories = {}
            
            # Check if this is an arthrogram if we have an order
            is_arthrogram = False
            if order and order.get('bundle_type'):
                # Check order bundle type
                bundle_type = order.get('bundle_type', '').lower() 
                is_arthrogram = bundle_type == 'arthrogram'
                
                # If not found in bundle_type, check CPT codes
                if not is_arthrogram:
                    arthrogram_cpts = {'20610', '20611', '77002', '77003', '77021'}
                    for item in order_items:
                        cpt = item.get('CPT', '').strip()
                        if cpt in arthrogram_cpts:
                            is_arthrogram = True
                            break
            
            # Get in-network rates for bill CPT codes if we have a provider with a TIN
            in_network_rates = {}
            if provider and provider.get('TIN'):
                tin_clean = provider.get('TIN', '').replace('-', '').replace(' ', '').strip()
                
                for item in bill_items:
                    cpt = item.get('cpt_code', '').strip()
                    modifier = item.get('modifier', '').strip()
                    
                    # Only include modifier if it's TC or 26
                    effective_modifier = None
                    if modifier:
                        if 'TC' in modifier.split(','):
                            effective_modifier = 'TC'
                        elif '26' in modifier.split(','):
                            effective_modifier = '26'
                    
                    if cpt:
                        if effective_modifier:
                            cursor.execute("""
                                SELECT rate
                                FROM ppo
                                WHERE REPLACE(REPLACE(TIN, '-', ''), ' ', '') = %s
                                AND proc_cd = %s
                                AND modifier = %s
                                LIMIT 1
                            """, [tin_clean, cpt, effective_modifier])
                        else:
                            cursor.execute("""
                                SELECT rate
                                FROM ppo
                                WHERE REPLACE(REPLACE(TIN, '-', ''), ' ', '') = %s
                                AND proc_cd = %s
                                AND (modifier IS NULL OR modifier = '')
                                LIMIT 1
                            """, [tin_clean, cpt])
                        
                        row = cursor.fetchone()
                        if row and row[0]:
                            in_network_rates[cpt] = float(row[0])
            
            # Get out-of-network rates if we have an order
            out_network_rates = {}
            if order and bill.get('claim_id'):
                for item in bill_items:
                    cpt = item.get('cpt_code', '').strip()
                    modifier = item.get('modifier', '').strip()
                    
                    # Only include modifier if it's TC or 26
                    effective_modifier = None
                    if modifier:
                        if 'TC' in modifier.split(','):
                            effective_modifier = 'TC'
                        elif '26' in modifier.split(','):
                            effective_modifier = '26'
                    
                    if cpt:
                        if effective_modifier:
                            cursor.execute("""
                                SELECT rate
                                FROM ota
                                WHERE ID_Order_PrimaryKey = %s
                                AND CPT = %s
                                AND modifier = %s
                                LIMIT 1
                            """, [bill.get('claim_id'), cpt, effective_modifier])
                        else:
                            cursor.execute("""
                                SELECT rate
                                FROM ota
                                WHERE ID_Order_PrimaryKey = %s
                                AND CPT = %s
                                AND (modifier IS NULL OR modifier = '')
                                LIMIT 1
                            """, [bill.get('claim_id'), cpt])
                        
                        row = cursor.fetchone()
                        if row and row[0]:
                            out_network_rates[cpt] = float(row[0])
            
            # Load list of ancillary codes
            cursor.execute("""
                SELECT proc_cd
                FROM dim_proc
                WHERE category = 'ancillary'
            """)
            ancillary_codes = {row[0] for row in cursor.fetchall()}
            
            # Perform CPT code comparison
            billed_cpts = {item['cpt_code'] for item in bill_items if item.get('cpt_code')}
            ordered_cpts = {item['CPT'] for item in order_items if item.get('CPT')}
            
            exact_matches = list(billed_cpts.intersection(ordered_cpts))
            billed_not_ordered = [cpt for cpt in list(billed_cpts - ordered_cpts) 
                                 if cpt not in ancillary_codes]
            ordered_not_billed = [cpt for cpt in list(ordered_cpts - billed_cpts)
                                 if cpt not in ancillary_codes]
            
            # Check for units violations
            units_violations = []
            for item in bill_items:
                cpt = item.get('cpt_code', '').strip()
                units = int(item.get('units', 1))
                
                if cpt and cpt not in ancillary_codes and units > 1:
                    units_violations.append({
                        'cpt': cpt,
                        'units': units,
                        'line_id': item.get('id')
                    })
            
            # Generate comparison data for side-by-side view
            comparison_data = generate_comparison_data(
                bill_items=bill_items,
                order_items=order_items,
                cpt_categories=cpt_categories,
                ancillary_codes=ancillary_codes
            )
            
            # Initialize mapping form with appropriate defaults
            logger.info(f"Setting up mapping form for bill {bill_id}")
            
            # Extract patient last name from bill
            patient_full_name = bill.get('patient_name', '')
            patient_last_name = extract_last_name(patient_full_name)
            
            # Get single date from bill line items for initial population
            target_date = None
            if bill_items:
                dates = []
                for item in bill_items:
                    if item.get('date_of_service'):
                        normalized = normalize_date(item['date_of_service'])
                        if normalized:
                            dates.append(normalized)
                if dates:
                    target_date = min(dates)  # Use earliest date as target

            # Set default target date
            if not target_date:
                target_date = date.today() - timedelta(days=30)  # Default to 30 days ago

            # Initialize form with date range
            initial_data = {
                'patient_last_name': patient_last_name,
                'date_from': target_date - timedelta(days=30),
                'date_to': target_date + timedelta(days=30)
            }

            # Handle form submission
            if request.method == 'POST' and 'patient_last_name' in request.POST:
                mapping_form = BillMappingForm(request.POST)
            else:
                mapping_form = BillMappingForm(initial=initial_data)

            # Perform search with date range
            search_results = []
            if mapping_form.is_valid():
                search_data = mapping_form.cleaned_data
                search_last_name = search_data['patient_last_name'].strip()
                search_first_name = search_data.get('patient_first_name', '').strip()
                date_from = search_data.get('date_from')
                date_to = search_data.get('date_to')
                
                if search_last_name:
                    try:
                        with connection.cursor() as search_cursor:
                            logger.info(f"Searching for: '{search_last_name}' with date range: {date_from} to {date_to}")
                            
                            # Build the query based on available date parameters
                            query = """
                                SELECT DISTINCT 
                                    o.Order_ID,
                                    o.Patient_Last_Name,
                                    o.Patient_First_Name,
                                    o.Patient_DOB,
                                    MIN(oli.DOS) as earliest_dos,
                                    MAX(oli.DOS) as latest_dos,
                                    COUNT(oli.CPT) as cpt_count,
                                    GROUP_CONCAT(DISTINCT oli.CPT) as cpt_codes,
                                    MIN(ABS(julianday(oli.DOS) - julianday(%s))) as min_date_diff
                                FROM orders o
                                JOIN order_line_items oli ON o.Order_ID = oli.Order_ID
                                WHERE o.Patient_Last_Name LIKE %s
                            """
                            params = [target_date.strftime('%Y-%m-%d'), f'%{search_last_name}%']
                            
                            # Add first name condition if provided
                            if search_first_name:
                                query += " AND o.Patient_First_Name LIKE %s"
                                params.append(f'%{search_first_name}%')
                                logger.info(f"Adding first name search: {search_first_name}")
                            
                            # Handle date range logic
                            if date_from and date_to:
                                # Both dates provided - use exact range
                                query += " AND oli.DOS >= %s AND oli.DOS <= %s"
                                params.extend([date_from.strftime('%Y-%m-%d'), date_to.strftime('%Y-%m-%d')])
                                logger.info(f"Using exact date range: {date_from} to {date_to}")
                            elif date_from:
                                # Only start date - search forward 30 days
                                query += " AND oli.DOS >= %s AND oli.DOS <= %s"
                                end_date = date_from + timedelta(days=30)
                                params.extend([date_from.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')])
                                logger.info(f"Using forward date range from {date_from} to {end_date}")
                            elif date_to:
                                # Only end date - search backward 30 days
                                query += " AND oli.DOS >= %s AND oli.DOS <= %s"
                                start_date = date_to - timedelta(days=30)
                                params.extend([start_date.strftime('%Y-%m-%d'), date_to.strftime('%Y-%m-%d')])
                                logger.info(f"Using backward date range from {start_date} to {date_to}")
                            else:
                                # No dates provided - no date restrictions
                                logger.info("No date restrictions applied")
                            
                            query += """
                                GROUP BY o.Order_ID, o.Patient_Last_Name, o.Patient_First_Name, o.Patient_DOB
                                ORDER BY 
                                    CASE 
                                        WHEN min_date_diff IS NOT NULL THEN min_date_diff 
                                        ELSE 999999 
                                    END,
                                    o.Patient_Last_Name, 
                                    o.Patient_First_Name
                                LIMIT 20
                            """
                            
                            logger.info(f"Executing search query with params: {params}")
                            search_cursor.execute(query, params)
                            
                            results = search_cursor.fetchall()
                            logger.info(f"Found {len(results)} potential matches")
                            
                            # Format results for template
                            for row in results:
                                search_results.append({
                                    'order_id': row[0],
                                    'patient_last_name': row[1],
                                    'patient_first_name': row[2],
                                    'patient_dob': row[3],
                                    'earliest_dos': row[4],
                                    'latest_dos': row[5],
                                    'cpt_count': row[6],
                                    'cpt_codes': row[7].split(',') if row[7] else [],
                                    'days_difference': f"{row[8]} days" if row[8] is not None else "Unknown"
                                })
                                
                    except Exception as e:
                        logger.error(f"Error searching for orders: {str(e)}")
                        search_results = []
            
            # Initialize context dictionary
            context = {
                'bill': bill,
                'bill_items': bill_items,
                'order': order,
                'order_items': order_items,
                'provider': provider,
                'form': form,
                'is_arthrogram': is_arthrogram,
                'cpt_categories': cpt_categories,
                'in_network_rates': in_network_rates,
                'out_network_rates': out_network_rates,
                'exact_matches': exact_matches,
                'billed_not_ordered': billed_not_ordered,
                'ordered_not_billed': ordered_not_billed,
                'units_violations': units_violations,
                'ancillary_codes': ancillary_codes,
                'provider_network': provider.get('Provider_Network') if provider else None,
                'mapping_form': mapping_form,
                'search_results': search_results,
                'auto_searched': True,  # Flag to show results were auto-generated
                'comparison_data': comparison_data,
                'add_line_item_form': AddLineItemForm(),
            }
            
            return render(request, 'bill_review/bill_detail.html', context)
            
    except Exception as e:
        logger.error(f"Error in bill_detail: {e}")
        messages.error(request, "An error occurred while loading the bill details.")
        return redirect('bill_review:dashboard')

def line_item_update(request, line_item_id):
    """Update a specific line item."""
    if request.method == 'POST':
        form = LineItemUpdateForm(request.POST)
        if form.is_valid():
            try:
                with connection.cursor() as cursor:
                    # Get the bill_id for redirect
                    cursor.execute("""
                        SELECT provider_bill_id 
                        FROM BillLineItem 
                        WHERE id = %s
                    """, [line_item_id])
                    row = cursor.fetchone()
                    if not row:
                        messages.error(request, 'Line item not found.')
                        return HttpResponseRedirect(reverse('bill_review:dashboard'))
                    
                    bill_id = row[0]
                    
                    # Update the line item
                    cursor.execute("""
                        UPDATE BillLineItem
                        SET cpt_code = %s,
                            modifier = %s,
                            units = %s,
                            charge_amount = %s,
                            allowed_amount = %s,
                            decision = %s,
                            reason_code = %s,
                            date_of_service = %s
                        WHERE id = %s
                    """, [
                        form.cleaned_data['cpt_code'],
                        form.cleaned_data['modifier'],
                        form.cleaned_data['units'],
                        form.cleaned_data['charge_amount'],
                        form.cleaned_data['allowed_amount'],
                        form.cleaned_data['decision'],
                        form.cleaned_data['reason_code'],
                        form.cleaned_data['date_of_service'],
                        line_item_id
                    ])
                
                messages.success(request, 'Line item updated successfully.')
                return HttpResponseRedirect(reverse('bill_review:bill_detail', args=[bill_id]))
            except Exception as e:
                logger.error(f"Error updating line item {line_item_id}: {e}")
                messages.error(request, 'Failed to update line item.')
    
    return HttpResponseRedirect(reverse('bill_review:dashboard'))

def reset_bill(request, bill_id):
    """Reset a bill to MAPPED status for reprocessing."""
    if request.method == 'POST':
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    UPDATE ProviderBill
                    SET status = 'MAPPED', action = NULL, last_error = NULL
                    WHERE id = %s
                """, [bill_id])
            messages.success(request, 'Bill has been reset to MAPPED status.')
        except Exception as e:
            logger.error(f"Error resetting bill {bill_id}: {e}")
            messages.error(request, 'Failed to reset bill.')
    
    # Redirect back to bill detail page instead of dashboard
    return redirect('bill_review:bill_detail', bill_id=bill_id)

def update_provider(request, provider_id, bill_id):
    if request.method == 'POST':
        try:
            with connection.cursor() as cursor:
                # Update provider information
                cursor.execute("""
                    UPDATE providers 
                    SET "DBA Name Billing Name" = %s,
                        "Billing Name" = %s,
                        "Address Line 1" = %s,
                        "Address Line 2" = %s,
                        City = %s,
                        State = %s,
                        "Postal Code" = %s,
                        TIN = %s,
                        NPI = %s,
                        "Provider Network" = %s,
                        "Billing Address 1" = %s,
                        "Billing Address 2" = %s,
                        "Billing Address City" = %s,
                        "Billing Address State" = %s,
                        "Billing Address Postal Code" = %s,
                        Phone = %s,
                        "Fax Number" = %s
                    WHERE PrimaryKey = %s
                """, [
                    request.POST.get('dba_name'),
                    request.POST.get('billing_name'),
                    request.POST.get('address1'),
                    request.POST.get('address2'),
                    request.POST.get('city'),
                    request.POST.get('state'),
                    request.POST.get('postal_code'),
                    request.POST.get('tin'),
                    request.POST.get('npi'),
                    request.POST.get('network'),
                    request.POST.get('billing_address1'),
                    request.POST.get('billing_address2'),
                    request.POST.get('billing_city'),
                    request.POST.get('billing_state'),
                    request.POST.get('billing_postal_code'),
                    request.POST.get('phone'),
                    request.POST.get('fax'),
                    provider_id
                ])
                
                # Reset the bill status to MAPPED and clear action
                cursor.execute("""
                    UPDATE ProviderBill
                    SET status = 'MAPPED',
                        action = NULL,
                        last_error = NULL
                    WHERE id = %s
                """, [bill_id])
                
                messages.success(request, 'Provider information updated and bill reset to MAPPED status.')
        except Exception as e:
            print(f"ERROR: Exception in update_provider: {str(e)}")
            print(f"ERROR: Exception type: {type(e)}")
            import traceback
            print(f"ERROR: Traceback: {traceback.format_exc()}")
            messages.error(request, 'Failed to update provider information.')
    
    # Redirect to dashboard with update_prov_info filter
    return redirect('bill_review:dashboard')

def update_bill(request, bill_id):
    """Update bill status and information."""
    if request.method == 'POST':
        form = BillUpdateForm(request.POST)
        if form.is_valid():
            try:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        UPDATE ProviderBill
                        SET status = %s,
                            action = %s,
                            last_error = %s
                        WHERE id = %s
                    """, [
                        form.cleaned_data['status'],
                        form.cleaned_data['action'],
                        form.cleaned_data['last_error'],
                        bill_id
                    ])
                messages.success(request, 'Bill updated successfully.')
            except Exception as e:
                logger.error(f"Error updating bill {bill_id}: {e}")
                messages.error(request, 'Failed to update bill.')
        else:
            logger.error(f"Form validation errors: {form.errors}")
            messages.error(request, 'Please correct the errors below.')
            for field, errors in form.errors.items():
                for error in errors:
                    messages.error(request, f"{field}: {error}")
    
    # Redirect to dashboard
    return redirect('bill_review:dashboard')

@require_GET
def view_bill_pdf(request, bill_id):
    """Generate a pre-signed URL for the bill PDF in S3."""
    try:
        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-2')
        )
        
        # Define S3 bucket
        bucket_name = os.environ.get('S3_BUCKET', 'bill-review-prod')
        
        logger.info(f"Starting PDF lookup for bill {bill_id} in bucket {bucket_name}")
        
        # Get the order_id for this bill
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT claim_id FROM ProviderBill WHERE id = %s
            """, [bill_id])
            row = cursor.fetchone()
            order_id = row[0] if row else None
            logger.info(f"Bill {bill_id} has order_id: {order_id}")
        
        # First, try to find the PDF by searching the entire bucket
        logger.info(f"Searching for PDF with bill_id {bill_id} anywhere in bucket")
        
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket_name, Prefix=''):
                if 'Contents' not in page:
                    continue
                for obj in page['Contents']:
                    key = obj['Key']
                    if bill_id in key and key.lower().endswith('.pdf'):
                        logger.info(f"Found potential PDF: {key}")
                        pdf_key = key
                        url = s3_client.generate_presigned_url(
                            'get_object',
                            Params={
                                'Bucket': bucket_name,
                                'Key': pdf_key,
                                'ResponseContentType': 'application/pdf'
                            },
                            ExpiresIn=3600  # URL expires in 1 hour
                        )
                        logger.info(f"Successfully generated pre-signed URL for: {pdf_key}")
                        return HttpResponseRedirect(url)
            
            # If no match found with bill_id, try with order_id if available
            if order_id:
                logger.info(f"Searching for PDF with order_id {order_id} anywhere in bucket")
                
                for page in paginator.paginate(Bucket=bucket_name, Prefix=''):
                    if 'Contents' not in page:
                        continue
                    for obj in page['Contents']:
                        key = obj['Key']
                        if order_id in key and key.lower().endswith('.pdf'):
                            logger.info(f"Found potential PDF with order_id: {key}")
                            pdf_key = key
                            url = s3_client.generate_presigned_url(
                                'get_object',
                                Params={
                                    'Bucket': bucket_name,
                                    'Key': pdf_key,
                                    'ResponseContentType': 'application/pdf'
                                },
                                ExpiresIn=3600
                            )
                            logger.info(f"Successfully generated pre-signed URL for: {pdf_key}")
                            return HttpResponseRedirect(url)
                        
        except Exception as e:
            logger.error(f"Error searching bucket for PDF: {str(e)}")
        
        # If we get here, none of the searches worked
        logger.error(f"Failed to find PDF for bill {bill_id} in bucket {bucket_name}")
        raise Http404(f"PDF for bill {bill_id} not found in S3 bucket {bucket_name}")
        
    except Exception as e:
        logger.exception(f"Error generating pre-signed URL for bill {bill_id}: {str(e)}")
        raise Http404(f"Error retrieving PDF: {str(e)}")

def line_item_delete(request, line_item_id):
    """Delete a specific line item."""
    if request.method == 'POST':
        try:
            with connection.cursor() as cursor:
                # Get the bill_id for redirect
                cursor.execute("""
                    SELECT provider_bill_id 
                    FROM BillLineItem 
                    WHERE id = %s
                """, [line_item_id])
                row = cursor.fetchone()
                if not row:
                    messages.error(request, 'Line item not found.')
                    return HttpResponseRedirect(reverse('bill_review:dashboard'))
                
                bill_id = row[0]
                
                # Delete the line item
                cursor.execute("""
                    DELETE FROM BillLineItem
                    WHERE id = %s
                """, [line_item_id])
            
            messages.success(request, 'Line item deleted successfully.')
            return HttpResponseRedirect(reverse('bill_review:bill_detail', args=[bill_id]))
        except Exception as e:
            logger.error(f"Error deleting line item {line_item_id}: {e}")
            messages.error(request, 'Failed to delete line item.')
    
    return HttpResponseRedirect(reverse('bill_review:dashboard'))

@require_http_methods(['POST'])
def map_bill_to_order(request, bill_id, order_id):
    """Map an unmapped bill to an order."""
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                UPDATE ProviderBill
                SET claim_id = %s,
                    status = 'MAPPED',
                    action = NULL,
                    last_error = NULL
                WHERE id = %s
            """, [order_id, bill_id])
            
            messages.success(request, f'Bill successfully mapped to order {order_id}')
            return redirect('bill_review:bill_detail', bill_id=bill_id)
            
    except Exception as e:
        logger.error(f"Error mapping bill {bill_id} to order {order_id}: {e}")
        messages.error(request, 'Failed to map bill to order')
        return redirect('bill_review:bill_detail', bill_id=bill_id)

def instructions(request):
    """Display process instructions for different bill statuses."""
    return render(request, 'bill_review/instructions.html')

def add_line_item(request, bill_id):
    """Add a new line item to a bill."""
    if request.method == 'POST':
        form = AddLineItemForm(request.POST)
        if form.is_valid():
            try:
                with connection.cursor() as cursor:
                    # Verify the bill exists
                    cursor.execute("""
                        SELECT id FROM ProviderBill WHERE id = %s
                    """, [bill_id])
                    if not cursor.fetchone():
                        messages.error(request, 'Bill not found.')
                        return redirect('bill_review:dashboard')
                    
                    # Insert the new line item
                    cursor.execute("""
                        INSERT INTO BillLineItem (
                            provider_bill_id, cpt_code, modifier, units, 
                            charge_amount, allowed_amount, decision, 
                            reason_code, date_of_service
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, [
                        bill_id,
                        form.cleaned_data['cpt_code'],
                        form.cleaned_data['modifier'],
                        form.cleaned_data['units'],
                        form.cleaned_data['charge_amount'],
                        form.cleaned_data['allowed_amount'],
                        form.cleaned_data['decision'],
                        form.cleaned_data['reason_code'],
                        form.cleaned_data['date_of_service']
                    ])
                
                messages.success(request, 'Line item added successfully.')
                return redirect('bill_review:bill_detail', bill_id=bill_id)
            except Exception as e:
                logger.error(f"Error adding line item to bill {bill_id}: {e}")
                messages.error(request, 'Failed to add line item.')
        else:
            # Form validation errors
            for field, errors in form.errors.items():
                for error in errors:
                    messages.error(request, f"{field}: {error}")
    
    # If GET request or form errors, redirect back to bill detail
    return redirect('bill_review:bill_detail', bill_id=bill_id)

def debug_s3_bucket(request):
    """Debug view to inspect S3 bucket contents and help identify PDF storage patterns."""
    try:
        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-2')
        )
        
        bucket_name = os.environ.get('S3_BUCKET', 'bill-review-prod')
        
        # Common prefixes to check
        prefixes = [
            'data/hcfa_pdf/',
            'data/ProviderBills/pdf/',
            'pdfs/',
            'bills/',
            'data/',
            ''  # Root level
        ]
        
        bucket_contents = {}
        
        for prefix in prefixes:
            try:
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    MaxKeys=500000  # Get more objects for better analysis
                )
                
                if 'Contents' in response:
                    objects = response['Contents']
                    bucket_contents[prefix] = {
                        'count': len(objects),
                        'objects': [obj['Key'] for obj in objects[:20]]  # Show first 20
                    }
                else:
                    bucket_contents[prefix] = {'count': 0, 'objects': []}
                    
            except Exception as e:
                bucket_contents[prefix] = {'error': str(e)}
        
        # Also get some sample bills from database to check their IDs
        sample_bills = []
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT id, claim_id, patient_name, status 
                    FROM ProviderBill 
                    ORDER BY id DESC 
                    LIMIT 10
                """)
                rows = cursor.fetchall()
                for row in rows:
                    sample_bills.append({
                        'bill_id': row[0],
                        'claim_id': row[1],
                        'patient_name': row[2],
                        'status': row[3]
                    })
        except Exception as e:
            sample_bills = [{'error': str(e)}]
        
        context = {
            'bucket_name': bucket_name,
            'bucket_contents': bucket_contents,
            'sample_bills': sample_bills,
            'environment_info': {
                'aws_region': os.environ.get('AWS_DEFAULT_REGION', 'us-east-2'),
                's3_bucket': bucket_name,
                'has_aws_key': bool(os.environ.get('AWS_ACCESS_KEY_ID')),
                'has_aws_secret': bool(os.environ.get('AWS_SECRET_ACCESS_KEY')),
            }
        }
        
        return render(request, 'bill_review/debug_s3.html', context)
        
    except Exception as e:
        logger.exception(f"Error in debug_s3_bucket: {str(e)}")
        return HttpResponse(f"Error: {str(e)}", status=500)