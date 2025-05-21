# billing/webapp/bill_review/views.py
from django.shortcuts import render, redirect, get_object_or_404
from django.db import connection
from django.urls import reverse
from django.http import HttpResponseRedirect, HttpResponse, Http404
from django.contrib import messages
import logging
from .forms import BillUpdateForm, LineItemUpdateForm
from django.contrib.auth.decorators import login_required
import boto3
import os
import tempfile
from botocore.exceptions import ClientError
from django.views.decorators.http import require_GET

logger = logging.getLogger(__name__)

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

def bill_detail(request, bill_id):
    """Show comprehensive details for a bill with all data needed for manual review."""
    try:
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
                # Debug log to see what provider data we're getting
                logger.info(f"Provider data extracted for bill {bill_id}: {provider}")
                logger.info(f"Raw bill data for provider fields: {bill}")
            
            # Get bill line items
            cursor.execute("""
                SELECT bli.* 
                FROM BillLineItem bli
                WHERE bli.provider_bill_id = %s
                ORDER BY bli.date_of_service, bli.cpt_code
            """, [bill_id])
            columns = [col[0] for col in cursor.description]
            bill_items = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
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
            
            # Initialize form
            form = BillUpdateForm(initial={
                'status': bill.get('status'),
                'action': bill.get('action'),
                'last_error': bill.get('last_error')
            })
            
            # Prepare context for template
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
            }
            
            return render(request, 'bill_review/bill_detail.html', context)
            
    except Exception as e:
        logger.exception(f"Error retrieving bill details: {e}")
        messages.error(request, f"Error retrieving bill details: {str(e)}")
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
                            reason_code = %s
                        WHERE id = %s
                    """, [
                        form.cleaned_data['cpt_code'],
                        form.cleaned_data['modifier'],
                        form.cleaned_data['units'],
                        form.cleaned_data['charge_amount'],
                        form.cleaned_data['allowed_amount'],
                        form.cleaned_data['decision'],
                        form.cleaned_data['reason_code'],
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
    
    return HttpResponseRedirect(reverse('bill_review:dashboard'))

@login_required
def update_provider(request, provider_id):
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
                
                # Get the bill_id from the request
                bill_id = request.GET.get('bill_id')
                if bill_id:
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
            logger.error(f"Error updating provider {provider_id}: {str(e)}")
            messages.error(request, 'Failed to update provider information.')
    
    # Redirect back to the bill detail page
    return redirect('bill_review:bill_detail', bill_id=request.GET.get('bill_id'))

@login_required
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
    
    return redirect('bill_review:bill_detail', bill_id=bill_id)

@login_required
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
        
        # Get the order_id for this bill
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT claim_id FROM ProviderBill WHERE id = %s
            """, [bill_id])
            row = cursor.fetchone()
            order_id = row[0] if row else None
        
        # Define possible PDF paths to check
        possible_paths = [
            f'data/hcfa_pdf/archived/{bill_id}.pdf',  # Using providerbill_id in archived
            f'data/hcfa_pdf/{bill_id}.pdf',          # Using providerbill_id in root
            f'data/ProviderBills/pdf/archive/{bill_id}.pdf',  # Using providerbill_id in new archive
        ]
        
        # Add order_id paths if we have an order_id
        if order_id:
            possible_paths.extend([
                f'data/hcfa_pdf/archived/{order_id}.pdf',  # Using order_id in archived
                f'data/hcfa_pdf/{order_id}.pdf',          # Using order_id in root
                f'data/ProviderBills/pdf/archive/{order_id}.pdf',  # Using order_id in new archive
            ])
        
        # Log the paths we're going to try
        logger.info(f"Attempting to find PDF for bill {bill_id} in the following paths:")
        for path in possible_paths:
            logger.info(f"Checking path: {path}")
        
        # Try each possible path
        for pdf_key in possible_paths:
            try:
                # First verify the object exists
                try:
                    s3_client.head_object(Bucket=bucket_name, Key=pdf_key)
                    logger.info(f"Found PDF at path: {pdf_key}")
                except ClientError as e:
                    error_code = e.response['Error']['Code']
                    if error_code == '404':
                        logger.info(f"PDF not found at path: {pdf_key}")
                        continue
                    else:
                        logger.error(f"Error checking path {pdf_key}: {str(e)}")
                        continue

                # Generate pre-signed URL
                url = s3_client.generate_presigned_url(
                    'get_object',
                    Params={
                        'Bucket': bucket_name,
                        'Key': pdf_key,
                        'ResponseContentType': 'application/pdf'
                    },
                    ExpiresIn=3600  # URL expires in 1 hour
                )
                logger.info(f"Successfully generated pre-signed URL for path: {pdf_key}")
                return HttpResponseRedirect(url)
            except ClientError as e:
                error_code = e.response['Error']['Code']
                error_message = e.response['Error']['Message']
                logger.error(f"Error generating URL for {pdf_key}: {error_code} - {error_message}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error for {pdf_key}: {str(e)}")
                continue
        
        # If we get here, none of the paths worked
        logger.error(f"Failed to find PDF for bill {bill_id} in any location")
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