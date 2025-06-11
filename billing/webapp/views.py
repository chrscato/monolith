from django.shortcuts import render
from django.db import connection
from django.views.generic import ListView
from datetime import datetime, timedelta
from collections import Counter

# Define status colors and descriptions
STATUS_METADATA = {
    'FLAGGED': {'color': 'warning', 'description': 'Bills that need review'},
    'ERROR': {'color': 'danger', 'description': 'Bills with processing errors'},
    'ARTHROGRAM': {'color': 'info', 'description': 'Arthrogram bills'},
    'MAPPED': {'color': 'success', 'description': 'Successfully mapped bills'},
    'PROCESSED': {'color': 'primary', 'description': 'Processed bills'},
    'REVIEW_FLAG': {'color': 'warning', 'description': 'Bills flagged for review'},
}

def get_flagged_bills():
    """Get all flagged bills with their details."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT 
                pb.id,
                pb.claim_id,
                pb.status,
                pb.action,
                pb.last_error,
                pb.created_at,
                p."DBA Name Billing Name" as provider_name,
                p.TIN
            FROM ProviderBill pb
            LEFT JOIN orders o ON pb.claim_id = o.Order_ID
            LEFT JOIN providers p ON o.provider_id = p.PrimaryKey
            WHERE pb.status IN ('FLAGGED', 'REVIEW_FLAG')
            ORDER BY pb.created_at DESC
        """
        )
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_error_bills():
    """Get all error bills with their details."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT 
                pb.id,
                pb.claim_id,
                pb.status,
                pb.action,
                pb.last_error,
                pb.created_at,
                p."DBA Name Billing Name" as provider_name,
                p.TIN
            FROM ProviderBill pb
            LEFT JOIN orders o ON pb.claim_id = o.Order_ID
            LEFT JOIN providers p ON o.provider_id = p.PrimaryKey
            WHERE pb.status = 'ERROR'
            ORDER BY pb.created_at DESC
        """
        )
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_arthrogram_bills():
    """Get all arthrogram bills with their details."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT 
                pb.id,
                pb.claim_id,
                pb.status,
                pb.action,
                pb.last_error,
                pb.created_at,
                p."DBA Name Billing Name" as provider_name,
                p.TIN
            FROM ProviderBill pb
            LEFT JOIN orders o ON pb.claim_id = o.Order_ID
            LEFT JOIN providers p ON o.provider_id = p.PrimaryKey
            WHERE pb.status = 'ARTHROGRAM'
            ORDER BY pb.created_at DESC
        """
        )
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_bill_line_items(bill_id):
    """Get line items for a specific bill."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT 
                bli.id,
                bli.cpt_code,
                bli.modifier,
                bli.units,
                bli.charge_amount,
                bli.allowed_amount,
                bli.decision,
                bli.reason_code,
                bli.date_of_service
            FROM BillLineItem bli
            WHERE bli.provider_bill_id = ?
            ORDER BY bli.date_of_service
        """,
            [bill_id],
        )
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_status_distribution(filtered=False, status=None, action=None):
    """Get distribution of bill statuses with metadata."""
    query = """
        SELECT 
            status,
            COUNT(*) as count,
            MIN(created_at) as first_occurrence,
            MAX(created_at) as last_occurrence
        FROM ProviderBill
        WHERE 1=1
    """
    params = []
    
    if filtered:
        if status:
            query += " AND status = %s"
            params.append(status)
        if action:
            query += " AND action = %s"
            params.append(action)
            
    query += " GROUP BY status ORDER BY count DESC"
    
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        columns = [col[0] for col in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        # Enhance results with metadata
        for result in results:
            status = result['status']
            result['color'] = STATUS_METADATA.get(status, {}).get('color', 'secondary')
            result['description'] = STATUS_METADATA.get(status, {}).get('description', '')
            
        return results


def get_action_distribution(filtered=False, status=None, action=None):
    """Get distribution of bill actions with metadata."""
    query = """
        SELECT 
            action,
            COUNT(*) as count,
            MIN(created_at) as first_occurrence,
            MAX(created_at) as last_occurrence
        FROM ProviderBill
        WHERE action IS NOT NULL
    """
    params = []
    
    if filtered:
        if status:
            query += " AND status = %s"
            params.append(status)
        if action:
            query += " AND action = %s"
            params.append(action)
            
    query += " GROUP BY action ORDER BY count DESC"
    
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_filtered_bills(status=None, action=None):
    """Get bills filtered by status and/or action."""
    query = """
        SELECT 
            pb.id,
            pb.claim_id,
            pb.status,
            pb.action,
            pb.last_error,
            pb.created_at,
            p."DBA Name Billing Name" as provider_name,
            p.TIN
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
    
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        columns = [col[0] for col in cursor.description]
        bills = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        # Add status description to each bill
        for bill in bills:
            bill['status_description'] = STATUS_METADATA.get(bill['status'], {}).get('description', '')
            
        return bills


class DashboardView(ListView):
    template_name = "bill_review/dashboard.html"
    context_object_name = "bills"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # Get filter parameters
        status = self.request.GET.get('status')
        action = self.request.GET.get('action')
        
        # Get filtered bills
        context["bills"] = get_filtered_bills(status, action)
        
        # Get distributions for charts (with filter context)
        context["status_distribution"] = get_status_distribution(
            filtered=True, status=status, action=action
        )
        context["action_distribution"] = get_action_distribution(
            filtered=True, status=status, action=action
        )
        
        # Get unique statuses and actions for filter dropdowns
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT status 
                FROM ProviderBill 
                WHERE status IS NOT NULL 
                ORDER BY status
            """)
            context["statuses"] = [row[0] for row in cursor.fetchall()]
            
            cursor.execute("""
                SELECT DISTINCT action 
                FROM ProviderBill 
                WHERE action IS NOT NULL 
                ORDER BY action
            """)
            context["actions"] = [row[0] for row in cursor.fetchall()]
        
        return context

    def get_queryset(self):
        return []  # Not used, but required by ListView


def normalize_date(date_str):
    if not date_str:
        return ''
    date_str = str(date_str).strip()
    if ' - ' in date_str:
        date_str = date_str.split(' - ')[0].strip()
    if ' ' in date_str:
        date_str = date_str.split(' ')[0]
    formats = [
        '%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y',
        '%Y/%m/%d', '%m/%d/%y', '%m-%d-%y',
        '%Y%m%d', '%m%d%Y', '%m%d%y'
    ]
    for fmt in formats:
        try:
            d = datetime.strptime(date_str, fmt).date()
            if 2020 <= d.year <= 2035:
                return d.strftime('%Y-%m-%d')
        except ValueError:
            continue
    return ''


def bill_detail(request, bill_id):
    """View for detailed bill information."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT 
                pb.*,
                p."DBA Name Billing Name" as provider_name,
                p.TIN,
                o.Order_ID,
                o.bundle_type
            FROM ProviderBill pb
            LEFT JOIN orders o ON pb.claim_id = o.Order_ID
            LEFT JOIN providers p ON o.provider_id = p.PrimaryKey
            WHERE pb.id = ?
        """,
            [bill_id],
        )
        columns = [col[0] for col in cursor.description]
        bill = dict(zip(columns, cursor.fetchone()))

    bill_line_items = get_bill_line_items(bill_id)
    for item in bill_line_items:
        item['date_of_service'] = normalize_date(item.get('date_of_service', ''))

    context = {"bill": bill, "line_items": bill_line_items}
    return render(request, "billing/bill_detail.html", context)
