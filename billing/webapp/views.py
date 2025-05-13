from django.shortcuts import render
from django.db import connection
from django.views.generic import ListView
from datetime import datetime, timedelta


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


class DashboardView(ListView):
    template_name = "billing/dashboard.html"
    context_object_name = "bills"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["flagged_bills"] = get_flagged_bills()
        context["error_bills"] = get_error_bills()
        context["arthrogram_bills"] = get_arthrogram_bills()
        return context

    def get_queryset(self):
        return []  # Not used, but required by ListView


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

    context = {"bill": bill, "line_items": get_bill_line_items(bill_id)}
    return render(request, "billing/bill_detail.html", context)
