#!/usr/bin/env python3
"""
find_failed_bills.py – Find bills that failed extraction

This script identifies bills that:
1. Have status other than 'RECEIVED' 
2. Have no line items
3. Have errors in last_error field
4. Are in archive but not properly processed

Usage:
    python find_failed_bills.py --status-failed
    python find_failed_bills.py --no-line-items
    python find_failed_bills.py --all-failures
"""

from __future__ import annotations
import os, sys, argparse, sqlite3
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[4]
sys.path.append(str(PROJECT_ROOT))

# Import S3 utilities
from config.s3_utils import list_objects

# Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "bill-review-prod")
INPUT_PREFIX = "data/ProviderBills/pdf/"
ARCHIVE_PREFIX = "data/ProviderBills/pdf/archive/"
DB_PATH = os.getenv("MONOLITH_DB_PATH", str(PROJECT_ROOT / "monolith.db"))

def find_bills_by_status(status_filter: str = None) -> list[str]:
    """Find bills with specific status."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        cur = conn.cursor()
        
        if status_filter:
            cur.execute("""
                SELECT id, status, last_error 
                FROM ProviderBill 
                WHERE status != ?
                ORDER BY id
            """, (status_filter,))
        else:
            cur.execute("""
                SELECT id, status, last_error 
                FROM ProviderBill 
                ORDER BY id
            """)
        
        results = cur.fetchall()
        return [(row[0], row[1], row[2]) for row in results]
        
    except Exception as exc:
        print(f" ❌ Database error: {exc}")
        return []
    finally:
        conn.close()

def find_bills_without_line_items() -> list[str]:
    """Find bills that have no line items."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT pb.id, pb.status, COUNT(bli.id) as line_count
            FROM ProviderBill pb
            LEFT JOIN BillLineItem bli ON pb.id = bli.provider_bill_id
            GROUP BY pb.id
            HAVING line_count = 0
            ORDER BY pb.id
        """)
        
        results = cur.fetchall()
        return [(row[0], row[1], row[2]) for row in results]
        
    except Exception as exc:
        print(f" ❌ Database error: {exc}")
        return []
    finally:
        conn.close()

def find_bills_with_errors() -> list[str]:
    """Find bills with errors in last_error field."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, status, last_error 
            FROM ProviderBill 
            WHERE last_error IS NOT NULL AND last_error != ''
            ORDER BY id
        """)
        
        results = cur.fetchall()
        return [(row[0], row[1], row[2]) for row in results]
        
    except Exception as exc:
        print(f" ❌ Database error: {exc}")
        return []
    finally:
        conn.close()

def find_archived_bills() -> list[str]:
    """Find bills that are in archive."""
    try:
        archive_objects = list_objects(ARCHIVE_PREFIX)
        return [obj.replace(ARCHIVE_PREFIX, "").replace(".pdf", "") 
                for obj in archive_objects if obj.endswith(".pdf")]
    except Exception as exc:
        print(f" ❌ S3 error: {exc}")
        return []

def check_bill_processing_status(bill_id: str) -> dict:
    """Check comprehensive status of a bill."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        cur = conn.cursor()
        
        # Get bill status
        cur.execute("""
            SELECT status, last_error, total_charge
            FROM ProviderBill 
            WHERE id = ?
        """, (bill_id,))
        
        bill_result = cur.fetchone()
        if not bill_result:
            return {"exists": False}
        
        # Get line item count
        cur.execute("""
            SELECT COUNT(*) 
            FROM BillLineItem 
            WHERE provider_bill_id = ?
        """, (bill_id,))
        
        line_count = cur.fetchone()[0]
        
        # Check if in archive
        archive_objects = list_objects(ARCHIVE_PREFIX)
        in_archive = f"{bill_id}.pdf" in [obj.replace(ARCHIVE_PREFIX, "") for obj in archive_objects]
        
        # Check if in input
        input_objects = list_objects(INPUT_PREFIX)
        in_input = f"{bill_id}.pdf" in [obj.replace(INPUT_PREFIX, "") for obj in input_objects]
        
        return {
            "exists": True,
            "status": bill_result[0],
            "last_error": bill_result[1],
            "total_charge": bill_result[2],
            "line_items": line_count,
            "in_archive": in_archive,
            "in_input": in_input
        }
        
    except Exception as exc:
        return {"error": str(exc)}
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="Find bills that failed extraction")
    parser.add_argument("--status-failed", action="store_true",
                       help="Find bills with status other than 'RECEIVED'")
    parser.add_argument("--no-line-items", action="store_true",
                       help="Find bills with no line items")
    parser.add_argument("--with-errors", action="store_true",
                       help="Find bills with errors in last_error field")
    parser.add_argument("--archived", action="store_true",
                       help="Find bills in archive")
    parser.add_argument("--all-failures", action="store_true",
                       help="Find all types of failures")
    parser.add_argument("--check", type=str,
                       help="Check status of specific bill ID")
    
    args = parser.parse_args()
    
    if args.check:
        print(f"Checking status of bill: {args.check}")
        status = check_bill_processing_status(args.check)
        print(f"Status: {status}")
        return
    
    if args.all_failures or not any([args.status_failed, args.no_line_items, args.with_errors, args.archived]):
        args.status_failed = True
        args.no_line_items = True
        args.with_errors = True
        args.archived = True
    
    all_failed_bills = set()
    
    if args.status_failed:
        print("\n=== Bills with non-RECEIVED status ===")
        failed_status = find_bills_by_status("RECEIVED")
        for bill_id, status, error in failed_status:
            print(f"  {bill_id}: {status} {f'(Error: {error})' if error else ''}")
            all_failed_bills.add(bill_id)
    
    if args.no_line_items:
        print("\n=== Bills with no line items ===")
        no_lines = find_bills_without_line_items()
        for bill_id, status, line_count in no_lines:
            print(f"  {bill_id}: {status} (Line items: {line_count})")
            all_failed_bills.add(bill_id)
    
    if args.with_errors:
        print("\n=== Bills with errors ===")
        with_errors = find_bills_with_errors()
        for bill_id, status, error in with_errors:
            print(f"  {bill_id}: {status} (Error: {error})")
            all_failed_bills.add(bill_id)
    
    if args.archived:
        print("\n=== Bills in archive ===")
        archived = find_archived_bills()
        for bill_id in archived:
            print(f"  {bill_id}")
            all_failed_bills.add(bill_id)
    
    print(f"\n" + "="*50)
    print(f"SUMMARY:")
    print(f"  Total unique failed bills: {len(all_failed_bills)}")
    
    if all_failed_bills:
        print(f"\nAll failed bill IDs:")
        bill_list = ",".join(sorted(all_failed_bills))
        print(bill_list)
        
        print(f"\nTo reprocess these bills, use:")
        print(f"  python reprocess_failed_bills.py --bill-ids \"{bill_list}\" --dry-run")

if __name__ == "__main__":
    main() 