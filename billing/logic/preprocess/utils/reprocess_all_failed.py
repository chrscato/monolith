#!/usr/bin/env python3
"""
reprocess_all_failed.py – Find and reprocess all failed bills automatically

This script:
1. Finds all bills that failed extraction
2. Reprocesses them automatically
3. Runs the extraction again

Usage:
    python reprocess_all_failed.py --dry-run
    python reprocess_all_failed.py --run-extraction
"""

from __future__ import annotations
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[4]
sys.path.append(str(PROJECT_ROOT))

from find_failed_bills import find_bills_by_status, find_bills_without_line_items, find_bills_with_errors
from reprocess_failed_bills import reprocess_bills

def find_all_failed_bills():
    """Find all types of failed bills."""
    all_failed_bills = set()
    
    # Find bills with non-RECEIVED status
    print("Finding bills with non-RECEIVED status...")
    failed_status = find_bills_by_status("RECEIVED")
    for bill_id, status, error in failed_status:
        all_failed_bills.add(bill_id)
    
    # Find bills with no line items
    print("Finding bills with no line items...")
    no_lines = find_bills_without_line_items()
    for bill_id, status, line_count in no_lines:
        all_failed_bills.add(bill_id)
    
    # Find bills with errors
    print("Finding bills with errors...")
    with_errors = find_bills_with_errors()
    for bill_id, status, error in with_errors:
        all_failed_bills.add(bill_id)
    
    return sorted(list(all_failed_bills))

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Find and reprocess all failed bills")
    parser.add_argument("--dry-run", action="store_true",
                       help="Show what would be done without making changes")
    parser.add_argument("--run-extraction", action="store_true",
                       help="Run the extraction after reprocessing")
    
    args = parser.parse_args()
    
    print("Finding all failed bills...")
    failed_bills = find_all_failed_bills()
    
    if not failed_bills:
        print("✓ No failed bills found!")
        return
    
    print(f"\nFound {len(failed_bills)} failed bills:")
    for i, bill_id in enumerate(failed_bills, 1):
        print(f"  {i}. {bill_id}")
    
    if args.dry_run:
        print(f"\n[DRY RUN] Would reprocess {len(failed_bills)} bills")
        return
    
    # Confirm before proceeding
    response = input(f"\nProceed with reprocessing {len(failed_bills)} bills? (y/N): ")
    if response.lower() != 'y':
        print("Cancelled.")
        return
    
    # Reprocess all bills
    reprocess_bills(failed_bills, dry_run=False, run_extraction=args.run_extraction)

if __name__ == "__main__":
    main() 