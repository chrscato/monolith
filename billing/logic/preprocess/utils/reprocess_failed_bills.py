#!/usr/bin/env python3
"""
reprocess_failed_bills.py – Reprocess bills that failed extraction

This script:
1. Takes a list of ProviderBill IDs that failed extraction
2. Moves their PDFs from archive back to input folder
3. Resets their database status to allow reprocessing
4. Optionally runs the extraction again

Usage:
    python reprocess_failed_bills.py --bill-ids "bill1,bill2,bill3" --run-extraction
    python reprocess_failed_bills.py --bill-ids "bill1,bill2,bill3" --dry-run
"""

from __future__ import annotations
import os, sys, argparse, sqlite3
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[4]
sys.path.append(str(PROJECT_ROOT))

# Load environment variables
load_dotenv(PROJECT_ROOT / ".env")

# Import S3 utilities
from config.s3_utils import list_objects, download, upload, move

# Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "bill-review-prod")
INPUT_PREFIX = "data/ProviderBills/pdf/"
ARCHIVE_PREFIX = "data/ProviderBills/pdf/archive/"
DB_PATH = r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\monolith\monolith.db"

def reset_bill_status(bill_id: str) -> bool:
    """Reset a bill's status to allow reprocessing."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        cur = conn.cursor()
        
        # Check if bill exists
        cur.execute("SELECT id, status FROM ProviderBill WHERE id=?", (bill_id,))
        result = cur.fetchone()
        
        if not result:
            print(f" ❌ ProviderBill {bill_id} not found in database")
            return False
        
        current_status = result[1]
        print(f"   Current status: {current_status}")
        
        # Reset status to allow reprocessing
        cur.execute("""
            UPDATE ProviderBill 
            SET status='PENDING', last_error=NULL, action=NULL
            WHERE id=?
        """, (bill_id,))
        
        # Clear any existing line items for this bill
        cur.execute("DELETE FROM BillLineItem WHERE provider_bill_id=?", (bill_id,))
        
        conn.commit()
        print(f"   ✓ Reset status to PENDING and cleared line items")
        return True
        
    except Exception as exc:
        print(f" ❌ Database error for {bill_id}: {exc}")
        conn.rollback()
        return False
    finally:
        conn.close()

def move_pdf_from_archive(bill_id: str, dry_run: bool = False) -> bool:
    """Move PDF from archive back to input folder."""
    archive_key = f"{ARCHIVE_PREFIX}{bill_id}.pdf"
    input_key = f"{INPUT_PREFIX}{bill_id}.pdf"
    
    try:
        # Debug S3 configuration
        bucket = os.getenv("S3_BUCKET", "bill-review-prod")
        aws_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        print(f"   Using S3 bucket: {bucket}")
        print(f"   AWS credentials: {'✓ Set' if aws_key and aws_secret else '❌ Missing'}")
        print(f"   Looking for: {archive_key}")
        
        # Check if PDF exists in archive
        try:
            archive_objects = list_objects(ARCHIVE_PREFIX)
            print(f"   list_objects returned: {type(archive_objects)} with {len(archive_objects) if archive_objects else 'None'} items")
        except Exception as list_exc:
            print(f" ❌ Error calling list_objects: {list_exc}")
            return False
            
        if archive_objects is None:
            print(f" ❌ Error listing archive objects - S3 connection issue")
            print(f"   Check AWS credentials and S3_BUCKET environment variable")
            return False
        
        # Check if PDF is in archive
        if archive_key not in archive_objects:
            print(f" ❌ PDF not found in archive: {archive_key}")
            
            # Check if PDF is already in input folder
            try:
                input_objects = list_objects(INPUT_PREFIX)
                if input_key in input_objects:
                    print(f"   ✓ PDF found in input folder - no need to move")
                    return True
                else:
                    print(f"   ❌ PDF not found in input folder either")
            except Exception as input_exc:
                print(f"   ❌ Error checking input folder: {input_exc}")
            
            # Show some archive files to help debug
            print(f"   Available archive objects: {len(archive_objects)} files")
            print(f"   First 10 archive files:")
            for i, obj in enumerate(archive_objects[:10]):
                print(f"     {i+1}. {obj}")
            
            # Look for files that might match the bill ID
            matching_files = [obj for obj in archive_objects if bill_id in obj]
            if matching_files:
                print(f"   Found {len(matching_files)} files containing '{bill_id}':")
                for obj in matching_files:
                    print(f"     {obj}")
            else:
                print(f"   No files found containing '{bill_id}'")
            
            return False
        
        if dry_run:
            print(f"   [DRY RUN] Would move {archive_key} → {input_key}")
            return True
        
        # Move from archive to input
        move(archive_key, input_key)
        print(f"   ✓ Moved PDF from archive to input")
        return True
        
    except Exception as exc:
        print(f" ❌ S3 move error for {bill_id}: {exc}")
        import traceback
        print(f"   Full error: {traceback.format_exc()}")
        return False

def reprocess_bills(bill_ids: list[str], dry_run: bool = False, run_extraction: bool = False):
    """Reprocess a list of failed bills."""
    print(f"Reprocessing {len(bill_ids)} bills...")
    print(f"Dry run: {dry_run}")
    print(f"Run extraction: {run_extraction}")
    print("-" * 50)
    
    successful = 0
    failed = 0
    
    for i, bill_id in enumerate(bill_ids, 1):
        print(f"\n[{i}/{len(bill_ids)}] Processing {bill_id}")
        
        # Step 1: Reset database status
        if not reset_bill_status(bill_id):
            failed += 1
            continue
        
        # Step 2: Move PDF from archive to input
        if not move_pdf_from_archive(bill_id, dry_run):
            failed += 1
            continue
        
        successful += 1
        print(f"   ✓ {bill_id} ready for reprocessing")
    
    print(f"\n" + "="*50)
    print(f"SUMMARY:")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print(f"  Total: {len(bill_ids)}")
    
    if successful > 0 and not dry_run:
        print(f"\nBills ready for reprocessing: {successful}")
        if run_extraction:
            print("\nRunning extraction...")
            # Import and run the extraction
            from llm_hcfa_vision import process_s3
            process_s3()
        else:
            print(f"\nTo run extraction, use:")
            print(f"  python billing/logic/preprocess/utils/llm_hcfa_vision.py")

def main():
    parser = argparse.ArgumentParser(description="Reprocess failed HCFA-1500 bills")
    parser.add_argument("--bill-ids", required=True, 
                       help="Comma-separated list of ProviderBill IDs")
    parser.add_argument("--dry-run", action="store_true",
                       help="Show what would be done without making changes")
    parser.add_argument("--run-extraction", action="store_true",
                       help="Run the extraction after reprocessing")
    
    args = parser.parse_args()
    
    # Parse bill IDs
    bill_ids = [bid.strip() for bid in args.bill_ids.split(",") if bid.strip()]
    
    if not bill_ids:
        print("❌ No valid bill IDs provided")
        return
    
    print(f"ProviderBill IDs to reprocess: {bill_ids}")
    
    # Confirm before proceeding (unless dry run)
    if not args.dry_run:
        response = input(f"\nProceed with reprocessing {len(bill_ids)} bills? (y/N): ")
        if response.lower() != 'y':
            print("Cancelled.")
            return
    
    reprocess_bills(bill_ids, args.dry_run, args.run_extraction)

if __name__ == "__main__":
    main() 