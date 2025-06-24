#!/usr/bin/env python3
"""
s3_backup_sync.py

Script to sync backup PDF files to S3 bucket folders.
Checks if backup PDFs exist in S3 input folder and moves them to archive if missing.

This script addresses the issue where PDF files are backed up locally but the S3 move operation
in llm_hcfa_vision.py fails, leaving files in the input folder instead of being archived.
"""

import os
import sys
import tempfile
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[4]
sys.path.append(str(PROJECT_ROOT))

# Load environment variables
load_dotenv(PROJECT_ROOT / ".env")

# Import S3 utilities
from config.s3_utils import list_objects, upload, move, download

# S3 Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "bill-review-prod")
INPUT_PREFIX = "data/ProviderBills/pdf/"
ARCHIVE_PREFIX = "data/ProviderBills/pdf/archive/"
BACKUP_DIR = PROJECT_ROOT / "billing" / "logic" / "preprocess" / "backup_pdfs"
LOG_PREFIX = "logs/backup_sync_errors.log"

def get_s3_files(prefix):
    """Get list of files in S3 with given prefix."""
    try:
        return set(list_objects(prefix))
    except Exception as e:
        print(f"❌ Error listing S3 files with prefix {prefix}: {e}")
        return set()

def check_and_sync_backups():
    """Check backup PDFs and sync them to S3 if needed."""
    print(f"🔍 Checking backup PDFs in: {BACKUP_DIR}")
    print(f"📦 S3 Bucket: {S3_BUCKET}")
    print(f"📁 Input prefix: {INPUT_PREFIX}")
    print(f"📁 Archive prefix: {ARCHIVE_PREFIX}")
    print("-" * 60)
    
    if not BACKUP_DIR.exists():
        print(f"❌ Backup directory not found: {BACKUP_DIR}")
        return
    
    # Get backup PDF files
    backup_files = list(BACKUP_DIR.glob("*.pdf"))
    if not backup_files:
        print("📭 No backup PDF files found")
        return
    
    print(f"📄 Found {len(backup_files)} backup PDF files")
    
    # Get S3 file lists
    print("🔍 Checking S3 input folder...")
    s3_input_files = get_s3_files(INPUT_PREFIX)
    print(f"   Found {len(s3_input_files)} files in S3 input")
    
    print("🔍 Checking S3 archive folder...")
    s3_archive_files = get_s3_files(ARCHIVE_PREFIX)
    print(f"   Found {len(s3_archive_files)} files in S3 archive")
    
    # Process each backup file
    processed_count = 0
    errors = []
    
    for backup_file in backup_files:
        bill_id = backup_file.stem
        input_key = f"{INPUT_PREFIX}{bill_id}.pdf"
        archive_key = f"{ARCHIVE_PREFIX}{bill_id}.pdf"
        
        print(f"\n📄 Processing: {bill_id}")
        
        # Check if file exists in S3 input
        if input_key in s3_input_files:
            print(f"   📦 Found in S3 input: {input_key}")
            print(f"   → Moving to S3 archive: {archive_key}")
            try:
                move(input_key, archive_key)
                print(f"   ✅ Successfully moved to archive")
                processed_count += 1
            except Exception as e:
                error_msg = f"Failed to move {input_key} to {archive_key}: {e}"
                print(f"   ❌ {error_msg}")
                errors.append(error_msg)
        elif archive_key in s3_archive_files:
            print(f"   📦 Already in S3 archive: {archive_key}")
            processed_count += 1
        else:
            print(f"   ❌ Not found in S3 input or archive.")
            print(f"   → Uploading local file to S3 input: {input_key}")
            try:
                upload(str(backup_file), input_key)
                print(f"   ✅ Uploaded to S3 input")
                processed_count += 1
            except Exception as e:
                error_msg = f"Failed to upload {backup_file} to {input_key}: {e}"
                print(f"   ❌ {error_msg}")
                errors.append(error_msg)
    
    # Log errors if any
    if errors:
        print(f"\n❌ {len(errors)} errors occurred:")
        err_file = tempfile.mktemp(suffix=".log")
        with open(err_file, "w") as f:
            f.write(f"{datetime.now()}: Backup sync errors:\n")
            for error in errors:
                f.write(f"  {error}\n")
        try:
            upload(err_file, LOG_PREFIX)
            print(f"📝 Errors logged to S3: {LOG_PREFIX}")
        except Exception as e:
            print(f"⚠️  Failed to upload error log: {e}")
        os.unlink(err_file)
    
    print(f"\n📊 Summary:")
    print(f"   Total backup files: {len(backup_files)}")
    print(f"   Successfully processed: {processed_count}")
    print(f"   Errors: {len(errors)}")

def cleanup_successful_backups():
    """Remove local backup files that are successfully archived in S3."""
    print(f"\n🧹 Cleaning up successful backups...")
    
    if not BACKUP_DIR.exists():
        print(f"❌ Backup directory not found: {BACKUP_DIR}")
        return
    
    # Get S3 archive files
    s3_archive_files = get_s3_files(ARCHIVE_PREFIX)
    print(f"📁 Found {len(s3_archive_files)} files in S3 archive")
    
    backup_files = list(BACKUP_DIR.glob("*.pdf"))
    cleaned_count = 0
    
    for backup_file in backup_files:
        bill_id = backup_file.stem
        archive_key = f"{ARCHIVE_PREFIX}{bill_id}.pdf"
        
        if archive_key in s3_archive_files:
            try:
                backup_file.unlink()
                print(f"   ✅ Removed: {bill_id}.pdf")
                cleaned_count += 1
            except Exception as e:
                print(f"   ❌ Failed to remove {bill_id}.pdf: {e}")
        else:
            print(f"   ⚠️  Keeping: {bill_id}.pdf (not in S3 archive)")
    
    print(f"📊 Cleaned up {cleaned_count} backup files")

def show_status():
    """Show current status of backup files vs S3."""
    print(f"📊 Backup Sync Status Report")
    print("=" * 50)
    
    if not BACKUP_DIR.exists():
        print(f"❌ Backup directory not found: {BACKUP_DIR}")
        return
    
    backup_files = list(BACKUP_DIR.glob("*.pdf"))
    s3_input_files = get_s3_files(INPUT_PREFIX)
    s3_archive_files = get_s3_files(ARCHIVE_PREFIX)
    
    print(f"📁 Local backup files: {len(backup_files)}")
    print(f"📦 S3 input files: {len(s3_input_files)}")
    print(f"📦 S3 archive files: {len(s3_archive_files)}")
    
    # Count files in different states
    in_input = 0
    in_archive = 0
    missing_from_s3 = 0
    
    for backup_file in backup_files:
        bill_id = backup_file.stem
        input_key = f"{INPUT_PREFIX}{bill_id}.pdf"
        archive_key = f"{ARCHIVE_PREFIX}{bill_id}.pdf"
        
        if input_key in s3_input_files:
            in_input += 1
        elif archive_key in s3_archive_files:
            in_archive += 1
        else:
            missing_from_s3 += 1
    
    print(f"\n📈 File Status:")
    print(f"   In S3 input: {in_input}")
    print(f"   In S3 archive: {in_archive}")
    print(f"   Missing from S3: {missing_from_s3}")

def main():
    """Main function with command line interface."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Sync backup PDF files to S3")
    parser.add_argument("--action", choices=["sync", "cleanup", "status"], 
                       default="sync", help="Action to perform")
    parser.add_argument("--no-cleanup", action="store_true", 
                       help="Skip cleanup after sync")
    
    args = parser.parse_args()
    
    if args.action == "sync":
        check_and_sync_backups()
        if not args.no_cleanup:
            cleanup_successful_backups()
    elif args.action == "cleanup":
        cleanup_successful_backups()
    elif args.action == "status":
        show_status()

if __name__ == "__main__":
    main() 