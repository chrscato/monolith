#!/usr/bin/env python3
"""
manage_backups.py

Utility script to manage local backup files created during PDF/OCR processing.
"""
import os
import shutil
from pathlib import Path
from datetime import datetime

def list_backups():
    """List all backup files and their sizes."""
    backup_dirs = ["backup_pdfs", "backup_ocr_files"]
    
    for backup_dir in backup_dirs:
        if Path(backup_dir).exists():
            print(f"\n📁 {backup_dir}/")
            print("=" * 50)
            
            files = list(Path(backup_dir).glob("*"))
            if not files:
                print("   No backup files found")
                continue
                
            total_size = 0
            for file_path in sorted(files):
                if file_path.is_file():
                    size = file_path.stat().st_size
                    total_size += size
                    size_mb = size / (1024 * 1024)
                    print(f"   {file_path.name} ({size_mb:.2f} MB)")
            
            total_mb = total_size / (1024 * 1024)
            print(f"\n   Total: {len(files)} files, {total_mb:.2f} MB")
        else:
            print(f"\n📁 {backup_dir}/ (directory not found)")

def cleanup_successful_backups():
    """Remove backup files for successfully processed items."""
    print("🧹 Cleaning up successful backups...")
    
    # Check if files exist in S3 archive (indicating successful processing)
    from config.s3_utils import list_objects
    
    backup_dirs = {
        "backup_pdfs": "data/ProviderBills/pdf/archive/",
        "backup_ocr_files": "data/ProviderBills/txt/archive/"
    }
    
    for backup_dir, s3_archive_prefix in backup_dirs.items():
        if not Path(backup_dir).exists():
            continue
            
        print(f"\n📁 Checking {backup_dir}/")
        
        # Get list of files in S3 archive
        try:
            archived_files = set(list_objects(s3_archive_prefix))
            print(f"   Found {len(archived_files)} files in S3 archive")
        except Exception as e:
            print(f"   Error checking S3 archive: {e}")
            continue
        
        # Check each local backup file
        local_files = list(Path(backup_dir).glob("*"))
        cleaned_count = 0
        
        for file_path in local_files:
            if not file_path.is_file():
                continue
                
            # Check if corresponding file exists in S3 archive
            file_name = file_path.name
            if file_name.endswith('.pdf'):
                s3_key = f"{s3_archive_prefix}{file_name}"
            else:
                # For OCR files, the S3 key might be different
                s3_key = f"{s3_archive_prefix}{file_name}"
            
            if s3_key in archived_files:
                try:
                    file_path.unlink()
                    print(f"   ✅ Removed: {file_name}")
                    cleaned_count += 1
                except Exception as e:
                    print(f"   ❌ Failed to remove {file_name}: {e}")
            else:
                print(f"   ⚠️  Keeping: {file_name} (not in S3 archive)")
        
        print(f"   Cleaned up {cleaned_count} files")

def manual_restore(backup_file, target_location):
    """Manually restore a backup file to a specific location."""
    backup_path = Path(backup_file)
    
    if not backup_path.exists():
        print(f"❌ Backup file not found: {backup_file}")
        return False
    
    target_path = Path(target_location)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        shutil.copy2(backup_path, target_path)
        print(f"✅ Restored {backup_file} to {target_location}")
        return True
    except Exception as e:
        print(f"❌ Failed to restore: {e}")
        return False

def show_help():
    """Show usage information."""
    print("""
🔧 Backup Management Utility

Usage:
  python manage_backups.py [command]

Commands:
  list                    - List all backup files and their sizes
  cleanup                 - Remove backups for successfully processed files
  restore <file> <dest>   - Manually restore a backup file
  help                    - Show this help message

Examples:
  python manage_backups.py list
  python manage_backups.py cleanup
  python manage_backups.py restore backup_pdfs/abc123.pdf /path/to/restore.pdf
""")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        show_help()
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == "list":
        list_backups()
    elif command == "cleanup":
        cleanup_successful_backups()
    elif command == "restore":
        if len(sys.argv) != 4:
            print("❌ Usage: python manage_backups.py restore <backup_file> <target_location>")
            sys.exit(1)
        manual_restore(sys.argv[2], sys.argv[3])
    elif command == "help":
        show_help()
    else:
        print(f"❌ Unknown command: {command}")
        show_help()
        sys.exit(1) 