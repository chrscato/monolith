#!/usr/bin/env python3
"""
file_id_debug.py

Debug script to find where ProviderBill IDs from the database are located in S3.
Queries monolith.db for distinct ProviderBill IDs and searches the entire S3 bucket
to locate where each file is stored.

OPTIMIZED VERSION: Fetches all S3 objects once, then does in-memory searches.
"""

import os
import sys
import sqlite3
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[4]
sys.path.append(str(PROJECT_ROOT))

# Load environment variables
load_dotenv(PROJECT_ROOT / ".env")

# Import S3 utilities
from config.s3_utils import list_objects

# S3 Configuration
S3_BUCKET = os.getenv("S3_BUCKET", "bill-review-prod")
DB_PATH = PROJECT_ROOT / "monolith.db"

# Common S3 prefixes to search
S3_PREFIXES = [
    "data/ProviderBills/pdf/",
    "data/ProviderBills/pdf/archive/",
    "data/ProviderBills/txt/",
    "data/ProviderBills/txt/archive/",
    "data/ProviderBills/json/",
    "data/ProviderBills/json/archive/",
    "logs/",
    "backup_pdfs/",
    "backup_ocr_files/"
]

def get_distinct_provider_bill_ids():
    """Get all distinct ProviderBill IDs from the database."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("SELECT DISTINCT id FROM ProviderBill ORDER BY id")
        bill_ids = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        return bill_ids
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return []

def fetch_all_s3_objects():
    """Fetch all S3 objects from relevant prefixes in one go."""
    print("🔍 Fetching all S3 objects...")
    all_objects = {}
    
    for prefix in S3_PREFIXES:
        try:
            print(f"   📁 Fetching: {prefix}")
            objects = list_objects(prefix)
            if objects:
                all_objects[prefix] = set(objects)
                print(f"      Found {len(objects)} objects")
            else:
                all_objects[prefix] = set()
                print(f"      No objects found")
        except Exception as e:
            print(f"   ⚠️  Error fetching {prefix}: {e}")
            all_objects[prefix] = set()
    
    # Also fetch entire bucket as fallback
    try:
        print(f"   📁 Fetching entire bucket (fallback)...")
        all_bucket_objects = list_objects("")
        if all_bucket_objects:
            all_objects["ENTIRE_BUCKET"] = set(all_bucket_objects)
            print(f"      Found {len(all_bucket_objects)} total objects")
        else:
            all_objects["ENTIRE_BUCKET"] = set()
    except Exception as e:
        print(f"   ⚠️  Error fetching entire bucket: {e}")
        all_objects["ENTIRE_BUCKET"] = set()
    
    return all_objects

def find_bill_locations(bill_id, s3_objects):
    """Find all PDF locations for a specific bill ID using pre-fetched S3 objects."""
    locations = []
    
    # Search in common prefixes first
    for prefix, objects in s3_objects.items():
        if prefix == "ENTIRE_BUCKET":
            continue  # Skip for now, use as fallback
        
        for obj in objects:
            if bill_id in obj and obj.endswith('.pdf'):
                locations.append(obj)
    
    # If not found in common prefixes, search entire bucket
    if not locations and "ENTIRE_BUCKET" in s3_objects:
        for obj in s3_objects["ENTIRE_BUCKET"]:
            if bill_id in obj and obj.endswith('.pdf'):
                locations.append(obj)
    
    return locations

def analyze_bill_locations(bill_id, locations):
    """Analyze where PDF files are located and provide status."""
    if not locations:
        return "NOT_FOUND", ""
    
    # All locations should be PDF files now
    if any('archive' in loc for loc in locations):
        status = "PDF_ARCHIVE"
    else:
        status = "PDF_INPUT"
    
    locations_str = " | ".join(locations)
    
    return status, locations_str

def main():
    """Main function to debug file locations."""
    import csv
    
    print("🔍 ProviderBill ID Debug Tool - PDF ONLY")
    print(f"📦 S3 Bucket: {S3_BUCKET}")
    print(f"🗄️  Database: {DB_PATH}")
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Get all ProviderBill IDs from database
    print("🔍 Querying database for ProviderBill IDs...")
    bill_ids = get_distinct_provider_bill_ids()
    
    if not bill_ids:
        print("❌ No ProviderBill IDs found in database")
        return
    
    print(f"📊 Found {len(bill_ids)} distinct ProviderBill IDs")
    
    # Fetch all S3 objects once
    s3_objects = fetch_all_s3_objects()
    
    # Prepare CSV output and collect data for summary
    csv_filename = f"provider_bill_locations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    # Collect data for summary
    results = []
    found_count = 0
    not_found_count = 0
    status_counts = {}
    location_counts = {}
    
    print("🔍 Processing bill IDs...")
    
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write header
        writer.writerow(['bill_id', 'found', 'status', 'locations'])
        
        # Process each bill ID
        for i, bill_id in enumerate(bill_ids, 1):
            if i % 50 == 0:  # Progress indicator every 50 items (much faster now)
                print(f"   Progress: {i}/{len(bill_ids)}")
            
            # Find locations using pre-fetched S3 objects
            locations = find_bill_locations(bill_id, s3_objects)
            
            # Analyze results
            status, locations_str = analyze_bill_locations(bill_id, locations)
            
            # Collect data for summary
            found = "YES" if locations else "NO"
            if locations:
                found_count += 1
                # Count status types
                for status_part in status.split(" | "):
                    status_counts[status_part] = status_counts.get(status_part, 0) + 1
                
                # Count location types
                for location in locations:
                    location_type = location.split('/')[0] if '/' in location else location
                    location_counts[location_type] = location_counts.get(location_type, 0) + 1
            else:
                not_found_count += 1
            
            # Write to CSV
            writer.writerow([bill_id, found, status, locations_str])
            results.append({'bill_id': bill_id, 'found': found, 'status': status, 'locations': locations})
    
    print(f"✅ CSV output saved to: {csv_filename}")
    
    # Print detailed summary
    print("\n" + "="*60)
    print("📊 DETAILED SUMMARY")
    print("="*60)
    
    # Overall statistics
    print(f"📈 OVERALL STATISTICS:")
    print(f"   Total ProviderBill IDs: {len(bill_ids)}")
    print(f"   Found in S3: {found_count}")
    print(f"   Not found in S3: {not_found_count}")
    print(f"   Success rate: {(found_count/len(bill_ids)*100):.1f}%")
    
    # PDF status distribution
    print(f"\n📋 PDF STATUS DISTRIBUTION:")
    if status_counts:
        for status, count in sorted(status_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / found_count * 100) if found_count > 0 else 0
            print(f"   {status}: {count} PDFs ({percentage:.1f}%)")
    else:
        print("   No PDFs found")
    
    # Location distribution
    print(f"\n📍 PDF LOCATION DISTRIBUTION:")
    if location_counts:
        for location, count in sorted(location_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / found_count * 100) if found_count > 0 else 0
            print(f"   {location}: {count} PDFs ({percentage:.1f}%)")
    else:
        print("   No PDFs found")
    
    # Missing PDFs
    if not_found_count > 0:
        print(f"\n❌ MISSING PDFS:")
        print(f"   {not_found_count} ProviderBill IDs have no PDF files in S3")
        print(f"   Check the CSV file for the complete list of missing PDFs")
    
    print("\n" + "="*60)

if __name__ == "__main__":
    main() 