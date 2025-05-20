#!/usr/bin/env python3
"""
map_bill_diagnostic.py

Diagnostic script to show name and date comparisons for bill mapping.
"""
import os
import sys
import logging
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
import re
from difflib import SequenceMatcher

# Get the project root directory (2 levels up from this file) for imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

# Load environment variables from the root .env file
load_dotenv(PROJECT_ROOT / '.env')

# Get the absolute path to the monolith root directory
DB_ROOT = Path(r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\monolith")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_name(name: str) -> str:
    """
    Clean a name by:
    1. Converting to lowercase
    2. Removing special characters
    3. Removing extra spaces
    4. Removing common suffixes (jr, sr, iii, etc)
    5. Removing commas
    """
    if not name:
        return ""
    
    # Convert to lowercase and strip
    name = name.lower().strip()
    
    # Remove commas
    name = name.replace(',', '')
    
    # Remove special characters but keep spaces and hyphens
    name = re.sub(r'[^a-z\s-]', '', name)
    
    # Remove common suffixes
    suffixes = ['jr', 'sr', 'ii', 'iii', 'iv', 'v', 'phd', 'md', 'do']
    for suffix in suffixes:
        name = re.sub(rf'\b{suffix}\b', '', name)
    
    # Remove extra spaces and hyphens
    name = re.sub(r'\s+', ' ', name)
    name = re.sub(r'-+', '-', name)
    
    return name.strip()

def format_name_for_matching(last_name: str, first_name: str) -> str:
    """
    Format name as last first for matching.
    Both last_name and first_name are cleaned before formatting.
    """
    last_name = clean_name(last_name)
    first_name = clean_name(first_name)
    return f"{last_name} {first_name}"

def normalize_date(date_str: str) -> datetime.date:
    """
    Normalize a date string to a date object.
    Handles various input formats and validates the result.
    Returns None if date cannot be normalized.
    """
    if not date_str:
        return None
        
    date_str = str(date_str).strip()
    
    # Handle date ranges (take the first date)
    if ' - ' in date_str:
        date_str = date_str.split(' - ')[0].strip()
    
    # Common date formats to try
    formats = [
        '%Y-%m-%d',      # 2024-01-01
        '%m/%d/%Y',      # 01/01/2024
        '%m-%d-%Y',      # 01-01-2024
        '%Y/%m/%d',      # 2024/01/01
        '%m/%d/%y',      # 01/01/24
        '%m-%d-%y',      # 01-01-24
    ]
    
    for fmt in formats:
        try:
            date_obj = datetime.strptime(date_str, fmt).date()
            # Validate year is reasonable (between 2020 and 2030)
            if 2020 <= date_obj.year <= 2030:
                return date_obj
        except ValueError:
            continue
            
    return None

def similar(a: str, b: str) -> float:
    """
    Return similarity ratio between two strings
    """
    return SequenceMatcher(None, a, b).ratio()

def show_name_comparisons(bill_id: str):
    """Show name comparisons for a specific bill."""
    db_path = DB_ROOT / 'monolith.db'
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    try:
        # Get the bill
        cursor.execute("""
            SELECT * FROM ProviderBill WHERE id = ?
        """, (bill_id,))
        bill = cursor.fetchone()
        
        if not bill:
            print(f"Bill {bill_id} not found")
            return
            
        # Get the bill's patient name
        bill_patient_name = clean_name(bill['patient_name'])
        print(f"\nProviderBill Name Comparison:")
        print(f"Original: {bill['patient_name']}")
        print(f"Cleaned:  {bill_patient_name}")
        
        # Get bill dates
        cursor.execute("""
            SELECT date_of_service
            FROM BillLineItem 
            WHERE provider_bill_id = ?
            ORDER BY date_of_service
        """, (bill_id,))
        bill_items = cursor.fetchall()
        
        print("\nBill Dates:")
        bill_dates = []
        for item in bill_items:
            original_date = item['date_of_service']
            normalized_date = normalize_date(original_date)
            print(f"Original: {original_date}")
            print(f"Normalized: {normalized_date}")
            if normalized_date:
                bill_dates.append(normalized_date)
        
        if bill_dates:
            print(f"\nBill Date Range:")
            print(f"Earliest: {min(bill_dates)}")
            print(f"Latest:   {max(bill_dates)}")
        
        # Get all potential matching orders
        cursor.execute("""
            SELECT DISTINCT o.Order_ID, o.Patient_Last_Name, o.Patient_First_Name, oli.DOS 
            FROM Orders o
            JOIN order_line_items oli ON o.Order_ID = oli.order_id
            WHERE oli.DOS >= '2024-01-01' 
            AND oli.DOS <= '2025-12-31'
        """)
        orders = cursor.fetchall()
        
        # Calculate similarity scores for all orders
        order_matches = []
        for order in orders:
            order_patient_name = format_name_for_matching(order['Patient_Last_Name'], order['Patient_First_Name'])
            similarity = similar(bill_patient_name, order_patient_name)
            original_dos = order['DOS']
            normalized_dos = normalize_date(original_dos)
            
            # Calculate date proximity if we have both dates
            date_proximity = None
            if normalized_dos and bill_dates:
                # Find closest date in bill_dates
                min_days_diff = min(abs((normalized_dos - bill_date).days) for bill_date in bill_dates)
                date_proximity = min_days_diff
            
            order_matches.append({
                'order_id': order['Order_ID'],
                'original_name': f"{order['Patient_Last_Name']}, {order['Patient_First_Name']}",
                'cleaned_name': order_patient_name,
                'similarity': similarity,
                'original_dos': original_dos,
                'normalized_dos': normalized_dos,
                'date_proximity': date_proximity
            })
        
        # Sort by similarity score (highest first)
        order_matches.sort(key=lambda x: x['similarity'], reverse=True)
        
        # Show top 10 matches
        print("\nTop 10 Name Matches:")
        for match in order_matches[:10]:
            print(f"\nOrder ID: {match['order_id']}")
            print(f"Original: {match['original_name']}")
            print(f"Cleaned:  {match['cleaned_name']}")
            print(f"Similarity: {match['similarity']:.2f}")
            print(f"Original DOS: {match['original_dos']}")
            print(f"Normalized DOS: {match['normalized_dos']}")
            if match['date_proximity'] is not None:
                print(f"Days from bill date: {match['date_proximity']}")
            
        # Show summary
        print(f"\nTotal orders checked: {len(order_matches)}")
        print(f"Best match similarity: {order_matches[0]['similarity']:.2f}")
        print(f"Number of matches above 0.85 threshold: {sum(1 for m in order_matches if m['similarity'] >= 0.85)}")
        
        # Show date proximity summary
        if bill_dates:
            print("\nDate Proximity Summary:")
            exact_date_matches = sum(1 for m in order_matches if m['date_proximity'] == 0)
            within_week = sum(1 for m in order_matches if m['date_proximity'] is not None and m['date_proximity'] <= 7)
            within_month = sum(1 for m in order_matches if m['date_proximity'] is not None and m['date_proximity'] <= 30)
            print(f"Exact date matches: {exact_date_matches}")
            print(f"Within 7 days: {within_week}")
            print(f"Within 30 days: {within_month}")
            
    finally:
        conn.close()

def show_dos_comparisons(bill_id: str):
    """Show date of service comparisons for a specific bill."""
    db_path = DB_ROOT / 'monolith.db'
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    try:
        # Get the bill's line items
        cursor.execute("""
            SELECT date_of_service, cpt_code
            FROM BillLineItem 
            WHERE provider_bill_id = ?
            ORDER BY date_of_service
        """, (bill_id,))
        bill_items = cursor.fetchall()
        
        print(f"\nProviderBill Dates:")
        bill_dates = []
        for item in bill_items:
            original_date = item['date_of_service']
            normalized_date = normalize_date(original_date)
            print(f"Original: {original_date}")
            print(f"Normalized: {normalized_date}")
            if normalized_date:
                bill_dates.append(normalized_date)
        
        if bill_dates:
            print(f"\nBill Date Range:")
            print(f"Earliest: {min(bill_dates)}")
            print(f"Latest:   {max(bill_dates)}")
            
        # Get potential matching orders
        cursor.execute("""
            SELECT DISTINCT o.Order_ID, oli.DOS 
            FROM Orders o
            JOIN order_line_items oli ON o.Order_ID = oli.order_id
            WHERE oli.DOS >= '2024-01-01' 
            AND oli.DOS <= '2025-12-31'
            LIMIT 20
        """)
        orders = cursor.fetchall()
        
        print("\nSample Order Dates (showing top 20 by date):")
        for order in orders:
            original_dos = order['DOS']
            normalized_dos = normalize_date(original_dos)
            print(f"\nOrder ID: {order['Order_ID']}")
            print(f"Original: {original_dos}")
            print(f"Normalized: {normalized_dos}")
            if normalized_dos and bill_dates:
                min_days_diff = min(abs((normalized_dos - bill_date).days) for bill_date in bill_dates)
                print(f"Days from bill date: {min_days_diff}")
            
    finally:
        conn.close()

def main():
    """Main function to run diagnostics."""
    if len(sys.argv) != 2:
        print("Usage: python map_bill_diagnostic.py <bill_id>")
        sys.exit(1)
        
    bill_id = sys.argv[1]
    
    print("=" * 50)
    print(f"Running diagnostics for bill {bill_id}")
    print("=" * 50)
    
    show_name_comparisons(bill_id)
    show_dos_comparisons(bill_id)

if __name__ == '__main__':
    main() 