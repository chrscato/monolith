#!/usr/bin/env python3
"""
map_bill.py

Maps validated ProviderBill records to claims.
Updates status and action based on mapping results.
"""
import os
import sys
import logging
import sqlite3
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import re
from difflib import SequenceMatcher

# Get the project root directory (2 levels up from this file)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

# Load environment variables from the root .env file
load_dotenv(PROJECT_ROOT / '.env')

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
    """
    if not name:
        return ""
    
    # Convert to lowercase and strip
    name = name.lower().strip()
    
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
    Format name as last,first for matching
    """
    last_name = clean_name(last_name)
    first_name = clean_name(first_name)
    return f"{last_name}, {first_name}"

def similar(a: str, b: str) -> float:
    """
    Return similarity ratio between two strings
    """
    return SequenceMatcher(None, a, b).ratio()

def clean_dos(date_str: str) -> datetime:
    """
    Clean and normalize date of service to YYYY-MM-DD format.
    Handles various input formats and returns datetime object.
    """
    if not date_str:
        return None
        
    date_str = str(date_str).strip()
    
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
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
            
    return None

def find_matching_claim(bill: dict, cursor: sqlite3.Cursor) -> str:
    """
    Find a matching claim for the bill based on:
    1. Patient name match (last, first) from Orders table (2024-2025 only)
    2. Date of service within 3 weeks
    3. CPT code match (if multiple matches found)
    Returns claim_id if found, None otherwise.
    """
    # Get all orders from 2024-2025 with their line items
    cursor.execute("""
        SELECT DISTINCT o.Order_ID, o.Patient_Last_Name, o.Patient_First_Name, oli.DOS 
        FROM Orders o
        JOIN Orders_Line_Items oli ON o.Order_ID = oli.order_id
        WHERE oli.DOS >= '2024-01-01' 
        AND oli.DOS <= '2025-12-31'
    """)
    orders = cursor.fetchall()
    
    if not orders:
        return None

    # Get the bill's patient name to match against
    bill_patient_name = clean_name(bill['patient_name'])
    
    # Find orders with matching patient names
    matching_orders = []
    for order_id, last_name, first_name, order_date in orders:
        if not last_name or not first_name:
            continue
            
        # Create last,first format from order
        order_patient_name = format_name_for_matching(last_name, first_name)
        
        # Check for exact match first
        if order_patient_name == bill_patient_name:
            matching_orders.append((order_id, order_date, 1.0))  # 1.0 for exact match
        else:
            # Try fuzzy matching if no exact match
            similarity = similar(order_patient_name, bill_patient_name)
            if similarity >= 0.85:  # 85% similarity threshold
                matching_orders.append((order_id, order_date, similarity))
    
    if not matching_orders:
        return None
    
    # Sort by similarity score (highest first)
    matching_orders.sort(key=lambda x: x[2], reverse=True)
    
    # Get the bill's service dates and CPT codes
    cursor.execute("""
        SELECT date_of_service, cpt_code
        FROM BillLineItem 
        WHERE provider_bill_id = ?
        ORDER BY date_of_service
    """, (bill['id'],))
    bill_items = cursor.fetchall()
    
    if not bill_items:
        return None

    # Normalize bill dates and collect CPT codes
    normalized_bill_items = []
    for date_str, cpt_code in bill_items:
        date_obj = clean_dos(date_str)
        if date_obj:
            normalized_bill_items.append((date_obj, cpt_code.strip() if cpt_code else None))

    if not normalized_bill_items:
        return None

    # Find orders with dates within 3 weeks of any bill date
    date_matching_orders = []
    for order_id, order_date, similarity in matching_orders:
        order_date_obj = clean_dos(order_date)
        if not order_date_obj:
            continue
            
        for bill_date, _ in normalized_bill_items:
            # Check if dates are within 3 weeks (21 days)
            if abs((order_date_obj - bill_date).days) <= 21:
                date_matching_orders.append((order_id, similarity))
                break

    if not date_matching_orders:
        return None

    # If only one match, return it
    if len(date_matching_orders) == 1:
        return date_matching_orders[0][0]

    # If multiple matches, use CPT codes to break tie
    # Get CPT codes for all matching orders
    order_cpt_matches = {}
    for order_id, similarity in date_matching_orders:
        cursor.execute("""
            SELECT cpt_code
            FROM Orders_Line_Items
            WHERE order_id = ?
        """, (order_id,))
        order_cpts = [row[0].strip() for row in cursor.fetchall() if row[0]]
        
        # Count matching CPT codes
        bill_cpts = [cpt for _, cpt in normalized_bill_items if cpt]
        matching_cpts = set(order_cpts) & set(bill_cpts)
        # Weight by both CPT matches and name similarity
        order_cpt_matches[order_id] = len(matching_cpts) * similarity

    # Return order with highest combined score
    if order_cpt_matches:
        return max(order_cpt_matches.items(), key=lambda x: x[1])[0]
    
    # If no CPT matches, return first order (highest name similarity)
    return date_matching_orders[0][0]

def map_provider_bill(bill_id: str, cursor: sqlite3.Cursor) -> tuple[str, str, str]:
    """
    Map a ProviderBill record to a claim.
    Returns (status, action, error_message)
    """
    # Get the ProviderBill record
    cursor.execute("""
        SELECT * FROM ProviderBill WHERE id = ?
    """, (bill_id,))
    bill = cursor.fetchone()
    
    if not bill:
        return 'INVALID', 'to_validate', f"ProviderBill {bill_id} not found"
    
    # Check if bill is in correct state for mapping
    if bill['status'] != 'VALID' or bill['action'] != 'to_map':
        return bill['status'], bill['action'], "Bill not ready for mapping"
    
    # Try to find a matching claim
    claim_id = find_matching_claim(bill, cursor)
    
    if claim_id:
        # Check if the order is already fully paid
        cursor.execute("""
            SELECT FULLY_PAID, BILLS_REC
            FROM Orders 
            WHERE Order_ID = ?
        """, (claim_id,))
        order_status = cursor.fetchone()
        
        if order_status and order_status[0] == 'Y':
            # Update BILLS_REC count
            cursor.execute("""
                UPDATE Orders 
                SET BILLS_REC = BILLS_REC + 1
                WHERE Order_ID = ?
            """, (claim_id,))
            
            # Update bill status to DUPLICATE
            cursor.execute("""
                UPDATE ProviderBill 
                SET claim_id = ?,
                    status = ?,
                    action = ?,
                    last_error = ?
                WHERE id = ?
            """, (claim_id, 'DUPLICATE', 'to_review', 'Order already fully paid', bill_id))
            return 'DUPLICATE', 'to_review', 'Order already fully paid'
        
        # If not fully paid, proceed with normal mapping
        cursor.execute("""
            UPDATE ProviderBill 
            SET claim_id = ?,
                status = ?,
                action = ?,
                last_error = NULL
            WHERE id = ?
        """, (claim_id, 'MAPPED', 'to_review', bill_id))
        return 'MAPPED', 'to_review', None
    else:
        # No matching claim found
        error = "No matching claim found for patient and dates"
        cursor.execute("""
            UPDATE ProviderBill 
            SET status = ?,
                action = ?,
                last_error = ?
            WHERE id = ?
        """, ('UNMAPPED', 'to_map', error, bill_id))
        return 'UNMAPPED', 'to_map', error

def process_mapping():
    """Process all ProviderBill records that need mapping."""
    db_path = PROJECT_ROOT / 'monolith.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Get all bills that need mapping
        cursor.execute("""
            SELECT id FROM ProviderBill 
            WHERE status = 'VALID' AND action = 'to_map'
        """)
        bills = cursor.fetchall()
        
        logger.info(f"Found {len(bills)} bills to map")
        
        for (bill_id,) in bills:
            logger.info(f"Mapping bill {bill_id}")
            
            # Perform mapping
            status, action, error = map_provider_bill(bill_id, cursor)
            
            logger.info(f"Updated bill {bill_id}: status={status}, action={action}")
            if error:
                logger.warning(f"Mapping error: {error}")
        
        conn.commit()
        logger.info("Mapping complete")
        
    except sqlite3.Error as e:
        conn.rollback()
        logger.error(f"Database error: {str(e)}")
    finally:
        conn.close()

if __name__ == '__main__':
    process_mapping()
