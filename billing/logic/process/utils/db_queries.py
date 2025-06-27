# billing/logic/process/utils/db_utils.py

import sqlite3
from typing import Dict, List, Tuple, Optional, Any, Set
import logging
import os


def get_db_connection(db_path: str = None) -> sqlite3.Connection:
    """Get a connection to the SQLite database."""
    if db_path is None:
        db_path = os.getenv("MONOLITH_DB_PATH", "monolith.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # Return results as dictionaries
    return conn


def get_mapped_bills(limit: Optional[int] = None) -> List[Dict]:
    """Get all provider bills with MAPPED status."""
    conn = get_db_connection()
    query = """
        SELECT * FROM ProviderBill 
        WHERE status = 'MAPPED'
        ORDER BY created_at DESC
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    cursor = conn.cursor()
    cursor.execute(query)
    bills = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return bills


def get_bill_with_line_items(bill_id: str) -> Tuple[Dict, List[Dict]]:
    """Get a bill with all its line items."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get the bill
    cursor.execute("SELECT * FROM ProviderBill WHERE id = ?", (bill_id,))
    bill = dict(cursor.fetchone())
    
    # Get the bill line items
    cursor.execute("""
        SELECT * FROM BillLineItem 
        WHERE provider_bill_id = ?
        ORDER BY date_of_service
    """, (bill_id,))
    line_items = [dict(row) for row in cursor.fetchall()]
    
    conn.close()
    return bill, line_items


def get_order_details(order_id: str) -> Dict:
    """Get all details for a specific order."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT o.*, p.PrimaryKey as provider_primary_key
        FROM orders o
        LEFT JOIN providers p ON o.provider_id = p.PrimaryKey
        WHERE o.Order_ID = ?
    """, (order_id,))
    order = dict(cursor.fetchone())
    
    # Log the provider_id for debugging
    logging.getLogger(__name__).debug(f"Order {order_id} provider_id: {order.get('provider_id')}")
    
    conn.close()
    return order


def get_order_line_items(order_id: str) -> List[Dict]:
    """Get all line items for a specific order."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM order_line_items
        WHERE Order_ID = ?
        ORDER BY line_number
    """, (order_id,))
    line_items = [dict(row) for row in cursor.fetchall()]
    
    conn.close()
    return line_items


def get_provider_details(provider_id: str) -> Dict:
    """Get provider details using provider_id."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            "DBA Name Billing Name", "Billing Name", 
            "Address Line 1", "Address Line 2", "City", "State", "Postal Code", 
            "Billing Address 1", "Billing Address 2", "Billing Address City", 
            "Billing Address State", "Billing Address Postal Code", 
            "Phone", "Fax Number", "TIN", "NPI", 
            "Provider Network", "Provider Type", "Provider Status"
        FROM providers
        WHERE PrimaryKey = ?
    """, (provider_id,))
    row = cursor.fetchone()
    provider = dict(row) if row else {}
    
    conn.close()
    return provider


def get_cpt_categories(cpt_codes: List[str]) -> Dict[str, Tuple[str, str]]:
    """
    Get category and subcategory for multiple CPT codes.
    Returns a dictionary mapping CPT codes to (category, subcategory) tuples.
    """
    if not cpt_codes:
        return {}
        
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Create parameter placeholders for SQL query
    placeholders = ', '.join(['?'] * len(cpt_codes))
    
    cursor.execute(f"""
        SELECT proc_cd, category, subcategory
        FROM dim_proc
        WHERE proc_cd IN ({placeholders})
    """, cpt_codes)
    
    results = {}
    for row in cursor.fetchall():
        results[row['proc_cd']] = (row['category'], row['subcategory'])
    
    conn.close()
    return results


def clean_tin(tin: str) -> str:
    """Clean TIN by removing dashes and spaces."""
    return tin.replace('-', '').replace(' ', '').strip()


def get_in_network_rate(tin: str, cpt_code: str, modifier: Optional[str] = None) -> Optional[float]:
    """Get in-network rate for a specific provider and CPT code."""
    logger = logging.getLogger(__name__)
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Clean the TIN
    clean_tin_value = clean_tin(tin)
    
    # Only include modifier if it's TC or 26
    effective_modifier = modifier if modifier in ['TC', '26'] else None
    logger.info(f"Looking up in-network rate for TIN {clean_tin_value}, CPT {cpt_code}, modifier {effective_modifier}")
    
    if effective_modifier:
        # If we have a TC or 26 modifier, only look for that specific rate
        cursor.execute("""
            SELECT rate
            FROM ppo
            WHERE REPLACE(REPLACE(TIN, '-', ''), ' ', '') = ? 
            AND proc_cd = ? 
            AND modifier = ?
            LIMIT 1
        """, (clean_tin_value, cpt_code, effective_modifier))
    else:
        # If no modifier or not TC/26, look for rate without modifier
        cursor.execute("""
            SELECT rate
            FROM ppo
            WHERE REPLACE(REPLACE(TIN, '-', ''), ' ', '') = ? 
            AND proc_cd = ? 
            AND (modifier IS NULL OR modifier = '')
            LIMIT 1
        """, (clean_tin_value, cpt_code))
    
    row = cursor.fetchone()
    rate = float(row['rate']) if row and row['rate'] else None
    
    if rate:
        logger.info(f"Found in-network rate {rate} for CPT {cpt_code}" + 
                   (f" with modifier {effective_modifier}" if effective_modifier else " without modifier"))
    else:
        logger.warning(f"No in-network rate found for CPT {cpt_code}" + 
                      (f" with modifier {effective_modifier}" if effective_modifier else ""))
    
    conn.close()
    return rate


def get_out_of_network_rate(order_id: str, cpt_code: str, modifier: Optional[str] = None) -> Optional[float]:
    """Get out-of-network rate for a specific order and CPT code."""
    logger = logging.getLogger(__name__)
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Only include modifier if it's TC or 26
    effective_modifier = modifier if modifier in ['TC', '26'] else None
    logger.info(f"Looking up out-of-network rate for order {order_id}, CPT {cpt_code}, modifier {effective_modifier}")
    
    if effective_modifier:
        # If we have a TC or 26 modifier, only look for that specific rate
        cursor.execute("""
            SELECT rate
            FROM ota
            WHERE ID_Order_PrimaryKey = ? 
            AND CPT = ? 
            AND modifier = ?
            LIMIT 1
        """, (order_id, cpt_code, effective_modifier))
    else:
        # If no modifier or not TC/26, look for rate without modifier
        cursor.execute("""
            SELECT rate
            FROM ota
            WHERE ID_Order_PrimaryKey = ? 
            AND CPT = ? 
            AND (modifier IS NULL OR modifier = '')
            LIMIT 1
        """, (order_id, cpt_code))
    
    row = cursor.fetchone()
    rate = float(row['rate']) if row and row['rate'] else None
    
    if rate:
        logger.info(f"Found out-of-network rate {rate} for CPT {cpt_code}" + 
                   (f" with modifier {effective_modifier}" if effective_modifier else " without modifier"))
    else:
        logger.warning(f"No out-of-network rate found for CPT {cpt_code}" + 
                      (f" with modifier {effective_modifier}" if effective_modifier else ""))
    
    conn.close()
    return rate


def update_bill_status(bill_id: str, status: str, action: str, error: Optional[str] = None) -> bool:
    """Update the status, action, and error message of a provider bill."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE ProviderBill
        SET status = ?, action = ?, last_error = ?
        WHERE id = ?
    """, (status, action, error, bill_id))
    
    conn.commit()
    success = cursor.rowcount > 0
    conn.close()
    return success


def update_line_item(line_id: int, decision: str, allowed_amount: Optional[float] = None, 
                     reason_code: Optional[str] = None) -> bool:
    """Update a bill line item with decision and allowed amount."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE BillLineItem
        SET decision = ?, allowed_amount = ?, reason_code = ?
        WHERE id = ?
    """, (decision, allowed_amount, reason_code, line_id))
    
    conn.commit()
    success = cursor.rowcount > 0
    conn.close()
    return success