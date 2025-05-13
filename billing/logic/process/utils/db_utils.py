from typing import List
from .db_queries import get_db_connection

def update_order_line_items_reviewed(order_id: str, bill_id: str, cpt_codes: List[str]) -> bool:
    """
    Update BILL_REVIEWED field for order line items that match the given CPT codes.
    
    Args:
        order_id: The order ID
        bill_id: The provider bill ID to set as reviewed
        cpt_codes: List of CPT codes that were matched
        
    Returns:
        bool: True if update was successful
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Create parameter placeholders for SQL query
    placeholders = ', '.join(['?'] * len(cpt_codes))
    
    cursor.execute(f"""
        UPDATE order_line_items
        SET BILL_REVIEWED = ?
        WHERE Order_ID = ? AND CPT IN ({placeholders})
    """, [bill_id, order_id] + cpt_codes)
    
    conn.commit()
    success = cursor.rowcount > 0
    conn.close()
    return success 