# billing/logic/process/utils/arthrogram.py

from typing import Dict, Optional
from .db_queries import get_order_details, update_bill_status, get_order_line_items
import logging

logger = logging.getLogger(__name__)

def check_arthrogram(bill_id: str, order_id: str) -> bool:
    """
    Check if this bill is for an arthrogram procedure.
    
    Args:
        bill_id: The provider bill ID
        order_id: The order ID
        
    Returns:
        bool: True if this is an arthrogram, False otherwise
    """
    logger.info(f"Checking if bill {bill_id} is for an arthrogram")
    
    # Get order details
    order = get_order_details(order_id)
    if not order:
        logger.warning(f"Order {order_id} not found")
        return False
        
    # Check if this is an arthrogram bundle
    bundle_type = order.get('bundle_type', '') or ''  # Convert None to empty string
    if bundle_type.lower() == 'arthrogram':
        logger.info(f"Bill {bill_id} is for an arthrogram bundle")
        return True
        
    # Check line items for arthrogram CPT codes
    line_items = get_order_line_items(order_id)
    arthrogram_cpts = {'20610', '20611', '77002', '77003', '77021'}
    
    for item in line_items:
        cpt = item.get('CPT', '').strip()
        if cpt in arthrogram_cpts:
            logger.info(f"Bill {bill_id} contains arthrogram CPT code {cpt}")
            return True
            
    logger.info(f"Bill {bill_id} is not for an arthrogram")
    return False