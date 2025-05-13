# billing/logic/process/utils/loader.py

from typing import List, Dict, Optional, Tuple
import logging
from .db_queries import get_db_connection, get_bill_with_line_items, get_order_details, get_order_line_items, get_provider_details

logger = logging.getLogger(__name__)

def load_mapped_bills(limit: Optional[int] = None) -> List[Dict]:
    """
    Load all provider bills with MAPPED status.
    
    Args:
        limit: Optional maximum number of bills to load
        
    Returns:
        List of bill dictionaries
    """
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
    
    logger.info(f"Loaded {len(bills)} MAPPED bills")
    return bills


def load_bill_data(bill_id: str) -> Tuple[Dict, List[Dict], Dict, List[Dict], Optional[Dict]]:
    """
    Load all data needed to process a bill: bill, line items, order, order line items, provider.
    
    Args:
        bill_id: ID of the provider bill to load
        
    Returns:
        Tuple containing:
        - bill: The bill record
        - bill_items: Bill line items
        - order: Associated order record (or empty dict if not found)
        - order_items: Order line items (or empty list if not found)
        - provider: Provider record (or None if not found)
    """
    logger.debug(f"Loading data for bill {bill_id}")
    
    # Get bill and its line items
    bill, bill_items = get_bill_with_line_items(bill_id)
    
    # Get order data if available
    order = {}
    order_items = []
    provider = None
    
    if bill.get('claim_id'):
        order_id = bill['claim_id']
        logger.debug(f"Found claim_id {order_id}, loading order data")
        
        try:
            order = get_order_details(order_id)
            order_items = get_order_line_items(order_id)
            
            # Get provider if available
            if order.get('provider_id'):
                provider_id = order['provider_id']
                logger.debug(f"Order {order_id} has provider_id: {provider_id}")
                provider = get_provider_details(provider_id)
                if provider:
                    logger.debug(f"Provider details loaded: {provider}")
                else:
                    logger.warning(f"Provider details not found for provider_id: {provider_id}")
            else:
                logger.warning(f"Order {order_id} has no provider_id")
                
        except Exception as e:
            logger.error(f"Error loading order data: {str(e)}")
    else:
        logger.warning(f"Bill {bill_id} has no claim_id, skipping order data")
    
    return bill, bill_items, order, order_items, provider