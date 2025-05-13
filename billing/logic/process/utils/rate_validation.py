from typing import Dict, List, Optional, Tuple
import logging
from .db_queries import (
    get_in_network_rate,
    get_out_of_network_rate,
    update_line_item,
    update_bill_status
)
from .validation import load_ancillary_codes

logger = logging.getLogger(__name__)

def validate_line_item_rate(
    bill_id: str,
    line_item: Dict,
    provider: Dict,
    order_id: str
) -> Tuple[bool, Optional[float], Optional[str]]:
    """
    Validate the rate for a single line item based on provider network status.
    
    Args:
        bill_id: The provider bill ID
        line_item: The bill line item to validate
        provider: The provider record
        order_id: The order ID
        
    Returns:
        Tuple containing:
        - bool: Whether validation was successful
        - float: The allowed amount (None if validation failed)
        - str: Reason code if validation failed
    """
    # Get CPT code and modifier
    cpt_code = line_item.get('cpt_code', '').strip()
    modifier = line_item.get('modifier', '').strip() or None
    
    if not cpt_code:
        return False, None, "missing_cpt"
        
    # Check if this is an ancillary code
    ancillary_codes = load_ancillary_codes()
    if cpt_code in ancillary_codes:
        # Ancillary codes get $0 rate
        return True, 0.0, None
        
    # Get provider network status
    network_status = provider.get('Provider Network', '').strip()
    if not network_status:
        return False, None, "missing_network_status"
        
    # Get TIN for in-network rate lookup
    tin = provider.get('TIN', '').strip()
    if network_status == 'In Network' and not tin:
        return False, None, "missing_tin"
        
    # Get rate based on network status
    rate = None
    if network_status == 'In Network':
        rate = get_in_network_rate(tin, cpt_code, modifier)
    elif network_status == 'Out of Network':
        rate = get_out_of_network_rate(order_id, cpt_code, modifier)
    else:
        return False, None, "invalid_network_status"
        
    if rate is None:
        return False, None, "no_rate_found"
        
    return True, rate, None

def validate_bill_rates(
    bill_id: str,
    bill_items: List[Dict],
    provider: Dict,
    order_id: str
) -> Dict:
    """
    Validate rates for all line items in a bill.
    
    Args:
        bill_id: The provider bill ID
        bill_items: List of bill line items
        provider: The provider record
        order_id: The order ID
        
    Returns:
        Dict containing validation results:
        - is_valid: Whether all line items were validated successfully
        - line_items: List of validation results for each line item
        - error: Error message if validation failed
    """
    results = {
        'is_valid': True,
        'line_items': [],
        'error': None
    }
    
    for item in bill_items:
        success, rate, reason = validate_line_item_rate(
            bill_id=bill_id,
            line_item=item,
            provider=provider,
            order_id=order_id
        )
        
        line_result = {
            'line_id': item['id'],
            'cpt_code': item.get('cpt_code', ''),
            'success': success,
            'rate': rate,
            'reason': reason
        }
        results['line_items'].append(line_result)
        
        if not success:
            results['is_valid'] = False
            results['error'] = f"Rate validation failed for CPT {item.get('cpt_code', '')}: {reason}"
            
            # Update line item with failure
            update_line_item(
                line_id=item['id'],
                decision='REJECTED',
                allowed_amount=None,
                reason_code=reason
            )
        else:
            # Update line item with success
            update_line_item(
                line_id=item['id'],
                decision='APPROVED',
                allowed_amount=rate,
                reason_code=None
            )
    
    # Update bill status based on validation results
    if results['is_valid']:
        update_bill_status(
            bill_id=bill_id,
            status='REVIEWED',
            action='apply_rate',
            error=None
        )
    else:
        update_bill_status(
            bill_id=bill_id,
            status='FLAGGED',
            action='review_rates',
            error=results['error']
        )
    
    return results
