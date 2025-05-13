# billing/logic/process/utils/validation.py

from typing import Dict, List, Tuple, Set, Optional
import json
import os
from pathlib import Path
from .db_queries import get_cpt_categories
import logging

def load_ancillary_codes() -> Set[str]:
    """Load list of ancillary CPT codes that should be ignored in validation."""
    data_path = Path(__file__).parent.parent / 'data' / 'ancillary_codes.json'
    try:
        with open(data_path, 'r') as f:
            data = json.load(f)
            return set(data.get('ignored_cpt_codes', []))
    except (FileNotFoundError, json.JSONDecodeError):
        # Fallback to a minimal set if file not found
        return {
            "36415", "36416", "99000", "99001", "A4550", "A4556", 
            "A4558", "A4570", "A4580", "A4590", "Q4001", "T1015"
        }


def compare_cpt_codes(bill_items: List[Dict], order_items: List[Dict]) -> Dict:
    """
    Flexible CPT code comparison between billed and ordered items.
    Handles many-to-many relationships between line items.
    
    Returns:
        Dict containing:
        - exact_matches: CPT codes that match exactly
        - billed_not_ordered: CPT codes billed but not ordered
        - ordered_not_billed: CPT codes ordered but not billed
        - category_matches: CPT codes that match by category but not code
        - category_mismatches: CPT codes with no category match
    """
    logger = logging.getLogger(__name__)
    
    # Get unique CPT codes from each source with counts
    billed_cpts = {}
    ordered_cpts = {}
    
    for item in bill_items:
        cpt = item['cpt_code'].strip() if item['cpt_code'] else ""
        if cpt:
            billed_cpts[cpt] = billed_cpts.get(cpt, 0) + 1
            logger.debug(f"Billed CPT: {cpt}")
            
    for item in order_items:
        cpt = item['CPT'].strip() if item['CPT'] else ""
        if cpt:
            ordered_cpts[cpt] = ordered_cpts.get(cpt, 0) + 1
            logger.debug(f"Ordered CPT: {cpt}")
    
    # Find exact matches and differences
    exact_matches = []
    for cpt in set(billed_cpts.keys()).intersection(set(ordered_cpts.keys())):
        exact_matches.append({
            'cpt': cpt,
            'billed_count': billed_cpts[cpt],
            'ordered_count': ordered_cpts[cpt]
        })
        logger.info(f"Exact match found for CPT {cpt}")
    
    billed_not_ordered = list(set(billed_cpts.keys()) - set(ordered_cpts.keys()))
    ordered_not_billed = list(set(ordered_cpts.keys()) - set(billed_cpts.keys()))
    
    # Filter out ancillary codes that should be ignored
    ancillary_codes = load_ancillary_codes()
    logger.debug(f"Ancillary codes: {ancillary_codes}")
    
    billed_not_ordered = [cpt for cpt in billed_not_ordered if cpt not in ancillary_codes]
    ordered_not_billed = [cpt for cpt in ordered_not_billed if cpt not in ancillary_codes]
    
    # Get categories for codes that don't match exactly
    unmatched_cpts = billed_not_ordered + ordered_not_billed
    if not unmatched_cpts:
        return {
            'exact_matches': exact_matches,
            'billed_not_ordered': [],
            'ordered_not_billed': [],
            'category_matches': [],
            'category_mismatches': []
        }
    
    # Get category information
    categories = get_cpt_categories(unmatched_cpts)
    logger.info(f"Category lookup results: {categories}")
    
    # Build category mapping
    billed_categories = {}
    ordered_categories = {}
    
    for cpt in billed_not_ordered:
        if cpt in categories:
            cat_key = categories[cpt]
            if cat_key not in billed_categories:
                billed_categories[cat_key] = []
            billed_categories[cat_key].append(cpt)
            logger.info(f"Billed CPT {cpt} maps to category {cat_key}")
    
    for cpt in ordered_not_billed:
        if cpt in categories:
            cat_key = categories[cpt]
            if cat_key not in ordered_categories:
                ordered_categories[cat_key] = []
            ordered_categories[cat_key].append(cpt)
            logger.info(f"Ordered CPT {cpt} maps to category {cat_key}")
    
    # Find category matches
    category_matches = []
    category_mismatches = []
    
    for cat_key, billed_cpts_in_cat in billed_categories.items():
        if cat_key in ordered_categories:
            # We have a category match
            for billed_cpt in billed_cpts_in_cat:
                category_matches.append({
                    'billed_cpt': billed_cpt,
                    'ordered_cpts': ordered_categories[cat_key],
                    'category': cat_key[0],
                    'subcategory': cat_key[1]
                })
                logger.info(f"Category match: Billed CPT {billed_cpt} matches ordered CPTs {ordered_categories[cat_key]} in category {cat_key}")
        else:
            # No category match
            for billed_cpt in billed_cpts_in_cat:
                category_mismatches.append({
                    'cpt': billed_cpt,
                    'category': cat_key[0],
                    'subcategory': cat_key[1]
                })
                logger.info(f"No category match found for billed CPT {billed_cpt} in category {cat_key}")
    
    return {
        'exact_matches': exact_matches,
        'billed_not_ordered': billed_not_ordered,
        'ordered_not_billed': ordered_not_billed,
        'category_matches': category_matches,
        'category_mismatches': category_mismatches
    }


def validate_provider_info(bill: Dict, provider: Dict) -> Dict[str, bool]:
    """
    Validate provider information by checking required fields are not null.
    
    Returns:
        Dict with validation results for each field
    """
    required_fields = [
        "Billing Name",
        "Billing Address 1",
        "Billing Address City",
        "Billing Address State",
        "Billing Address Postal Code",
        "TIN",
        "Provider Network",
        "DBA Name Billing Name"
    ]
    
    results = {}
    
    # Check each required field
    for field in required_fields:
        value = provider.get(field)
        results[f"{field.lower().replace(' ', '_')}_present"] = bool(value)
    
    # Overall validation result - all fields must be present
    results['is_valid'] = all(results.values())
    
    return results


def validate_units(bill_items: List[Dict]) -> Dict:
    """
    Check if any non-ancillary CPT codes have more than 1 unit.
    
    Returns:
        Dict with validation results
    """
    ancillary_codes = load_ancillary_codes()
    violations = []
    
    for item in bill_items:
        cpt = item['cpt_code'].strip() if item['cpt_code'] else ""
        units = int(item['units']) if item['units'] else 1
        
        if cpt and cpt not in ancillary_codes and units > 1:
            violations.append({
                'cpt': cpt,
                'units': units,
                'line_id': item['id']
            })
    
    return {
        'has_violations': len(violations) > 0,
        'violations': violations
    }