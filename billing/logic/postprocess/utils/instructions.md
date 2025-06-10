Update my excel_generator.py file with these changes:

1. In create_duplicate_key(): Change from using bill.get('FileMaker_Record_Number') to bill.get('order_id')
2. In enhanced_duplicate_check(): Remove all patient+date checking logic, only check for exact duplicates and same order_id with different CPTs
3. In create_excel_row(): Add 'Order ID': order_id to the excel_row dictionary, and validate that both order_id and FileMaker_Record_Number exist
4. In format_description(): Change from using FileMaker_Record_Number to order_id in the description
5. In _create_empty_historical_df(): Add 'Order ID' to the columns list
6. Add this validation function to the class:
   def validate_bill_for_processing(bill): check for required fields order_id, FileMaker_Record_Number, PatientName, line_items

KEEP UNCHANGED: get_next_eobr_number() should still use FileMaker_Record_Number for EOBR numbering.

The goal: Switch duplicate checking to use order_id+CPTs while keeping EOBR numbering on FileMaker records.



ARTIFACT:


# Step 2 CORRECTED: Update Excel Generator for Order ID + CPT Duplicate Checking
# =============================================================================

# CORRECTED MODIFICATIONS TO YOUR excel_generator.py
# Replace these methods in your ExcelBatchGenerator class

import pandas as pd
import holidays
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import logging
import re

# =============================================================================
# CORRECTED UPDATED METHODS FOR ExcelBatchGenerator CLASS
# =============================================================================

def create_duplicate_key(self, bill: Dict[str, Any]) -> str:
    """
    Create Full Duplicate Key from order_id and CPT codes.
    
    Args:
        bill: Bill dictionary with order_id and line_items
        
    Returns:
        Full Duplicate Key in format: "ORDER_ID|CPT1,CPT2,CPT3"
    """
    try:
        # Get order_id - this is the primary change from FileMaker_Record_Number
        order_id = bill.get('order_id', '')
        if not order_id:
            logger.warning(f"No order_id found for bill {bill.get('id')}")
            order_id = 'UNKNOWN'
        
        # Get all CPT codes from line items (same as before)
        line_items = bill.get('line_items', [])
        cpts = []
        
        for item in line_items:
            cpt = item.get('cpt_code', '').strip()
            if cpt:
                cpts.append(cpt)
        
        # Sort CPTs for consistent key generation
        cpts.sort()
        cpt_string = ','.join(cpts)
        
        full_key = f"{order_id}|{cpt_string}"
        
        # Log detailed information about key creation
        logger.info(f"Creating duplicate key for bill {bill.get('id')}:")
        logger.info(f"  Order ID: {order_id}")
        logger.info(f"  CPT codes: {cpts}")
        logger.info(f"  Generated key: {full_key}")
        
        return full_key
        
    except Exception as e:
        logger.error(f"Error creating duplicate key for bill {bill.get('id')}: {str(e)}")
        return f"ERROR_{bill.get('id', 'UNKNOWN')}|"

def enhanced_duplicate_check(self, full_duplicate_key: str, bill: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Enhanced duplicate check using order_id + CPT combinations.
    SIMPLIFIED: Only checks for exact duplicates and same order_id with different CPTs.
    
    Args:
        full_duplicate_key: The primary duplicate key to check
        bill: The current bill being processed
        
    Returns:
        Tuple of (is_duplicate, duplicate_type)
        duplicate_type can be: "exact", "same_order_different_cpts", "none"
    """
    if self.historical_df.empty and not self.current_batch_keys:
        return False, "none"
    
    current_order_id = bill.get('order_id', '').strip()
    
    logger.info(f"Enhanced duplicate check for key: {full_duplicate_key}")
    logger.info(f"Current order_id: {current_order_id}")
    
    # Check exact duplicate first (order_id + CPT combination)
    existing_keys = []
    if not self.historical_df.empty and 'Full Duplicate Key' in self.historical_df.columns:
        existing_keys = self.historical_df['Full Duplicate Key'].fillna('').tolist()
    
    # Check historical data for exact match
    historical_exact_match = full_duplicate_key in existing_keys
    if historical_exact_match:
        matching_row = self.historical_df[self.historical_df['Full Duplicate Key'] == full_duplicate_key].iloc[0]
        row_num = self.historical_df[self.historical_df['Full Duplicate Key'] == full_duplicate_key].index[0] + 1
        logger.info(f"Found EXACT duplicate in historical data at row {row_num}:")
        logger.info(f"  EOBR Number: {matching_row['EOBR Number']}")
        logger.info(f"  Order ID: {matching_row.get('Order ID', 'N/A')}")
        logger.info(f"  Description: {matching_row['Description']}")
        
        # Safely handle Amount formatting
        try:
            amount_value = float(matching_row['Amount'])
            logger.info(f"  Amount: ${amount_value:.2f}")
        except (ValueError, TypeError):
            logger.info(f"  Amount: {matching_row['Amount']} (could not format as currency)")
        
        return True, "exact"
    
    # Check current batch for exact match
    current_batch_exact_match = full_duplicate_key in self.current_batch_keys
    if current_batch_exact_match:
        logger.info(f"Found EXACT duplicate in current batch")
        return True, "exact"
    
    # Enhanced check: Look for same order_id with different CPT combinations
    if current_order_id:
        logger.info(f"Checking for same order_id with different CPTs...")
        
        # Check historical data for same order_id
        if not self.historical_df.empty and 'Order ID' in self.historical_df.columns:
            same_order_matches = self.historical_df[
                self.historical_df['Order ID'] == current_order_id
            ]
            
            if not same_order_matches.empty:
                logger.warning(f"Found {len(same_order_matches)} records with same order_id but different CPTs:")
                for idx, match_row in same_order_matches.iterrows():
                    logger.warning(f"  Row {idx + 1}: {match_row.get('Full Duplicate Key', 'N/A')}")
                    logger.warning(f"    EOBR: {match_row.get('EOBR Number', 'N/A')}")
                
                # This is a yellow flag - same order but different services
                return True, "same_order_different_cpts"
    
    # No duplicates found
    logger.debug(f"No duplicates found for: {full_duplicate_key}")
    self.current_batch_keys.add(full_duplicate_key)
    return False, "none"

def get_next_eobr_number(self, fm_record_number: str) -> str:
    """
    Get the next EOBR number for a FileMaker_Record_Number across ALL history.
    UNCHANGED: Still using FileMaker Record Number for EOBR numbering.
    
    Args:
        fm_record_number: The FileMaker Record Number
        
    Returns:
        Next EOBR number in format: "FM_RECORD-X"
    """
    try:
        if self.historical_df.empty:
            return f"{fm_record_number}-1"
        
        # Find all existing EOBR numbers for this FM record
        eobr_pattern = f"{re.escape(fm_record_number)}-"
        existing_eobrs = self.historical_df[
            self.historical_df['EOBR Number'].str.contains(eobr_pattern, na=False)
        ]['EOBR Number'].tolist()
        
        if not existing_eobrs:
            next_number = f"{fm_record_number}-1"
            logger.debug(f"First EOBR for {fm_record_number}: {next_number}")
            return next_number
        
        # Extract sequence numbers and find max
        sequence_numbers = []
        for eobr in existing_eobrs:
            try:
                # Extract number after last dash
                seq_str = eobr.split('-')[-1]
                seq = int(seq_str)
                sequence_numbers.append(seq)
            except (ValueError, IndexError):
                logger.warning(f"Could not parse sequence from EOBR number: {eobr}")
                continue
        
        next_seq = max(sequence_numbers) + 1 if sequence_numbers else 1
        next_number = f"{fm_record_number}-{next_seq}"
        
        logger.debug(f"Next EOBR for {fm_record_number}: {next_number} (existing: {len(existing_eobrs)})")
        return next_number
        
    except Exception as e:
        logger.error(f"Error getting EOBR number for {fm_record_number}: {str(e)}")
        return f"{fm_record_number}-1"

def format_description(self, bill: Dict[str, Any], bill_date: str) -> str:
    """
    Format description field: dates, CPTs, patient name, order_id.
    
    Args:
        bill: Bill dictionary
        bill_date: Formatted bill date
        
    Returns:
        Formatted description string
    """
    try:
        description_parts = []
        
        # Add bill date
        description_parts.append(bill_date)
        
        # Add CPT codes
        line_items = bill.get('line_items', [])
        cpts = []
        for item in line_items:
            cpt = item.get('cpt_code', '').strip()
            if cpt:
                cpts.append(cpt)
        
        if cpts:
            cpts.sort()  # Sort for consistency
            description_parts.append(', '.join(cpts))
        
        # Add patient name
        patient_name = bill.get('PatientName', '').strip()
        if patient_name:
            description_parts.append(patient_name)
        
        # Add Order ID (changed from FileMaker Record Number)
        order_id = bill.get('order_id', '').strip()
        if order_id:
            description_parts.append(order_id)
        
        description = ', '.join(description_parts)
        logger.debug(f"Formatted description: {description}")
        
        return description
        
    except Exception as e:
        logger.error(f"Error formatting description: {str(e)}")
        return "Description formatting error"

def create_excel_row(self, bill: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a single Excel row for a bill with order_id-based duplicate detection.
    
    Args:
        bill: Bill dictionary with all required data (must include order_id AND FileMaker_Record_Number)
        
    Returns:
        Dictionary representing one Excel row
    """
    try:
        logger.info(f"Processing bill {bill.get('id')} for Excel generation")
        
        # Validate that we have both order_id and FileMaker_Record_Number
        order_id = bill.get('order_id', '').strip()
        fm_record = bill.get('FileMaker_Record_Number', '').strip()
        
        if not order_id:
            logger.error(f"Bill {bill.get('id')} missing order_id - cannot process")
            raise ValueError(f"Bill {bill.get('id')} missing required order_id")
        
        if not fm_record:
            logger.error(f"Bill {bill.get('id')} missing FileMaker_Record_Number - cannot create EOBR")
            raise ValueError(f"Bill {bill.get('id')} missing required FileMaker_Record_Number")
        
        # Create duplicate key (using order_id)
        full_duplicate_key = self.create_duplicate_key(bill)
        
        # Enhanced duplicate check
        is_duplicate, duplicate_type = self.enhanced_duplicate_check(full_duplicate_key, bill)
        
        # Get EOBR number (using FileMaker_Record_Number)
        eobr_number = self.get_next_eobr_number(fm_record)
        
        # Get earliest service date
        line_items = bill.get('line_items', [])
        bill_date = self.get_earliest_service_date(line_items)
        
        # Calculate due date
        due_date = self.calculate_due_date(bill_date)
        
        # Calculate total amount
        total_amount = self.calculate_total_amount(line_items)
        
        # Format fields
        vendor = bill.get('provider_billing_name', '').strip()
        mailing_address = self.format_mailing_address(bill)
        description = self.format_description(bill, bill_date)
        memo = self.format_memo(bill, bill_date)
        
        # Determine release payment and duplicate check based on duplicate type
        if duplicate_type == "exact":
            release_payment = "N"
            duplicate_check = "Y"
            logger.info(f"Bill marked as EXACT duplicate - will not be released")
        elif duplicate_type == "same_order_different_cpts":
            release_payment = "Y"  # Still process, but flag for manual review
            duplicate_check = "YELLOW"  # Special flag for manual review
            logger.warning(f"Bill marked for manual review - same order_id with different CPTs")
        else:
            release_payment = "Y"
            duplicate_check = "N"
        
        # Create Excel row (NOW INCLUDING ORDER ID)
        excel_row = {
            'Release Payment': release_payment,
            'Duplicate Check': duplicate_check,
            'Full Duplicate Key': full_duplicate_key,
            'Input File': bill.get('id', ''),
            'Order ID': order_id,  # NEW COLUMN
            'EOBR Number': eobr_number,  # Still based on FileMaker_Record_Number
            'Vendor': vendor,
            'Mailing Address': mailing_address,
            'Terms': 'Net 45',
            'Bill Date': bill_date,
            'Due Date': due_date,
            'Category': 'Subcontracted Services:Provider Services',
            'Description': description,
            'Amount': round(total_amount, 2),
            'Memo': memo,
            'Total': round(total_amount, 2)
        }
        
        logger.info(f"Created Excel row for bill {bill.get('id')}: "
                   f"Order ID={order_id}, EOBR={eobr_number}, Amount=${total_amount:.2f}, Duplicate={duplicate_check}")
        
        return excel_row
        
    except Exception as e:
        logger.error(f"Error creating Excel row for bill {bill.get('id')}: {str(e)}")
        raise

def _create_empty_historical_df(self):
    """Create empty DataFrame with required columns including Order ID."""
    columns = [
        'Release Payment', 'Duplicate Check', 'Full Duplicate Key',
        'Input File', 'Order ID', 'EOBR Number', 'Vendor', 'Mailing Address',
        'Terms', 'Bill Date', 'Due Date', 'Category', 'Description',
        'Amount', 'Memo', 'Total'
    ]
    self.historical_df = pd.DataFrame(columns=columns)

# =============================================================================
# BILL DATA VALIDATION
# =============================================================================

def validate_bill_for_processing(bill: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Validate that a bill has all required fields for processing.
    
    Args:
        bill: Bill dictionary
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    required_fields = ['order_id', 'FileMaker_Record_Number', 'PatientName', 'line_items']
    
    for field in required_fields:
        if field not in bill or not bill[field]:
            return False, f"Missing required field: {field}"
    
    # Validate line_items structure
    line_items = bill.get('line_items', [])
    if not isinstance(line_items, list) or len(line_items) == 0:
        return False, "line_items must be a non-empty list"
    
    # Validate each line item has required fields
    for i, item in enumerate(line_items):
        if not isinstance(item, dict):
            return False, f"line_items[{i}] must be a dictionary"
        
        if 'cpt_code' not in item or not item['cpt_code']:
            return False, f"line_items[{i}] missing cpt_code"
        
        if 'allowed_amount' not in item:
            return False, f"line_items[{i}] missing allowed_amount"
    
    return True, ""

# =============================================================================
# CORRECTED USAGE NOTES
# =============================================================================

"""
CORRECTED INTEGRATION STEPS:

KEY CHANGES FROM ORIGINAL:
1. ✅ Duplicate checking uses order_id + CPTs
2. ✅ EOBR numbering STILL uses FileMaker_Record_Number (unchanged)
3. ❌ REMOVED patient+date duplicate checking (simplified)
4. ✅ Added Order ID column to Excel output
5. ✅ Enhanced detection for same order_id with different CPT combinations

REQUIRED BILL FIELDS:
- order_id (for duplicate checking)
- FileMaker_Record_Number (for EOBR numbering)
- PatientName
- line_items (with cpt_code and allowed_amount)

DUPLICATE DETECTION LOGIC:
- EXACT (RED): Same order_id + same CPT combination → Blocked
- YELLOW: Same order_id with different CPTs → Manual review
- GREEN: New order_id + CPT combination → Processed normally

NO MORE patient+date checking - simplified logic only.
"""