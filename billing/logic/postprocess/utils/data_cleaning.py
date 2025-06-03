# billing/logic/postprocess/utils/data_cleaning.py

import logging
import sqlite3
import re
from typing import List, Dict, Any, Tuple, Optional
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)

# Get the absolute path to the monolith root directory
DB_ROOT = Path(r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\monolith")

def get_db_connection(db_path: str = None) -> sqlite3.Connection:
    """Get a connection to the SQLite database."""
    if db_path is None:
        db_path = str(DB_ROOT / 'monolith.db')
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def clean_phone_number(phone: str) -> str:
    """
    Clean and standardize phone numbers.
    
    Args:
        phone: Raw phone number string
        
    Returns:
        Cleaned phone number in format (XXX) XXX-XXXX
    """
    if not phone:
        return ""
    
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', phone)
    
    # Handle different lengths
    if len(digits) == 10:
        return f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
    elif len(digits) == 11 and digits[0] == '1':
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:11]}"
    else:
        # Return original if can't parse
        return phone.strip()

def clean_zip_code(zip_code: str) -> str:
    """
    Clean and standardize ZIP codes.
    
    Args:
        zip_code: Raw ZIP code string
        
    Returns:
        Cleaned ZIP code in format XXXXX or XXXXX-XXXX
    """
    if not zip_code:
        return ""
    
    # Remove spaces and non-alphanumeric except dash
    cleaned = re.sub(r'[^\w-]', '', str(zip_code))
    
    # Handle 5-digit ZIP
    if len(cleaned) == 5 and cleaned.isdigit():
        return cleaned
    
    # Handle 9-digit ZIP
    if len(cleaned) == 9 and cleaned.isdigit():
        return f"{cleaned[0:5]}-{cleaned[5:9]}"
    
    # Handle ZIP+4 with dash
    if len(cleaned) == 10 and '-' in cleaned:
        parts = cleaned.split('-')
        if len(parts) == 2 and len(parts[0]) == 5 and len(parts[1]) == 4:
            if parts[0].isdigit() and parts[1].isdigit():
                return cleaned
    
    # Return first 5 digits if longer
    digits = re.sub(r'\D', '', cleaned)
    if len(digits) >= 5:
        return digits[0:5]
    
    return cleaned

def clean_tin(tin: str) -> str:
    """
    Clean and standardize TIN numbers.
    
    Args:
        tin: Raw TIN string
        
    Returns:
        Cleaned TIN in format XX-XXXXXXX
    """
    if not tin:
        return ""
    
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', tin)
    
    # TIN should be 9 digits
    if len(digits) == 9:
        return f"{digits[0:2]}-{digits[2:9]}"
    
    # Return original if can't parse
    return tin.strip()

def clean_npi(npi: str) -> str:
    """
    Clean and validate NPI numbers.
    
    Args:
        npi: Raw NPI string
        
    Returns:
        Cleaned NPI (10 digits) or original if invalid
    """
    if not npi:
        return ""
    
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', npi)
    
    # NPI should be exactly 10 digits
    if len(digits) == 10:
        return digits
    
    # Return original if can't parse
    return npi.strip()

def clean_currency_amount(amount: Any) -> Optional[Decimal]:
    """
    Clean and standardize currency amounts.
    
    Args:
        amount: Raw amount (string, int, float, or Decimal)
        
    Returns:
        Decimal amount or None if invalid
    """
    if amount is None:
        return None
    
    if isinstance(amount, Decimal):
        return amount
    
    if isinstance(amount, (int, float)):
        try:
            return Decimal(str(amount)).quantize(Decimal('0.01'))
        except InvalidOperation:
            return None
    
    if isinstance(amount, str):
        # Remove currency symbols and whitespace
        cleaned = re.sub(r'[$,\s]', '', amount.strip())
        
        # Handle parentheses for negative amounts
        if cleaned.startswith('(') and cleaned.endswith(')'):
            cleaned = '-' + cleaned[1:-1]
        
        try:
            return Decimal(cleaned).quantize(Decimal('0.01'))
        except (InvalidOperation, ValueError):
            return None
    
    return None

def standardize_date_format(date_str: str) -> Optional[str]:
    """
    Standardize date strings to YYYY-MM-DD format.
    Handles single dates and date ranges (takes first date from range).
    
    Args:
        date_str: Raw date string (can be single date or range like "12/26/24 - 12/26/24")
        
    Returns:
        Standardized date string in YYYY-MM-DD format or None if invalid
    """
    if not date_str:
        return None
    
    date_str = str(date_str).strip()
    
    # Handle date ranges - take the first date
    range_separators = [' - ', ' – ', ' — ', '-', '–', '—', ' to ', ' TO ']
    for separator in range_separators:
        if separator in date_str:
            date_str = date_str.split(separator)[0].strip()
            break
    
    # Remove any non-date text (like "MX" or other prefixes)
    date_str = re.sub(r'^[^0-9]+', '', date_str)
    
    # Common date formats to try
    formats = [
        # Year first formats
        '%Y-%m-%d',      # 2024-01-01
        '%Y/%m/%d',      # 2024/01/01
        '%Y%m%d',        # 20240101
        
        # Month first formats (US style)
        '%m/%d/%Y',      # 01/01/2024
        '%m-%d-%Y',      # 01-01-2024
        '%m/%d/%y',      # 01/01/24
        '%m-%d-%y',      # 01-01-24
        '%m.%d.%Y',      # 01.01.2024
        '%m.%d.%y',      # 01.01.24
        
        # Day first formats (international)
        '%d/%m/%Y',      # 01/01/2024
        '%d-%m-%Y',      # 01-01-2024
        '%d/%m/%y',      # 01/01/24
        '%d-%m-%y',      # 01-01-24
        '%d.%m.%Y',      # 01.01.2024
        '%d.%m.%y',      # 01.01.24
        
        # Month name formats
        '%B %d, %Y',     # January 01, 2024
        '%b %d, %Y',     # Jan 01, 2024
        '%d %B %Y',      # 01 January 2024
        '%d %b %Y',      # 01 Jan 2024
        
        # Special formats
        '%m %d %Y',      # 01 01 2024
        '%m %d %y',      # 01 01 24
        '%d %m %Y',      # 01 01 2024
        '%d %m %y',      # 01 01 24
    ]
    
    # Try each format
    for fmt in formats:
        try:
            parsed_date = datetime.strptime(date_str, fmt).date()
            
            # Handle 2-digit years - assume 20xx for years 00-30, 19xx for 31-99
            if parsed_date.year < 100:
                if parsed_date.year <= 30:
                    parsed_date = parsed_date.replace(year=parsed_date.year + 2000)
                else:
                    parsed_date = parsed_date.replace(year=parsed_date.year + 1900)
            
            # Validate reasonable year range (medical billing context)
            if 1900 <= parsed_date.year <= 2030:
                return parsed_date.strftime('%Y-%m-%d')
                
        except ValueError:
            continue
    
    # Try to handle some edge cases with regex
    # Handle formats like "12/26/24" more explicitly
    date_patterns = [
        r'(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{2,4})',  # MM/DD/YY or MM/DD/YYYY
        r'(\d{4})[\/\-\.](\d{1,2})[\/\-\.](\d{1,2})',     # YYYY/MM/DD
        r'(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{2,4})',  # DD/MM/YY or DD/MM/YYYY
        r'(\d{1,2})\s+(\d{1,2})\s+(\d{2,4})',            # MM DD YY or MM DD YYYY
        r'(\d{1,2})\s+(\d{1,2})\s+(\d{2,4})',            # DD MM YY or DD MM YYYY
    ]
    
    for pattern in date_patterns:
        match = re.match(pattern, date_str)
        if match:
            part1, part2, part3 = match.groups()
            
            # Try different interpretations
            try:
                # Assume MM/DD/YY format first (US standard)
                month, day, year = int(part1), int(part2), int(part3)
                
                # Handle 2-digit years
                if year < 100:
                    year = year + 2000 if year <= 30 else year + 1900
                
                # Validate ranges
                if 1 <= month <= 12 and 1 <= day <= 31 and 1900 <= year <= 2030:
                    try:
                        parsed_date = date(year, month, day)
                        return parsed_date.strftime('%Y-%m-%d')
                    except ValueError:
                        pass  # Invalid date (like Feb 30)
                        
            except ValueError:
                pass
    
    logger.warning(f"Could not parse date: {date_str}")
    return None

def clean_text_field(text: str, max_length: int = None) -> str:
    """
    Clean text fields by trimming whitespace and handling length.
    
    Args:
        text: Raw text string
        max_length: Maximum allowed length
        
    Returns:
        Cleaned text string
    """
    if not text:
        return ""
    
    # Convert to string and strip whitespace
    cleaned = str(text).strip()
    
    # Replace multiple spaces with single space
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    # Truncate if too long
    if max_length and len(cleaned) > max_length:
        cleaned = cleaned[:max_length].strip()
    
    return cleaned

def clean_state_field(state: str) -> str:
    """
    Clean and validate state abbreviations.
    
    Args:
        state: Raw state string
        
    Returns:
        Cleaned state abbreviation (2 uppercase letters)
    """
    if not state:
        return ""
    
    cleaned = str(state).strip().upper()
    
    # Should be exactly 2 alphabetic characters
    if len(cleaned) == 2 and cleaned.isalpha():
        return cleaned
    
    # If longer, try to extract state abbreviation
    if len(cleaned) > 2:
        # Check if it starts with valid 2-letter combo
        if cleaned[:2].isalpha():
            return cleaned[:2]
    
    return cleaned

def clean_orders_data(bill: Dict[str, Any]) -> Dict[str, Any]:
    """
    Clean data fields from the orders table.
    
    Args:
        bill: Bill dictionary containing orders data
        
    Returns:
        Bill dictionary with cleaned orders fields
    """
    cleaned_bill = bill.copy()
    
    # Clean PatientName
    if 'PatientName' in cleaned_bill:
        cleaned_bill['PatientName'] = clean_text_field(cleaned_bill['PatientName'], max_length=255)
    
    # Clean Patient_DOB (standardize date format)
    if 'Patient_DOB' in cleaned_bill and cleaned_bill['Patient_DOB']:
        cleaned_date = standardize_date_format(cleaned_bill['Patient_DOB'])
        if cleaned_date:
            cleaned_bill['Patient_DOB'] = cleaned_date
        else:
            logger.warning(f"Could not parse Patient_DOB: {cleaned_bill['Patient_DOB']}")
    
    # Clean Patient_Injury_Date (standardize date format)
    if 'Patient_Injury_Date' in cleaned_bill and cleaned_bill['Patient_Injury_Date']:
        cleaned_date = standardize_date_format(cleaned_bill['Patient_Injury_Date'])
        if cleaned_date:
            cleaned_bill['Patient_Injury_Date'] = cleaned_date
        else:
            logger.warning(f"Could not parse Patient_Injury_Date: {cleaned_bill['Patient_Injury_Date']}")
    
    # Clean FileMaker_Record_Number (should be numeric or alphanumeric)
    if 'FileMaker_Record_Number' in cleaned_bill:
        cleaned_bill['FileMaker_Record_Number'] = clean_text_field(cleaned_bill['FileMaker_Record_Number'], max_length=50)
    
    return cleaned_bill

def clean_provider_bill_data(bill: Dict[str, Any]) -> Dict[str, Any]:
    """
    Clean data fields from the ProviderBill table.
    
    Args:
        bill: Bill dictionary containing ProviderBill data
        
    Returns:
        Bill dictionary with cleaned ProviderBill fields
    """
    cleaned_bill = bill.copy()
    
    # Clean patient_account_no
    if 'patient_account_no' in cleaned_bill:
        cleaned_bill['patient_account_no'] = clean_text_field(cleaned_bill['patient_account_no'], max_length=100)
    
    return cleaned_bill

def clean_providers_data(bill: Dict[str, Any]) -> Dict[str, Any]:
    """
    Clean data fields from the providers table.
    
    Args:
        bill: Bill dictionary containing providers data
        
    Returns:
        Bill dictionary with cleaned providers fields
    """
    cleaned_bill = bill.copy()
    
    # Clean Billing Address 1
    if 'provider_billing_address1' in cleaned_bill:
        cleaned_bill['provider_billing_address1'] = clean_text_field(cleaned_bill['provider_billing_address1'], max_length=100)
    
    # Clean Billing Address 2
    if 'provider_billing_address2' in cleaned_bill:
        cleaned_bill['provider_billing_address2'] = clean_text_field(cleaned_bill['provider_billing_address2'], max_length=100)
    
    # Clean Billing Address City
    if 'provider_billing_city' in cleaned_bill:
        cleaned_bill['provider_billing_city'] = clean_text_field(cleaned_bill['provider_billing_city'], max_length=50)
    
    # Clean Billing Address State
    if 'provider_billing_state' in cleaned_bill:
        cleaned_bill['provider_billing_state'] = clean_state_field(cleaned_bill['provider_billing_state'])
    
    # Clean Billing Address Postal Code
    if 'provider_billing_postal_code' in cleaned_bill:
        cleaned_bill['provider_billing_postal_code'] = clean_zip_code(cleaned_bill['provider_billing_postal_code'])
    
    # Clean Billing Name
    if 'provider_billing_name' in cleaned_bill:
        cleaned_bill['provider_billing_name'] = clean_text_field(cleaned_bill['provider_billing_name'], max_length=255)
    
    # Clean TIN
    if 'provider_tin' in cleaned_bill:
        cleaned_bill['provider_tin'] = clean_tin(cleaned_bill['provider_tin'])
    
    # Clean NPI (can be null per requirements)
    if 'provider_npi' in cleaned_bill and cleaned_bill['provider_npi']:
        cleaned_bill['provider_npi'] = clean_npi(cleaned_bill['provider_npi'])
    
    return cleaned_bill

def clean_cpt_code(cpt: str) -> str:
    """
    Clean and validate CPT codes.
    
    Args:
        cpt: Raw CPT code string
        
    Returns:
        Cleaned CPT code (5 characters) or original if invalid
    """
    if not cpt:
        return ""
    
    # Remove whitespace and convert to uppercase
    cleaned = str(cpt).strip().upper()
    
    # CPT should be exactly 5 characters (all digits or 1 letter + 4 digits)
    if len(cleaned) == 5:
        if cleaned.isdigit() or (cleaned[0].isalpha() and cleaned[1:].isdigit()):
            return cleaned
    
    # Return original if can't validate
    return cpt.strip()

def clean_modifier(modifier: str) -> str:
    """
    Clean and validate modifiers - only keep LT, RT, 26, TC.
    
    Args:
        modifier: Raw modifier string
        
    Returns:
        Cleaned modifier string with only valid modifiers, or empty string
    """
    if not modifier:
        return ""
    
    # Valid modifiers to keep
    valid_modifiers = {'LT', 'RT', '26', 'TC'}
    
    # Split by comma and clean each modifier
    kept_modifiers = []
    for mod in str(modifier).split(','):
        cleaned_mod = mod.strip().upper()
        if cleaned_mod in valid_modifiers:
            kept_modifiers.append(cleaned_mod)
    
    return ','.join(kept_modifiers) if kept_modifiers else ""

def clean_place_of_service(pos: str) -> str:
    """
    Clean place of service codes - default to '11' if not valid.
    
    Args:
        pos: Raw place of service string
        
    Returns:
        Cleaned place of service code (defaults to '11')
    """
    if not pos:
        return "11"
    
    # Remove non-digit characters
    digits = re.sub(r'\D', '', str(pos))
    
    # Place of service should be 2 digits
    if len(digits) >= 2:
        return digits[:2]
    elif len(digits) == 1:
        return '0' + digits  # Pad with leading zero
    else:
        return "11"  # Default to office (11)

def clean_line_item_data(line_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Clean data fields from the BillLineItem table.
    
    Args:
        line_item: Line item dictionary
        
    Returns:
        Cleaned line item dictionary
    """
    cleaned_item = line_item.copy()
    
    # Clean cpt_code
    if 'cpt_code' in cleaned_item:
        cleaned_item['cpt_code'] = clean_cpt_code(cleaned_item['cpt_code'])
    
    # Clean modifier (can be null per requirements)
    if 'modifier' in cleaned_item and cleaned_item['modifier']:
        cleaned_item['modifier'] = clean_modifier(cleaned_item['modifier'])
    
    # Clean units (ensure positive integer)
    if 'units' in cleaned_item and cleaned_item['units'] is not None:
        try:
            units = int(float(cleaned_item['units']))
            if units > 0:
                cleaned_item['units'] = units
            else:
                logger.warning(f"Invalid units value: {cleaned_item['units']}")
        except (ValueError, TypeError):
            logger.warning(f"Could not parse units: {cleaned_item['units']}")
    
    # Clean charge_amount
    if 'charge_amount' in cleaned_item and cleaned_item['charge_amount'] is not None:
        cleaned_amount = clean_currency_amount(cleaned_item['charge_amount'])
        if cleaned_amount is not None:
            cleaned_item['charge_amount'] = float(cleaned_amount)
        else:
            logger.warning(f"Could not parse charge_amount: {cleaned_item['charge_amount']}")
    
    # Clean allowed_amount
    if 'allowed_amount' in cleaned_item and cleaned_item['allowed_amount'] is not None:
        cleaned_amount = clean_currency_amount(cleaned_item['allowed_amount'])
        if cleaned_amount is not None:
            cleaned_item['allowed_amount'] = float(cleaned_amount)
        else:
            logger.warning(f"Could not parse allowed_amount: {cleaned_item['allowed_amount']}")
    
    # Clean decision (standardize to uppercase)
    if 'decision' in cleaned_item and cleaned_item['decision']:
        decision = str(cleaned_item['decision']).strip().upper()
        valid_decisions = ['APPROVED', 'DENIED', 'REDUCED', 'PENDING']
        if decision in valid_decisions:
            cleaned_item['decision'] = decision
        else:
            logger.warning(f"Invalid decision value: {cleaned_item['decision']}")
    
    # Clean reason_code
    if 'reason_code' in cleaned_item and cleaned_item['reason_code']:
        cleaned_item['reason_code'] = clean_text_field(cleaned_item['reason_code'], max_length=20)
    
    # Clean date_of_service
    if 'date_of_service' in cleaned_item and cleaned_item['date_of_service']:
        # Handle date ranges (take the first date)
        date_str = str(cleaned_item['date_of_service']).strip()
        if ' - ' in date_str:
            date_str = date_str.split(' - ')[0].strip()
        
        cleaned_date = standardize_date_format(date_str)
        if cleaned_date:
            cleaned_item['date_of_service'] = cleaned_date
        else:
            logger.warning(f"Could not parse date_of_service: {cleaned_item['date_of_service']}")
    
    # Clean place_of_service
    if 'place_of_service' in cleaned_item and cleaned_item['place_of_service']:
        cleaned_item['place_of_service'] = clean_place_of_service(cleaned_item['place_of_service'])
    
    return cleaned_item

def clean_bill_data(bill: Dict[str, Any]) -> Dict[str, Any]:
    """
    Clean all data fields in a bill record across all required tables.
    
    Args:
        bill: Raw bill dictionary with data from all tables
        
    Returns:
        Cleaned bill dictionary
    """
    logger.debug(f"Cleaning data for bill {bill.get('bill_id')}")
    
    # Start with the original bill
    cleaned_bill = bill.copy()
    
    # Clean data from each table
    cleaned_bill = clean_orders_data(cleaned_bill)
    cleaned_bill = clean_provider_bill_data(cleaned_bill)
    cleaned_bill = clean_providers_data(cleaned_bill)
    
    return cleaned_bill

def clean_bills_data(bills: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Clean data for multiple bills and their line items.
    
    Args:
        bills: List of bill dictionaries
        
    Returns:
        Tuple of (cleaned_bills, cleaning_report)
    """
    cleaning_report = {
        'total_bills': len(bills),
        'cleaned_bills': 0,
        'cleaning_issues': [],
        'cleaning_timestamp': datetime.now().isoformat()
    }
    
    cleaned_bills = []
    
    logger.info(f"Cleaning data for {len(bills)} bills")
    
    for bill in bills:
        bill_id = bill.get('bill_id')
        
        try:
            # Clean the bill data
            cleaned_bill = clean_bill_data(bill)
            
            # Get and clean line items
            from .data_validation import get_bill_line_items
            line_items = get_bill_line_items(bill_id)
            cleaned_line_items = []
            
            for item in line_items:
                try:
                    cleaned_item = clean_line_item_data(item)
                    cleaned_line_items.append(cleaned_item)
                except Exception as e:
                    logger.error(f"Error cleaning line item {item.get('id')} for bill {bill_id}: {str(e)}")
                    cleaning_report['cleaning_issues'].append({
                        'bill_id': bill_id,
                        'line_item_id': item.get('id'),
                        'error': str(e)
                    })
            
            # Add cleaned line items to the bill
            cleaned_bill['line_items'] = cleaned_line_items
            cleaned_bills.append(cleaned_bill)
            cleaning_report['cleaned_bills'] += 1
            
        except Exception as e:
            logger.error(f"Error cleaning bill {bill_id}: {str(e)}")
            cleaning_report['cleaning_issues'].append({
                'bill_id': bill_id,
                'line_item_id': None,
                'error': str(e)
            })
    
    logger.info(f"Cleaning complete: {cleaning_report['cleaned_bills']} bills cleaned successfully")
    
    return cleaned_bills, cleaning_report

def update_database_with_cleaned_data(cleaned_bills: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Update the database with cleaned data.
    
    Args:
        cleaned_bills: List of cleaned bill dictionaries with line items
        
    Returns:
        Update report with success/failure counts
    """
    update_report = {
        'total_bills': len(cleaned_bills),
        'updated_bills': 0,
        'updated_line_items': 0,
        'update_errors': [],
        'update_timestamp': datetime.now().isoformat()
    }
    
    conn = get_db_connection()
    
    try:
        cursor = conn.cursor()
        
        for bill in cleaned_bills:
            bill_id = bill.get('bill_id')
            
            try:
                # Update ProviderBill table
                cursor.execute("""
                    UPDATE ProviderBill 
                    SET patient_account_no = ?
                    WHERE id = ?
                """, (
                    bill.get('patient_account_no'),
                    bill_id
                ))
                
                # Update orders table
                cursor.execute("""
                    UPDATE orders 
                    SET PatientName = ?,
                        Patient_DOB = ?,
                        Patient_Injury_Date = ?,
                        FileMaker_Record_Number = ?
                    WHERE Order_ID = ?
                """, (
                    bill.get('PatientName'),
                    bill.get('Patient_DOB'),
                    bill.get('Patient_Injury_Date'),
                    bill.get('FileMaker_Record_Number'),
                    bill.get('Order_ID')
                ))
                
                # Update providers table
                cursor.execute("""
                    UPDATE providers 
                    SET "Billing Address 1" = ?,
                        "Billing Address 2" = ?,
                        "Billing Address City" = ?,
                        "Billing Address State" = ?,
                        "Billing Address Postal Code" = ?,
                        "Billing Name" = ?,
                        TIN = ?,
                        NPI = ?
                    WHERE PrimaryKey = ?
                """, (
                    bill.get('provider_billing_address1'),
                    bill.get('provider_billing_address2'),
                    bill.get('provider_billing_city'),
                    bill.get('provider_billing_state'),
                    bill.get('provider_billing_postal_code'),
                    bill.get('provider_billing_name'),
                    bill.get('provider_tin'),
                    bill.get('provider_npi'),
                    bill.get('provider_id')
                ))
                
                # Update line items
                line_items_updated = 0
                for item in bill.get('line_items', []):
                    cursor.execute("""
                        UPDATE BillLineItem 
                        SET cpt_code = ?,
                            modifier = ?,
                            units = ?,
                            charge_amount = ?,
                            allowed_amount = ?,
                            decision = ?,
                            reason_code = ?,
                            date_of_service = ?,
                            place_of_service = ?
                        WHERE id = ?
                    """, (
                        item.get('cpt_code'),
                        item.get('modifier'),
                        item.get('units'),
                        item.get('charge_amount'),
                        item.get('allowed_amount'),
                        item.get('decision'),
                        item.get('reason_code'),
                        item.get('date_of_service'),
                        item.get('place_of_service'),
                        item.get('id')
                    ))
                    line_items_updated += 1
                
                update_report['updated_bills'] += 1
                update_report['updated_line_items'] += line_items_updated
                
            except sqlite3.Error as e:
                logger.error(f"Database error updating bill {bill_id}: {str(e)}")
                update_report['update_errors'].append({
                    'bill_id': bill_id,
                    'error': str(e)
                })
        
        conn.commit()
        logger.info(f"Database update complete: {update_report['updated_bills']} bills, "
                   f"{update_report['updated_line_items']} line items updated")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error during database update: {str(e)}")
        raise
    finally:
        conn.close()
    
    return update_report

def print_cleaning_summary(cleaning_report: Dict[str, Any], update_report: Dict[str, Any] = None):
    """
    Print a human-readable summary of the cleaning and update operations.
    
    Args:
        cleaning_report: The cleaning report from clean_bills_data
        update_report: Optional update report from update_database_with_cleaned_data
    """
    print("=" * 60)
    print("DATA CLEANING SUMMARY")
    print("=" * 60)
    print(f"Total Bills: {cleaning_report['total_bills']}")
    print(f"Successfully Cleaned: {cleaning_report['cleaned_bills']}")
    print(f"Cleaning Issues: {len(cleaning_report['cleaning_issues'])}")
    
    if cleaning_report['cleaning_issues']:
        print("\nCLEANING ISSUES:")
        print("-" * 30)
        for issue in cleaning_report['cleaning_issues'][:5]:  # Show first 5
            bill_id = issue['bill_id']
            line_item_id = issue.get('line_item_id', 'N/A')
            error = issue['error']
            print(f"  Bill {bill_id}, Line Item {line_item_id}: {error}")
        
        if len(cleaning_report['cleaning_issues']) > 5:
            print(f"  ... and {len(cleaning_report['cleaning_issues']) - 5} more issues")
    
    if update_report:
        print(f"\nDATABASE UPDATE SUMMARY:")
        print("-" * 30)
        print(f"Bills Updated: {update_report['updated_bills']}")
        print(f"Line Items Updated: {update_report['updated_line_items']}")
        print(f"Update Errors: {len(update_report['update_errors'])}")
        
        if update_report['update_errors']:
            print("\nUPDATE ERRORS:")
            for error in update_report['update_errors'][:3]:  # Show first 3
                print(f"  Bill {error['bill_id']}: {error['error']}")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    # Test the cleaning functions
    logging.basicConfig(level=logging.INFO)
    
    # Import validation functions to get bills
    from .data_validation import get_approved_unpaid_bills
    
    # Get approved unpaid bills
    bills = get_approved_unpaid_bills(limit=5)
    
    if bills:
        # Clean the bills
        cleaned_bills, cleaning_report = clean_bills_data(bills)
        
        # Print cleaning summary
        print_cleaning_summary(cleaning_report)
        
        # Optionally update database (uncomment to actually update)
        # update_report = update_database_with_cleaned_data(cleaned_bills)
        # print_cleaning_summary(cleaning_report, update_report)
    else:
        print("No approved unpaid bills found for cleaning")