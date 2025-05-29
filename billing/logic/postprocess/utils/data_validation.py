# billing/logic/postprocess/utils/data_validation.py

import logging
import sqlite3
from typing import List, Dict, Any, Tuple, Optional
from pathlib import Path
from datetime import datetime

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

def get_approved_unpaid_bills(limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Fetch bills that are approved and not yet paid.
    
    Args:
        limit: Optional maximum number of bills to fetch
        
    Returns:
        List of bill dictionaries with full details
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        query = """
            SELECT 
                pb.*,
                o.Order_ID,
                o.PatientName,
                o.Patient_First_Name,
                o.Patient_Last_Name,
                o.Patient_DOB,
                o.Patient_Address,
                o.Patient_City,
                o.Patient_State,
                o.Patient_Zip,
                o.PatientPhone,
                o.Referring_Physician,
                o.Referring_Physician_NPI,
                o.Assigning_Company,
                o.Assigning_Adjuster,
                o.Claim_Number,
                o.Order_Type,
                o.Jurisdiction_State,
                o.bundle_type,
                o.provider_id,
                p."DBA Name Billing Name" as provider_name,
                p."Billing Name" as provider_billing_name,
                p."Address Line 1" as provider_address1,
                p."Address Line 2" as provider_address2,
                p.City as provider_city,
                p.State as provider_state,
                p."Postal Code" as provider_postal_code,
                p."Billing Address 1" as provider_billing_address1,
                p."Billing Address 2" as provider_billing_address2,
                p."Billing Address City" as provider_billing_city,
                p."Billing Address State" as provider_billing_state,
                p."Billing Address Postal Code" as provider_billing_postal_code,
                p.TIN as provider_tin,
                p.NPI as provider_npi,
                p."Provider Network" as provider_network,
                p.Phone as provider_phone,
                p."Fax Number" as provider_fax
            FROM ProviderBill pb
            INNER JOIN orders o ON pb.claim_id = o.Order_ID
            INNER JOIN providers p ON o.provider_id = p.PrimaryKey
            WHERE pb.status = 'REVIEWED' 
            AND (pb.bill_paid IS NULL OR pb.bill_paid = 'N')
            ORDER BY pb.created_at ASC
        """
        
        if limit:
            query += f" LIMIT {limit}"
            
        cursor.execute(query)
        bills = [dict(row) for row in cursor.fetchall()]
        
        logger.info(f"Found {len(bills)} approved unpaid bills")
        return bills
        
    except sqlite3.Error as e:
        logger.error(f"Database error retrieving approved unpaid bills: {str(e)}")
        return []
    finally:
        conn.close()

def get_bill_line_items(bill_id: str) -> List[Dict[str, Any]]:
    """
    Get all line items for a specific bill.
    
    Args:
        bill_id: The provider bill ID
        
    Returns:
        List of line item dictionaries
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT 
                bli.*,
                dp.category,
                dp.subcategory,
                dp.proc_desc
            FROM BillLineItem bli
            LEFT JOIN dim_proc dp ON bli.cpt_code = dp.proc_cd
            WHERE bli.provider_bill_id = ?
            ORDER BY bli.date_of_service, bli.cpt_code
        """, (bill_id,))
        
        line_items = [dict(row) for row in cursor.fetchall()]
        logger.debug(f"Retrieved {len(line_items)} line items for bill {bill_id}")
        return line_items
        
    except sqlite3.Error as e:
        logger.error(f"Database error retrieving line items for bill {bill_id}: {str(e)}")
        return []
    finally:
        conn.close()

def validate_bill_completeness(bill: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate that a bill has all required data for postprocessing.
    
    Args:
        bill: Bill dictionary with full details
        
    Returns:
        Validation result with is_valid flag and missing fields
    """
    validation_result = {
        'is_valid': True,
        'missing_fields': [],
        'warnings': [],
        'bill_id': bill.get('id')
    }
    
    # Required bill fields
    required_bill_fields = {
        'id': 'Bill ID',
        'claim_id': 'Claim ID',
        'patient_name': 'Patient Name',
        'total_charge': 'Total Charge',
        'billing_provider_name': 'Billing Provider Name',
        'billing_provider_tin': 'Provider TIN',
        'billing_provider_npi': 'Provider NPI'
    }
    
    # Required order fields
    required_order_fields = {
        'Order_ID': 'Order ID',
        'PatientName': 'Patient Name in Order',
        'Assigning_Company': 'Assigning Company',
        'Claim_Number': 'Claim Number'
    }
    
    # Required provider fields
    required_provider_fields = {
        'provider_name': 'Provider Name',
        'provider_tin': 'Provider TIN',
        'provider_npi': 'Provider NPI',
        'provider_network': 'Provider Network Status'
    }
    
    # Check required fields
    all_required_fields = {**required_bill_fields, **required_order_fields, **required_provider_fields}
    
    for field, display_name in all_required_fields.items():
        value = bill.get(field)
        if not value or (isinstance(value, str) and value.strip() == ''):
            validation_result['missing_fields'].append(display_name)
            validation_result['is_valid'] = False
    
    # Check for warnings (optional but recommended fields)
    warning_fields = {
        'Patient_DOB': 'Patient Date of Birth',
        'provider_address1': 'Provider Address',
        'provider_billing_address1': 'Provider Billing Address',
        'provider_phone': 'Provider Phone'
    }
    
    for field, display_name in warning_fields.items():
        value = bill.get(field)
        if not value or (isinstance(value, str) and value.strip() == ''):
            validation_result['warnings'].append(display_name)
    
    return validation_result

def validate_line_items_completeness(line_items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Validate that line items have all required data.
    
    Args:
        line_items: List of line item dictionaries
        
    Returns:
        Validation result with details about each line item
    """
    validation_result = {
        'is_valid': True,
        'line_item_issues': [],
        'total_line_items': len(line_items)
    }
    
    if not line_items:
        validation_result['is_valid'] = False
        validation_result['line_item_issues'].append({
            'issue': 'No line items found',
            'line_item_id': None
        })
        return validation_result
    
    required_fields = ['cpt_code', 'charge_amount', 'units', 'date_of_service', 'decision', 'allowed_amount']
    
    for item in line_items:
        item_issues = []
        
        # Check required fields
        for field in required_fields:
            value = item.get(field)
            if value is None or (isinstance(value, str) and value.strip() == ''):
                item_issues.append(f"Missing {field}")
        
        # Validate decision is approved
        if item.get('decision') != 'APPROVED':
            item_issues.append(f"Decision is '{item.get('decision')}', expected 'APPROVED'")
        
        # Validate allowed_amount is set
        if item.get('allowed_amount') is None:
            item_issues.append("Missing allowed_amount (rate not applied)")
        
        # Validate charge_amount and allowed_amount are numeric and positive
        try:
            charge = float(item.get('charge_amount', 0))
            if charge <= 0:
                item_issues.append("Charge amount must be positive")
        except (ValueError, TypeError):
            item_issues.append("Invalid charge amount format")
        
        try:
            allowed = float(item.get('allowed_amount', 0))
            if allowed < 0:
                item_issues.append("Allowed amount cannot be negative")
        except (ValueError, TypeError):
            item_issues.append("Invalid allowed amount format")
        
        # Validate units
        try:
            units = int(item.get('units', 0))
            if units <= 0:
                item_issues.append("Units must be positive")
        except (ValueError, TypeError):
            item_issues.append("Invalid units format")
        
        # If there are issues with this line item, record them
        if item_issues:
            validation_result['is_valid'] = False
            validation_result['line_item_issues'].append({
                'line_item_id': item.get('id'),
                'cpt_code': item.get('cpt_code'),
                'issues': item_issues
            })
    
    return validation_result

def validate_bill_data(bills: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Validate all bills and their line items for postprocessing readiness.
    
    Args:
        bills: List of bill dictionaries
        
    Returns:
        Comprehensive validation report
    """
    validation_report = {
        'total_bills': len(bills),
        'valid_bills': [],
        'invalid_bills': [],
        'validation_timestamp': datetime.now().isoformat(),
        'summary': {
            'valid_count': 0,
            'invalid_count': 0,
            'total_line_items': 0,
            'valid_line_items': 0
        }
    }
    
    logger.info(f"Validating {len(bills)} bills for postprocessing")
    
    for bill in bills:
        bill_id = bill.get('id')
        logger.debug(f"Validating bill {bill_id}")
        
        # Validate bill completeness
        bill_validation = validate_bill_completeness(bill)
        
        # Get and validate line items
        line_items = get_bill_line_items(bill_id)
        line_items_validation = validate_line_items_completeness(line_items)
        
        # Combine validations
        overall_valid = bill_validation['is_valid'] and line_items_validation['is_valid']
        
        bill_result = {
            'bill_id': bill_id,
            'is_valid': overall_valid,
            'bill_validation': bill_validation,
            'line_items_validation': line_items_validation,
            'line_items': line_items,
            'bill_data': bill
        }
        
        if overall_valid:
            validation_report['valid_bills'].append(bill_result)
            validation_report['summary']['valid_count'] += 1
        else:
            validation_report['invalid_bills'].append(bill_result)
            validation_report['summary']['invalid_count'] += 1
        
        validation_report['summary']['total_line_items'] += len(line_items)
        validation_report['summary']['valid_line_items'] += len([
            item for item in line_items 
            if item.get('decision') == 'APPROVED' and item.get('allowed_amount') is not None
        ])
    
    logger.info(f"Validation complete: {validation_report['summary']['valid_count']} valid, "
                f"{validation_report['summary']['invalid_count']} invalid bills")
    
    return validation_report

def print_validation_summary(validation_report: Dict[str, Any]):
    """
    Print a human-readable summary of the validation report.
    
    Args:
        validation_report: The validation report from validate_bill_data
    """
    summary = validation_report['summary']
    
    print("=" * 60)
    print("POSTPROCESSING DATA VALIDATION SUMMARY")
    print("=" * 60)
    print(f"Total Bills: {validation_report['total_bills']}")
    print(f"Valid Bills: {summary['valid_count']}")
    print(f"Invalid Bills: {summary['invalid_count']}")
    print(f"Total Line Items: {summary['total_line_items']}")
    print(f"Valid Line Items: {summary['valid_line_items']}")
    print()
    
    if validation_report['invalid_bills']:
        print("INVALID BILLS:")
        print("-" * 40)
        for invalid_bill in validation_report['invalid_bills']:
            bill_id = invalid_bill['bill_id']
            print(f"\nBill ID: {bill_id}")
            
            # Bill-level issues
            bill_val = invalid_bill['bill_validation']
            if bill_val['missing_fields']:
                print(f"  Missing Fields: {', '.join(bill_val['missing_fields'])}")
            if bill_val['warnings']:
                print(f"  Warnings: {', '.join(bill_val['warnings'])}")
            
            # Line item issues
            line_val = invalid_bill['line_items_validation']
            if line_val['line_item_issues']:
                print(f"  Line Item Issues:")
                for issue in line_val['line_item_issues']:
                    cpt = issue.get('cpt_code', 'Unknown')
                    issues = ', '.join(issue['issues'])
                    print(f"    CPT {cpt}: {issues}")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    # Test the validation functions
    logging.basicConfig(level=logging.INFO)
    
    # Get approved unpaid bills
    bills = get_approved_unpaid_bills(limit=10)
    
    if bills:
        # Validate the bills
        validation_report = validate_bill_data(bills)
        
        # Print summary
        print_validation_summary(validation_report)
    else:
        print("No approved unpaid bills found for validation")