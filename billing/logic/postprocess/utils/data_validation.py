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

def inspect_bill_line_item_table():
    """Inspect the BillLineItem table structure to debug foreign key issues."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get BillLineItem table schema
        cursor.execute("PRAGMA table_info(BillLineItem)")
        columns = cursor.fetchall()
        
        print("BillLineItem table columns:")
        for col in columns:
            print(f"  {col[1]} ({col[2]}) - {'NOT NULL' if col[3] else 'NULL'}")
        
        # Get sample data to see foreign key values
        cursor.execute("SELECT * FROM BillLineItem LIMIT 3")
        samples = cursor.fetchall()
        
        print(f"\nSample BillLineItem data ({len(samples)} rows):")
        for i, row in enumerate(samples):
            print(f"  Row {i + 1}: {dict(row)}")
        
        # Get count
        cursor.execute("SELECT COUNT(*) as count FROM BillLineItem")
        count = cursor.fetchone()[0]
        print(f"\nTotal BillLineItem records: {count}")
        
        # Also inspect order_line_items for comparison
        print("\n" + "="*50)
        cursor.execute("PRAGMA table_info(order_line_items)")
        oli_columns = cursor.fetchall()
        
        print("order_line_items table columns:")
        for col in oli_columns:
            print(f"  {col[1]} ({col[2]}) - {'NOT NULL' if col[3] else 'NULL'}")
        
        # Get sample order line items
        cursor.execute("SELECT * FROM order_line_items LIMIT 3")
        oli_samples = cursor.fetchall()
        
        print(f"\nSample order_line_items data ({len(oli_samples)} rows):")
        for i, row in enumerate(oli_samples):
            row_dict = dict(row)
            # Only show key fields to avoid clutter
            key_fields = ['id', 'Order_ID', 'DOS', 'CPT', 'Modifier', 'Units', 'Charge']
            filtered_row = {k: v for k, v in row_dict.items() if k in key_fields}
            print(f"  Row {i + 1}: {filtered_row}")
        
        # Get count
        cursor.execute("SELECT COUNT(*) as count FROM order_line_items")
        oli_count = cursor.fetchone()[0]
        print(f"\nTotal order_line_items records: {oli_count}")
        
    except sqlite3.Error as e:
        logger.error(f"Error inspecting tables: {str(e)}")
    finally:
        conn.close()

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
                o.FileMaker_Record_Number,
                o.PatientName,
                o.Patient_First_Name,
                o.Patient_Last_Name,
                o.Patient_DOB,
                o.Patient_Address,
                o.Patient_City,
                o.Patient_State,
                o.Patient_Zip,
                o.PatientPhone,
                o.Patient_Injury_Date,
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
            AND pb.action = 'apply_rate'
            AND (pb.bill_paid IS NULL OR pb.bill_paid = 'N')
            ORDER BY pb.created_at ASC
        """
        
        if limit:
            query += f" LIMIT {limit}"
            
        cursor.execute(query)
        bills = [dict(row) for row in cursor.fetchall()]
        
        # Debug logging
        if bills:
            logger.debug("Sample bill data:")
            sample_bill = bills[0]
            logger.debug(f"Provider fields in first bill:")
            logger.debug(f"provider_name: {sample_bill.get('provider_name')}")
            logger.debug(f"provider_tin: {sample_bill.get('provider_tin')}")
            logger.debug(f"provider_npi: {sample_bill.get('provider_npi')}")
            logger.debug(f"bill id: {sample_bill.get('id')}")
        
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
        # First, let's check what foreign key column names exist
        cursor.execute("PRAGMA table_info(BillLineItem)")
        columns = [col[1] for col in cursor.fetchall()]
        logger.debug(f"BillLineItem columns: {columns}")
        
        # Try different possible foreign key column names
        possible_fk_columns = [
            'provider_bill_id',
            'bill_id', 
            'provider_bill',
            'ProviderBill_id',
            'providerbill_id'
        ]
        
        fk_column = None
        for col in possible_fk_columns:
            if col in columns:
                fk_column = col
                break
        
        if not fk_column:
            logger.error(f"Could not find foreign key column in BillLineItem. Available columns: {columns}")
            return []
        
        logger.debug(f"Using foreign key column: {fk_column}")
        
        # Query with the correct foreign key column
        query = f"""
            SELECT 
                bli.*,
                dp.category,
                dp.subcategory,
                dp.proc_desc
            FROM BillLineItem bli
            LEFT JOIN dim_proc dp ON bli.cpt_code = dp.proc_cd
            WHERE bli.{fk_column} = ?
            ORDER BY bli.date_of_service, bli.cpt_code
        """
        
        cursor.execute(query, (bill_id,))
        line_items = [dict(row) for row in cursor.fetchall()]
        
        if not line_items:
            # Debug: Check if there are any line items with this bill_id
            cursor.execute(f"SELECT COUNT(*) as count FROM BillLineItem WHERE {fk_column} = ?", (bill_id,))
            count = cursor.fetchone()[0]
            logger.debug(f"Found {count} line items for bill {bill_id} using column {fk_column}")
            
            # Check if the bill_id exists at all
            cursor.execute("SELECT id FROM ProviderBill WHERE id = ?", (bill_id,))
            bill_exists = cursor.fetchone()
            if not bill_exists:
                logger.warning(f"Bill {bill_id} does not exist in ProviderBill table")
            
            # Show some sample line items to see what's available
            cursor.execute(f"SELECT {fk_column}, COUNT(*) as count FROM BillLineItem GROUP BY {fk_column} LIMIT 5")
            sample_bills = cursor.fetchall()
            logger.debug(f"Sample bill IDs with line items: {[dict(row) for row in sample_bills]}")
        
        logger.debug(f"Retrieved {len(line_items)} line items for bill {bill_id}")
        return line_items
        
    except sqlite3.Error as e:
        logger.error(f"Database error retrieving line items for bill {bill_id}: {str(e)}")
        return []
    finally:
        conn.close()

def get_order_line_items(order_id: str) -> List[Dict[str, Any]]:
    """
    Get all order_line_items for a specific order (for matching purposes).
    
    Args:
        order_id: The Order ID
        
    Returns:
        List of order line item dictionaries with standardized column names
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Updated query based on actual table structure
        cursor.execute("""
            SELECT 
                oli.id,
                oli.Order_ID,
                oli.DOS as date_of_service,
                oli.CPT as cpt_code,
                oli.Modifier as modifier,
                oli.Units as units,
                oli.Description as description,
                oli.Charge as charge_amount,
                oli.line_number,
                oli.created_at,
                oli.updated_at,
                oli.is_active,
                oli.BR_paid,
                oli.BR_rate,
                oli.EOBR_doc_no,
                oli.HCFA_doc_no,
                oli.BR_date_processed,
                oli.BILLS_PAID,
                oli.BILL_REVIEWED,
                dp.category,
                dp.subcategory,
                dp.proc_desc
            FROM order_line_items oli
            LEFT JOIN dim_proc dp ON oli.CPT = dp.proc_cd
            WHERE oli.Order_ID = ?
            ORDER BY oli.DOS, oli.CPT
        """, (order_id,))
        
        order_line_items = [dict(row) for row in cursor.fetchall()]
        
        if order_line_items:
            logger.debug(f"Retrieved {len(order_line_items)} order line items for order {order_id}")
            # Debug: show first item
            first_item = order_line_items[0]
            logger.debug(f"First item: CPT={first_item.get('cpt_code')}, DOS={first_item.get('date_of_service')}")
        else:
            logger.debug(f"No order line items found for order {order_id}")
            
            # Debug: check if the Order_ID exists at all
            cursor.execute("SELECT COUNT(*) FROM order_line_items WHERE Order_ID = ?", (order_id,))
            count = cursor.fetchone()[0]
            logger.debug(f"Total order_line_items with Order_ID {order_id}: {count}")
        
        return order_line_items
        
    except sqlite3.Error as e:
        logger.error(f"Database error retrieving order line items for order {order_id}: {str(e)}")
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
    
    # Debug logging
    logger.debug(f"Validating bill {bill.get('id')}")
    logger.debug("Provider fields received:")
    logger.debug(f"provider_name: {bill.get('provider_name')}")
    logger.debug(f"provider_tin: {bill.get('provider_tin')}")
    logger.debug(f"provider_npi: {bill.get('provider_npi')}")
    
    # Required bill fields
    required_bill_fields = {
        'id': 'Bill ID',
        'claim_id': 'Claim ID',
        'PatientName': 'Patient Name',
        'total_charge': 'Total Charge',
        'provider_name': 'Billing Provider Name',
        'provider_tin': 'Provider TIN',
        'provider_npi': 'Provider NPI'
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
            logger.debug(f"Missing or empty field: {field} (display name: {display_name})")
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

def match_bill_to_order_line_items(bill_line_items: List[Dict[str, Any]], 
                                   order_line_items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Match BillLineItems to order_line_items for verification and tracking.
    
    Args:
        bill_line_items: List of BillLineItem dictionaries
        order_line_items: List of order_line_items dictionaries
        
    Returns:
        Dictionary with matching results and unmatched items
    """
    matches = []
    unmatched_bill_items = []
    unmatched_order_items = order_line_items.copy()
    
    for bill_item in bill_line_items:
        bill_cpt = bill_item.get('cpt_code', '').strip()
        bill_dos = bill_item.get('date_of_service', '').strip()
        bill_modifier = bill_item.get('modifier', '').strip()
        
        # Find matching order line item
        matched = False
        for i, order_item in enumerate(unmatched_order_items):
            order_cpt = order_item.get('cpt_code', '').strip()
            order_dos = order_item.get('date_of_service', '').strip()
            order_modifier = order_item.get('modifier', '').strip()
            
            # Match on CPT, date of service, and modifier
            if (bill_cpt == order_cpt and 
                bill_dos == order_dos and 
                bill_modifier == order_modifier):
                
                matches.append({
                    'bill_item': bill_item,
                    'order_item': order_item,
                    'match_type': 'exact'
                })
                unmatched_order_items.pop(i)
                matched = True
                break
        
        if not matched:
            # Try looser matching (CPT and date only)
            for i, order_item in enumerate(unmatched_order_items):
                order_cpt = order_item.get('cpt_code', '').strip()
                order_dos = order_item.get('date_of_service', '').strip()
                
                if bill_cpt == order_cpt and bill_dos == order_dos:
                    matches.append({
                        'bill_item': bill_item,
                        'order_item': order_item,
                        'match_type': 'partial'
                    })
                    unmatched_order_items.pop(i)
                    matched = True
                    break
        
        if not matched:
            unmatched_bill_items.append(bill_item)
    
    return {
        'matches': matches,
        'unmatched_bill_items': unmatched_bill_items,
        'unmatched_order_items': unmatched_order_items,
        'match_summary': {
            'total_bill_items': len(bill_line_items),
            'total_order_items': len(order_line_items),
            'exact_matches': len([m for m in matches if m['match_type'] == 'exact']),
            'partial_matches': len([m for m in matches if m['match_type'] == 'partial']),
            'unmatched_bill': len(unmatched_bill_items),
            'unmatched_order': len(unmatched_order_items)
        }
    }

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
        
        # Get order line items for reference (not validated, just attached)
        order_id = bill.get('Order_ID')
        order_line_items = []
        matching_result = {}
        
        try:
            if order_id:
                order_line_items = get_order_line_items(order_id)
                logger.debug(f"Retrieved {len(order_line_items)} order line items for order {order_id}")
            else:
                logger.warning(f"No Order_ID found for bill {bill_id}")
                
            # Match bill line items to order line items
            matching_result = match_bill_to_order_line_items(line_items, order_line_items)
            
        except Exception as e:
            logger.error(f"Error getting/matching order line items for bill {bill_id}: {str(e)}")
            order_line_items = []
            matching_result = {}
        
        # Combine validations
        overall_valid = bill_validation['is_valid'] and line_items_validation['is_valid']
        
        bill_result = {
            'bill_id': bill_id,
            'is_valid': overall_valid,
            'bill_validation': bill_validation,
            'line_items_validation': line_items_validation,
            'line_items': line_items,
            'order_line_items': order_line_items,  # For matching purposes
            'matching_result': matching_result,  # Matching analysis
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
    
    # Print details for all bills (both valid and invalid)
    all_bills = validation_report['valid_bills'] + validation_report['invalid_bills']
    for bill in all_bills:
        bill_id = bill['bill_id']
        print(f"\nBill ID: {bill_id}")
        print("-" * 40)
        
        # Print bill data
        bill_data = bill['bill_data']
        print("Basic Information:")
        print(f"  Patient Name: {bill_data.get('PatientName', 'N/A')}")
        print(f"  Claim ID: {bill_data.get('claim_id', 'N/A')}")
        print(f"  Total Charge: ${bill_data.get('total_charge', 0):.2f}")
        
        print("\nProvider Information:")
        print(f"  Provider Name: {bill_data.get('provider_name', 'N/A')}")
        print(f"  Provider TIN: {bill_data.get('provider_tin', 'N/A')}")
        print(f"  Provider NPI: {bill_data.get('provider_npi', 'N/A')}")
        
        # Print line items
        line_items = bill['line_items']
        if line_items:
            print(f"\nBill Line Items ({len(line_items)}):")
            for item in line_items:
                print(f"  CPT: {item.get('cpt_code', 'N/A')}")
                print(f"    Date of Service: {item.get('date_of_service', 'N/A')}")
                print(f"    Charge Amount: ${item.get('charge_amount', 0):.2f}")
                print(f"    Allowed Amount: ${item.get('allowed_amount', 0):.2f}")
                print(f"    Units: {item.get('units', 'N/A')}")
                print(f"    Modifier: {item.get('modifier', 'N/A')}")
                print(f"    Decision: {item.get('decision', 'N/A')}")
                print()
        else:
            print("\nNo bill line items found")
        
        # Print order line items for reference
        order_line_items = bill.get('order_line_items', [])
        if order_line_items:
            print(f"\nOrder Line Items ({len(order_line_items)}) - for reference:")
            for item in order_line_items:
                print(f"  CPT: {item.get('cpt_code', 'N/A')}")
                print(f"    Date of Service: {item.get('date_of_service', 'N/A')}")
                print(f"    Charge Amount: ${float(item.get('charge_amount', 0)):.2f}")
                print(f"    Units: {item.get('units', 'N/A')}")
                print(f"    BR_paid: {item.get('BR_paid', 'N/A')}")
                print(f"    BILL_REVIEWED: {item.get('BILL_REVIEWED', 'N/A')}")
                print()
        
        # Print matching analysis
        matching_result = bill.get('matching_result', {})
        if matching_result:
            match_summary = matching_result.get('match_summary', {})
            print(f"\nLine Item Matching Analysis:")
            print(f"  Bill Line Items: {match_summary.get('total_bill_items', 0)}")
            print(f"  Order Line Items: {match_summary.get('total_order_items', 0)}")
            print(f"  Exact Matches: {match_summary.get('exact_matches', 0)}")
            print(f"  Partial Matches: {match_summary.get('partial_matches', 0)}")
            print(f"  Unmatched Bill Items: {match_summary.get('unmatched_bill', 0)}")
            print(f"  Unmatched Order Items: {match_summary.get('unmatched_order', 0)}")
            
            # Show unmatched items if any
            unmatched_bill = matching_result.get('unmatched_bill_items', [])
            if unmatched_bill:
                print(f"\n  Unmatched Bill Items:")
                for item in unmatched_bill:
                    print(f"    CPT {item.get('cpt_code')} on {item.get('date_of_service')}")
            
            unmatched_order = matching_result.get('unmatched_order_items', [])
            if unmatched_order:
                print(f"\n  Unmatched Order Items:")
                for item in unmatched_order:
                    print(f"    CPT {item.get('cpt_code')} on {item.get('date_of_service')}")
        
        # Print validation issues if any
        if not bill['is_valid']:
            print("\nValidation Issues:")
            bill_val = bill['bill_validation']
            if bill_val['missing_fields']:
                print(f"  Missing Fields: {', '.join(bill_val['missing_fields'])}")
            if bill_val['warnings']:
                print(f"  Warnings: {', '.join(bill_val['warnings'])}")
            
            line_val = bill['line_items_validation']
            if line_val['line_item_issues']:
                print(f"  Line Item Issues:")
                for issue in line_val['line_item_issues']:
                    cpt = issue.get('cpt_code', 'Unknown')
                    issues = ', '.join(issue['issues'])
                    print(f"    CPT {cpt}: {issues}")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    # Test the validation functions
    logging.basicConfig(level=logging.DEBUG)  # Set to DEBUG for more detail
    
    # First inspect the table structure
    print("Inspecting BillLineItem table structure...")
    inspect_bill_line_item_table()
    
    # Get approved unpaid bills
    bills = get_approved_unpaid_bills(limit=3)
    
    if bills:
        # Validate the bills
        validation_report = validate_bill_data(bills)
        
        # Print summary
        print_validation_summary(validation_report)
    else:
        print("No approved unpaid bills found for validation")