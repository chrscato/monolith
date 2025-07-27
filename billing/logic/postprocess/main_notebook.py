# %%
# Cell 1: Data Pulling - SIMPLIFIED
# =============================================================================

import logging
from pathlib import Path
from utils.data_validation import get_approved_unpaid_bills

# Simple logging setup
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

print("STEP 1: DATA PULLING")
print("=" * 40)

# Pull approved unpaid bills
limit = 100  # Adjust as needed
bills = get_approved_unpaid_bills(limit=limit)

print(f"📊 Found {len(bills)} approved unpaid bills")

if bills:
    # Quick summary
    total_amount = 0
    for bill in bills:
        try:
            bill_total = float(bill.get('total_charge', 0) or 0)
            total_amount += bill_total
        except (ValueError, TypeError):
            continue
    
    print(f"💰 Total amount: ${total_amount:,.2f}")
    print(f"📋 Sample: {bills[0].get('PatientName', 'Unknown')} - {bills[0].get('provider_billing_name', 'Unknown')}")
    print(f"✅ Data pulling complete")
else:
    print("❌ No bills found - check database status")


# %%
# %%
# Cell 1: Flexible Data Pulling - NEW BILLS or SPECIFIC BILL IDs
# =============================================================================

import logging
from pathlib import Path
from utils.data_validation import get_approved_unpaid_bills

# Simple logging setup
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

print("STEP 1: FLEXIBLE DATA PULLING")
print("=" * 45)

# ============================================================================
# CONFIGURATION - Choose your mode:
# ============================================================================

# MODE 1: Pull new approved unpaid bills (normal mode)
PULL_NEW_BILLS = False
LIMIT = 100  # Limit for new bills

# MODE 2: Reprocess specific bill IDs (rerun mode)
REPROCESS_BILL_IDS = [
'20250416_133158020',
'bad9f483-314d-49e2-839b-73adcec0233a',
'182d5de4-6736-4b82-9fbb-f6c5f71f5358',
'9a8b2642-82e9-4594-8f55-d1710c6abcd3',
'574af50b-809e-46ec-9c01-a250d7264f1e',
'48f1b2a1-4374-45d7-8930-fe73e0754eab',
'b507f683-14c5-4690-93a9-1dc20fe239d8',
'c6551c8f-e491-4c88-a1db-47fbed5374b1',
'b3d91523-247a-4a96-85ee-9cd457ea78f2',
'e541e6c4-bbec-4521-b7b9-210a75abe931',
'4a5760bb-a205-4499-9671-eb125f268c84',
'707c5e79-e8e2-4771-86eb-feeaaa3824a4',
'9b95c799-855a-42b5-abb3-60d5c739ad59',
'628903c1-34e0-48ba-bf16-fb45bf960625'

]

# ============================================================================

def get_bills_by_ids(bill_ids):
    """Get specific bills by their IDs (for reprocessing)"""
    try:
        from utils.data_validation import get_db_connection
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create placeholders for the IN clause
        placeholders = ','.join(['?' for _ in bill_ids])
        
        query = f"""
            SELECT 
                pb.*,
                o.*,
                p.*
            FROM ProviderBill pb
            INNER JOIN orders o ON pb.claim_id = o.Order_ID
            INNER JOIN providers p ON o.provider_id = p.PrimaryKey
            WHERE pb.id IN ({placeholders})
            ORDER BY pb.created_at DESC
        """
        
        cursor.execute(query, bill_ids)
        columns = [col[0] for col in cursor.description]
        
        bills = []
        for row in cursor.fetchall():
            bill_data = dict(zip(columns, row))
            
            # Apply same field mapping as get_approved_unpaid_bills
            mapped_bill = {
                # ProviderBill fields
                'id': bill_data.get('id'),
                'claim_id': bill_data.get('claim_id'),
                'status': bill_data.get('status'),
                'action': bill_data.get('action'),
                'bill_paid': bill_data.get('bill_paid'),
                'created_at': bill_data.get('created_at'),
                'patient_name': bill_data.get('patient_name'),
                'total_charge': bill_data.get('total_charge'),
                
                # orders fields
                'Order_ID': bill_data.get('Order_ID'),
                'FileMaker_Record_Number': bill_data.get('FileMaker_Record_Number'),
                'PatientName': bill_data.get('PatientName'),
                'Patient_First_Name': bill_data.get('Patient_First_Name'),
                'Patient_Last_Name': bill_data.get('Patient_Last_Name'),
                'Patient_DOB': bill_data.get('Patient_DOB'),
                'Patient_Injury_Date': bill_data.get('Patient_Injury_Date'),
                'bundle_type': bill_data.get('bundle_type'),
                'provider_id': bill_data.get('provider_id'),
                
                # providers fields with mapping
                'provider_name': bill_data.get('DBA Name Billing Name'),
                'provider_billing_name': bill_data.get('Billing Name'),
                'provider_tin': bill_data.get('TIN'),
                'provider_npi': bill_data.get('NPI'),
                'provider_billing_address1': bill_data.get('Billing Address 1'),
                'provider_billing_address2': bill_data.get('Billing Address 2'),
                'provider_billing_city': bill_data.get('Billing Address City'),
                'provider_billing_state': bill_data.get('Billing Address State'),
                'provider_billing_postal_code': bill_data.get('Billing Address Postal Code'),
                'Provider_Network': bill_data.get('Provider Network'),
            }
            bills.append(mapped_bill)
        
        conn.close()
        return bills
        
    except Exception as e:
        logger.error(f"Error getting bills by IDs: {str(e)}")
        return []

def validate_bill_ids_exist(bill_ids):
    """Check which bill IDs actually exist in the database"""
    try:
        from utils.data_validation import get_db_connection
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        placeholders = ','.join(['?' for _ in bill_ids])
        cursor.execute(f"SELECT id FROM ProviderBill WHERE id IN ({placeholders})", bill_ids)
        
        existing_ids = [row[0] for row in cursor.fetchall()]
        missing_ids = [bid for bid in bill_ids if bid not in existing_ids]
        
        conn.close()
        return existing_ids, missing_ids
        
    except Exception as e:
        logger.error(f"Error validating bill IDs: {str(e)}")
        return [], bill_ids

# ============================================================================
# MAIN EXECUTION
# ============================================================================

if PULL_NEW_BILLS and not REPROCESS_BILL_IDS:
    # MODE 1: Normal operation - pull new bills
    print(f"🔄 MODE: Pulling new approved unpaid bills (limit: {LIMIT})")
    
    bills = get_approved_unpaid_bills(limit=LIMIT)
    
    print(f"📊 Found {len(bills)} approved unpaid bills")
    
    if bills:
        # Quick summary
        total_amount = 0
        for bill in bills:
            try:
                bill_total = float(bill.get('total_charge', 0) or 0)
                total_amount += bill_total
            except (ValueError, TypeError):
                continue
        
        print(f"💰 Total amount: ${total_amount:,.2f}")
        print(f"📋 Sample: {bills[0].get('PatientName', 'Unknown')} - {bills[0].get('provider_billing_name', 'Unknown')}")
        print(f"✅ Data pulling complete")
    else:
        print("❌ No bills found - check database status")

elif REPROCESS_BILL_IDS and not PULL_NEW_BILLS:
    # MODE 2: Reprocess specific bill IDs
    print(f"🔄 MODE: Reprocessing specific bill IDs")
    print(f"📋 Bill IDs to reprocess: {len(REPROCESS_BILL_IDS)}")
    
    if not REPROCESS_BILL_IDS:
        print("❌ No bill IDs specified for reprocessing")
        print("   Add bill IDs to REPROCESS_BILL_IDS list above")
        bills = []
    else:
        # Validate bill IDs exist
        existing_ids, missing_ids = validate_bill_ids_exist(REPROCESS_BILL_IDS)
        
        if missing_ids:
            print(f"⚠️  Missing bill IDs (not found in database):")
            for missing_id in missing_ids:
                print(f"      {missing_id}")
        
        if existing_ids:
            print(f"✅ Found {len(existing_ids)} valid bill IDs")
            
            # Get the bills
            bills = get_bills_by_ids(existing_ids)
            
            if bills:
                # Quick summary
                total_amount = 0
                for bill in bills:
                    try:
                        bill_total = float(bill.get('total_charge', 0) or 0)
                        total_amount += bill_total
                    except (ValueError, TypeError):
                        continue
                
                print(f"📊 Retrieved {len(bills)} bills for reprocessing")
                print(f"💰 Total amount: ${total_amount:,.2f}")
                print(f"📋 Sample: {bills[0].get('PatientName', 'Unknown')} - {bills[0].get('provider_billing_name', 'Unknown')}")
                
                # Show bill details
                print(f"\n📋 BILLS TO REPROCESS:")
                for i, bill in enumerate(bills, 1):
                    print(f"   {i:2d}. {bill.get('id')} - {bill.get('PatientName', 'Unknown')} - ${bill.get('total_charge', 0):.2f}")
                
                print(f"✅ Data pulling complete")
            else:
                print("❌ Failed to retrieve bills")
        else:
            print("❌ No valid bill IDs found")
            bills = []

else:
    # ERROR: Both modes enabled or neither enabled
    print("❌ CONFIGURATION ERROR:")
    if PULL_NEW_BILLS and REPROCESS_BILL_IDS:
        print("   Cannot enable both PULL_NEW_BILLS and REPROCESS_BILL_IDS")
        print("   Set one to True and the other to False")
    else:
        print("   Must enable either PULL_NEW_BILLS or REPROCESS_BILL_IDS")
    
    print("\n🔧 CONFIGURATION INSTRUCTIONS:")
    print("   For new bills:")
    print("     PULL_NEW_BILLS = True")
    print("     REPROCESS_BILL_IDS = []")
    print("\n   For reprocessing:")
    print("     PULL_NEW_BILLS = False") 
    print("     REPROCESS_BILL_IDS = ['bill_id_1', 'bill_id_2']")
    
    bills = []

# Final status
if 'bills' in locals() and bills:
    print(f"\n📝 Variable 'bills' ready for Cell 2 ({len(bills)} bills)")
else:
    print(f"\n⚠️  No bills loaded - fix configuration or database issues")

# %%
# Cell 2: Data Validation - SIMPLIFIED  
# =============================================================================

from utils.data_validation import validate_bill_data

print("\nSTEP 2: DATA VALIDATION")
print("=" * 40)

if not bills:
    print("❌ No bills to validate")
    valid_bills_data = []
else:
    # Run validation
    print(f"🔍 Validating {len(bills)} bills...")
    validation_report = validate_bill_data(bills)
    
    # Extract results
    valid_bills_data = validation_report.get('valid_bills', [])
    invalid_bills_data = validation_report.get('invalid_bills', [])
    
    # Simple summary
    print(f"✅ Valid bills: {len(valid_bills_data)}")
    print(f"❌ Invalid bills: {len(invalid_bills_data)}")
    
    if valid_bills_data:
        # Calculate totals for valid bills only
        total_line_items = 0
        total_valid_amount = 0
        
        for bill_result in valid_bills_data:
            line_items = bill_result.get('line_items', [])
            total_line_items += len(line_items)
            
            for item in line_items:
                allowed = item.get('allowed_amount', 0)
                try:
                    total_valid_amount += float(allowed) if allowed else 0
                except (ValueError, TypeError):
                    continue
        
        print(f"💊 Total line items: {total_line_items}")
        print(f"💰 Valid amount: ${total_valid_amount:,.2f}")
    
    # Show top issues for invalid bills (if any)
    if invalid_bills_data:
        print(f"\n⚠️  Top validation issues:")
        issue_counts = {}
        
        for invalid_bill in invalid_bills_data[:5]:  # Check first 5
            missing_fields = invalid_bill.get('bill_validation', {}).get('missing_fields', [])
            for field in missing_fields:
                issue_counts[field] = issue_counts.get(field, 0) + 1
        
        for issue, count in sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)[:3]:
            print(f"   • {issue}: {count} bills")

print(f"\n🎯 Validation complete - {len(valid_bills_data)} bills ready for processing")

# Store for next cells
if 'valid_bills_data' in locals() and valid_bills_data:
    print(f"📝 Variable 'valid_bills_data' ready for Cell 3")
else:
    print(f"⚠️  No valid bills - check validation issues above")

# %%
# Cell 2B: DETAILED VALIDATION FAILURES (for troubleshooting)
# =============================================================================
# Run this cell only when you need to see detailed validation failures

print("\nSTEP 2B: DETAILED VALIDATION FAILURES")
print("=" * 50)

if not bills:
    print("❌ No bills loaded")
elif 'invalid_bills_data' not in locals() or not invalid_bills_data:
    print("✅ No validation failures to show - all bills passed!")
else:
    print(f"🔍 ANALYZING {len(invalid_bills_data)} FAILED BILLS")
    print("=" * 50)
    
    for i, invalid_bill in enumerate(invalid_bills_data, 1):
        bill_id = invalid_bill.get('bill_id', 'Unknown')
        bill_data = invalid_bill.get('bill_data', {})
        
        print(f"\n--- FAILED BILL {i}/{len(invalid_bills_data)} ---")
        print(f"Bill ID: {bill_id}")
        print(f"Patient: {bill_data.get('PatientName', 'Unknown')}")
        print(f"Provider: {bill_data.get('provider_billing_name', 'Unknown')}")
        print(f"Claim ID: {bill_data.get('claim_id', 'Unknown')}")
        print(f"Order ID: {bill_data.get('Order_ID', 'Unknown')}")
        print(f"FM Record: {bill_data.get('FileMaker_Record_Number', 'Unknown')}")
        
        # Bill-level validation issues
        bill_validation = invalid_bill.get('bill_validation', {})
        missing_fields = bill_validation.get('missing_fields', [])
        warnings = bill_validation.get('warnings', [])
        
        if missing_fields:
            print(f"\n❌ MISSING REQUIRED FIELDS:")
            for field in missing_fields:
                print(f"   • {field}")
                
                # Show what we actually have for key fields
                if 'Provider' in field:
                    print(f"     Current provider_tin: '{bill_data.get('provider_tin', 'NULL')}'")
                    print(f"     Current provider_npi: '{bill_data.get('provider_npi', 'NULL')}'")
                    print(f"     Current provider_name: '{bill_data.get('provider_name', 'NULL')}'")
                elif 'Patient' in field:
                    print(f"     Current PatientName: '{bill_data.get('PatientName', 'NULL')}'")
                elif 'Billing' in field:
                    print(f"     Current billing_name: '{bill_data.get('provider_billing_name', 'NULL')}'")
        
        if warnings:
            print(f"\n⚠️  WARNINGS (optional fields):")
            for warning in warnings:
                print(f"   • {warning}")
        
        # Line items validation issues
        line_validation = invalid_bill.get('line_items_validation', {})
        line_issues = line_validation.get('line_item_issues', [])
        line_items = invalid_bill.get('line_items', [])
        
        print(f"\n💊 LINE ITEMS: {len(line_items)} found")
        
        if not line_items:
            print(f"   ❌ NO LINE ITEMS FOUND - this is likely the main issue")
            print(f"   💡 Check if BillLineItem records exist for bill_id: {bill_id}")
        elif line_issues:
            print(f"   ❌ LINE ITEM ISSUES:")
            for issue in line_issues:
                line_id = issue.get('line_item_id', 'Unknown')
                cpt = issue.get('cpt_code', 'Unknown')
                problems = issue.get('issues', [])
                
                print(f"     Line {line_id} (CPT {cpt}):")
                for problem in problems:
                    print(f"       • {problem}")
        else:
            print(f"   ✅ Line items look good")
            
        # Quick fix suggestions
        print(f"\n🔧 QUICK FIX SUGGESTIONS:")
        
        if not line_items:
            print(f"   1. Check BillLineItem table for provider_bill_id = '{bill_id}'")
            print(f"   2. Verify foreign key relationship is correct")
        
        if missing_fields:
            print(f"   3. Update missing fields in database:")
            for field in missing_fields[:3]:  # Show first 3
                if 'Provider TIN' in field:
                    print(f"      UPDATE providers SET TIN = 'XX-XXXXXXX' WHERE PrimaryKey = '{bill_data.get('provider_id', 'Unknown')}'")
                elif 'Provider NPI' in field:
                    print(f"      UPDATE providers SET NPI = 'XXXXXXXXXX' WHERE PrimaryKey = '{bill_data.get('provider_id', 'Unknown')}'")
                elif 'Provider' in field and 'Name' in field:
                    print(f"      UPDATE providers SET \"Billing Name\" = 'Provider Name' WHERE PrimaryKey = '{bill_data.get('provider_id', 'Unknown')}'")
        
        # Show related database records
        print(f"\n📋 DATABASE RECORD INFO:")
        print(f"   Provider ID: {bill_data.get('provider_id', 'Unknown')}")
        print(f"   Order ID: {bill_data.get('Order_ID', 'Unknown')}")
        print(f"   Claim ID: {bill_data.get('claim_id', 'Unknown')}")
        
        print(f"\n" + "─" * 50)
    
    # Summary of all issues
    print(f"\n📊 VALIDATION FAILURE SUMMARY:")
    print("=" * 40)
    
    all_missing = []
    all_line_issues = []
    
    for invalid_bill in invalid_bills_data:
        missing_fields = invalid_bill.get('bill_validation', {}).get('missing_fields', [])
        all_missing.extend(missing_fields)
        
        line_issues = invalid_bill.get('line_items_validation', {}).get('line_item_issues', [])
        for issue in line_issues:
            all_line_issues.extend(issue.get('issues', []))
    
    # Count most common issues
    from collections import Counter
    
    if all_missing:
        print(f"\nMost common missing fields:")
        missing_counts = Counter(all_missing)
        for field, count in missing_counts.most_common(5):
            print(f"   {field}: {count} bills")
    
    if all_line_issues:
        print(f"\nMost common line item issues:")
        line_counts = Counter(all_line_issues)
        for issue, count in line_counts.most_common(5):
            print(f"   {issue}: {count} line items")
    
    print(f"\n🎯 NEXT STEPS:")
    print(f"   1. Fix the database issues shown above")
    print(f"   2. Re-run Cells 1 & 2 to validate fixes")
    print(f"   3. Proceed to Cell 3 when all bills pass validation")


# %%
# Cell 3: Data Cleaning - SIMPLIFIED
# =============================================================================

import re
from datetime import datetime

print("STEP 3: DATA CLEANING")
print("=" * 40)

def clean_date(date_str):
    """Convert various date formats to YYYY-MM-DD"""
    if not date_str:
        return ""
    
    date_str = str(date_str).strip()
    
    # Handle date ranges - take first date
    if ' - ' in date_str:
        date_str = date_str.split(' - ')[0].strip()
    
    # Remove timestamps
    if ' ' in date_str and ':' in date_str:
        date_str = date_str.split(' ')[0]
    
    # Try common formats
    formats = [
        '%Y-%m-%d',      # 2024-11-25 (already correct)
        '%m/%d/%y',      # 11/25/24
        '%m/%d/%Y',      # 11/25/2024
        '%m %d %Y',      # 07 28 1997
    ]
    
    for fmt in formats:
        try:
            parsed_date = datetime.strptime(date_str, fmt).date()
            
            # Handle 2-digit years
            if parsed_date.year < 100:
                year = parsed_date.year + 2000 if parsed_date.year <= 30 else parsed_date.year + 1900
                parsed_date = parsed_date.replace(year=year)
            
            return parsed_date.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    # If no format matches, return original
    return date_str

def clean_tin(tin):
    """Format TIN as XX-XXXXXXX"""
    if not tin:
        return ""
    
    # Remove all non-digits
    digits = re.sub(r'\D', '', str(tin))
    
    # Format if 9 digits
    if len(digits) == 9:
        return f"{digits[0:2]}-{digits[2:9]}"
    
    return str(tin).strip()

def clean_zip(zip_code):
    """Format ZIP as XXXXX or XXXXX-XXXX"""
    if not zip_code:
        return ""
    
    # Remove non-alphanumeric except dash
    cleaned = re.sub(r'[^\w-]', '', str(zip_code))
    digits = re.sub(r'\D', '', cleaned)
    
    # Format based on length
    if len(digits) == 5:
        return digits
    elif len(digits) >= 9:
        return f"{digits[0:5]}-{digits[5:9]}"
    elif len(digits) >= 5:
        return digits[0:5]
    
    return cleaned

def clean_modifier(modifier):
    """Keep only valid modifiers: LT, RT, 26, TC"""
    if not modifier:
        return ""
    
    valid_modifiers = {'LT', 'RT', '26', 'TC'}
    kept_modifiers = []
    
    for mod in str(modifier).split(','):
        mod = mod.strip().upper()
        if mod in valid_modifiers:
            kept_modifiers.append(mod)
    
    return ','.join(kept_modifiers)

def clean_currency(amount):
    """Convert to float, handle currency symbols"""
    if amount is None or amount == '':
        return 0.0
    
    # Handle string amounts
    if isinstance(amount, str):
        # Remove currency symbols and commas
        cleaned = re.sub(r'[$,\s]', '', amount.strip())
        
        # Handle parentheses for negative
        if cleaned.startswith('(') and cleaned.endswith(')'):
            cleaned = '-' + cleaned[1:-1]
    else:
        cleaned = str(amount)
    
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return 0.0

def clean_text(text, max_length=None):
    """Clean text: trim whitespace, handle None values"""
    if not text or str(text).strip().lower() in ['none', 'null']:
        return ""
    
    # Trim and collapse spaces
    cleaned = re.sub(r'\s+', ' ', str(text).strip())
    
    # Truncate if needed
    if max_length and len(cleaned) > max_length:
        cleaned = cleaned[:max_length].strip()
    
    return cleaned

# Process the bills
if 'valid_bills_data' not in locals() or not valid_bills_data:
    print("❌ No valid bills to clean")
    cleaned_bills = []
else:
    print(f"🧹 Cleaning {len(valid_bills_data)} bills...")
    
    cleaned_bills = []
    cleaning_issues = 0
    
    for bill_result in valid_bills_data:
        try:
            # Get the bill data and line items
            bill_data = bill_result['bill_data'].copy()
            line_items = bill_result.get('line_items', [])
            
            # Clean bill-level fields
            bill_data['PatientName'] = clean_text(bill_data.get('PatientName'), 255)
            bill_data['Patient_DOB'] = clean_date(bill_data.get('Patient_DOB'))
            bill_data['Patient_Injury_Date'] = clean_date(bill_data.get('Patient_Injury_Date'))
            bill_data['FileMaker_Record_Number'] = clean_text(bill_data.get('FileMaker_Record_Number'), 50)
            bill_data['patient_account_no'] = clean_text(bill_data.get('patient_account_no'), 100)
            
            # Clean provider fields
            bill_data['provider_billing_name'] = clean_text(bill_data.get('provider_billing_name'), 255)
            bill_data['provider_billing_address1'] = clean_text(bill_data.get('provider_billing_address1'), 100)
            bill_data['provider_billing_address2'] = clean_text(bill_data.get('provider_billing_address2'), 100)
            bill_data['provider_billing_city'] = clean_text(bill_data.get('provider_billing_city'), 50)
            bill_data['provider_billing_state'] = clean_text(bill_data.get('provider_billing_state'), 2).upper()
            bill_data['provider_billing_postal_code'] = clean_zip(bill_data.get('provider_billing_postal_code'))
            bill_data['provider_tin'] = clean_tin(bill_data.get('provider_tin'))
            bill_data['provider_npi'] = clean_text(bill_data.get('provider_npi'), 10)
            
            # Build formatted address
            address_parts = []
            if bill_data['provider_billing_address1']: 
                address_parts.append(bill_data['provider_billing_address1'])
            if bill_data['provider_billing_address2']: 
                address_parts.append(bill_data['provider_billing_address2'])
            if bill_data['provider_billing_city']: 
                address_parts.append(bill_data['provider_billing_city'])
            if bill_data['provider_billing_state']: 
                address_parts.append(bill_data['provider_billing_state'])
            if bill_data['provider_billing_postal_code']: 
                address_parts.append(bill_data['provider_billing_postal_code'])
            
            bill_data['formatted_address'] = ', '.join(address_parts)
            
            # Clean line items
            cleaned_line_items = []
            for item in line_items:
                cleaned_item = item.copy()
                
                # Clean line item fields
                cleaned_item['cpt_code'] = clean_text(cleaned_item.get('cpt_code'), 5).upper()
                cleaned_item['modifier'] = clean_modifier(cleaned_item.get('modifier'))
                cleaned_item['date_of_service'] = clean_date(cleaned_item.get('date_of_service'))
                cleaned_item['place_of_service'] = clean_text(cleaned_item.get('place_of_service') or '11', 2)
                cleaned_item['reason_code'] = clean_text(cleaned_item.get('reason_code'), 20)
                
                # Clean amounts
                cleaned_item['charge_amount'] = clean_currency(cleaned_item.get('charge_amount'))
                cleaned_item['allowed_amount'] = clean_currency(cleaned_item.get('allowed_amount'))
                
                # Clean units
                try:
                    units = int(float(cleaned_item.get('units', 1)))
                    cleaned_item['units'] = max(1, units)  # Minimum 1 unit
                except (ValueError, TypeError):
                    cleaned_item['units'] = 1
                
                # Clean decision
                decision = str(cleaned_item.get('decision', '')).strip().upper()
                if decision in ['APPROVED', 'DENIED', 'REDUCED', 'PENDING']:
                    cleaned_item['decision'] = decision
                
                cleaned_line_items.append(cleaned_item)
            
            # Add cleaned line items to bill
            bill_data['line_items'] = cleaned_line_items
            cleaned_bills.append(bill_data)
            
        except Exception as e:
            print(f"   ⚠️  Error cleaning bill {bill_result.get('bill_id')}: {str(e)}")
            cleaning_issues += 1
    
    print(f"✅ Cleaned {len(cleaned_bills)} bills successfully")
    if cleaning_issues > 0:
        print(f"⚠️  {cleaning_issues} bills had cleaning issues")
    
    # Show sample of cleaned data
    if cleaned_bills:
        sample = cleaned_bills[0]
        print(f"\n📋 Sample cleaned bill:")
        print(f"   Patient: '{sample.get('PatientName')}'")
        print(f"   DOB: '{sample.get('Patient_DOB')}'")
        print(f"   Provider: '{sample.get('provider_billing_name')}'")
        print(f"   TIN: '{sample.get('provider_tin')}'")
        print(f"   Address: '{sample.get('formatted_address')}'")
        print(f"   Line Items: {len(sample.get('line_items', []))}")
        
        if sample.get('line_items'):
            item = sample['line_items'][0]
            print(f"   Sample CPT: {item.get('cpt_code')} on {item.get('date_of_service')} for ${item.get('allowed_amount'):.2f}")

print(f"\n🎯 Data cleaning complete - ready for Cell 4")

# Set order_id for duplicate detection (use UUID Order_ID field)
if cleaned_bills:
    print(f"\n🔧 Setting order_id for duplicate detection...")
    for bill in cleaned_bills:
        order_id = bill.get('Order_ID', '').strip()
        if order_id:
            bill['order_id'] = order_id
        else:
            print(f"   ⚠️  No Order_ID found for bill {bill.get('id')}")
    
    print(f"✅ order_id field set for {len(cleaned_bills)} bills")

# %%
# Cell 4: Pre-Excel Validation - SIMPLIFIED
# =============================================================================

print("STEP 4: PRE-EXCEL VALIDATION")
print("=" * 40)

def validate_excel_readiness(bill):
    """Check if bill has all required fields for Excel generation"""
    required_fields = {
        'order_id': 'Order ID (for duplicate detection)',
        'FileMaker_Record_Number': 'FileMaker Record (for EOBR numbering)', 
        'PatientName': 'Patient Name',
        'provider_billing_name': 'Provider Name',
        'provider_tin': 'Provider TIN',
        'line_items': 'Line Items'
    }
    
    missing = []
    for field, description in required_fields.items():
        value = bill.get(field)
        if not value or (isinstance(value, list) and len(value) == 0):
            missing.append(description)
    
    # Check line items have required data
    line_items = bill.get('line_items', [])
    if line_items:
        for i, item in enumerate(line_items):
            if not item.get('cpt_code'):
                missing.append(f"Line {i+1} CPT code")
            if item.get('allowed_amount') is None:
                missing.append(f"Line {i+1} allowed amount")
    
    return len(missing) == 0, missing

if 'cleaned_bills' not in locals() or not cleaned_bills:
    print("❌ No cleaned bills available")
    excel_ready_bills = []
else:
    print(f"🔍 Validating {len(cleaned_bills)} cleaned bills for Excel generation...")
    
    excel_ready_bills = []
    validation_issues = []
    total_amount = 0
    total_line_items = 0
    
    for bill in cleaned_bills:
        bill_id = bill.get('id', 'Unknown')
        patient_name = bill.get('PatientName', 'Unknown')
        
        # Validate Excel readiness
        is_ready, missing_fields = validate_excel_readiness(bill)
        
        if is_ready:
            excel_ready_bills.append(bill)
            
            # Calculate amounts
            line_items = bill.get('line_items', [])
            total_line_items += len(line_items)
            
            bill_amount = sum(float(item.get('allowed_amount', 0)) for item in line_items)
            total_amount += bill_amount
            
        else:
            validation_issues.append({
                'bill_id': bill_id,
                'patient_name': patient_name,
                'missing_fields': missing_fields
            })
    
    # Results Summary
    print(f"✅ Excel-ready bills: {len(excel_ready_bills)}")
    print(f"❌ Failed validation: {len(validation_issues)}")
    print(f"💊 Total line items: {total_line_items}")
    print(f"💰 Total amount: ${total_amount:,.2f}")
    
    # Show validation issues (if any)
    if validation_issues:
        print(f"\n⚠️  VALIDATION ISSUES:")
        for issue in validation_issues[:5]:  # Show first 5
            print(f"   {issue['bill_id']} ({issue['patient_name']}):")
            for field in issue['missing_fields'][:3]:  # Show first 3 issues
                print(f"     • Missing: {field}")
        
        if len(validation_issues) > 5:
            print(f"   ... and {len(validation_issues) - 5} more bills with issues")
    
    # Excel Generation Preview
    if excel_ready_bills:
        print(f"\n📊 EXCEL GENERATION PREVIEW:")
        
        # Show what duplicate keys will look like
        sample_bill = excel_ready_bills[0]
        sample_line_items = sample_bill.get('line_items', [])
        sample_cpts = [item.get('cpt_code', '') for item in sample_line_items[:3]]
        sample_key = f"{sample_bill.get('order_id', 'Unknown')}|{','.join(sorted(sample_cpts))}"
        
        print(f"   Sample duplicate key: {sample_key}")
        print(f"   Sample EOBR number: {sample_bill.get('FileMaker_Record_Number', 'Unknown')}-X")
        print(f"   Sample vendor: {sample_bill.get('provider_billing_name', 'Unknown')}")
        
        # Breakdown by provider
        provider_breakdown = {}
        for bill in excel_ready_bills:
            provider = bill.get('provider_billing_name', 'Unknown')
            if provider not in provider_breakdown:
                provider_breakdown[provider] = {'bills': 0, 'amount': 0}
            
            provider_breakdown[provider]['bills'] += 1
            line_items = bill.get('line_items', [])
            bill_amount = sum(float(item.get('allowed_amount', 0)) for item in line_items)
            provider_breakdown[provider]['amount'] += bill_amount
        
        print(f"\n📋 PROVIDER BREAKDOWN:")
        for provider, data in sorted(provider_breakdown.items(), key=lambda x: x[1]['amount'], reverse=True)[:5]:
            print(f"   {provider}: {data['bills']} bills, ${data['amount']:,.2f}")
        
        if len(provider_breakdown) > 5:
            print(f"   ... and {len(provider_breakdown) - 5} more providers")
        
        # Duplicate key analysis
        duplicate_keys = set()
        potential_duplicates = 0
        
        for bill in excel_ready_bills:
            order_id = bill.get('order_id', '')
            line_items = bill.get('line_items', [])
            cpts = sorted([item.get('cpt_code', '') for item in line_items if item.get('cpt_code')])
            
            if order_id and cpts:
                key = f"{order_id}|{','.join(cpts)}"
                if key in duplicate_keys:
                    potential_duplicates += 1
                else:
                    duplicate_keys.add(key)
        
        print(f"\n🔑 DUPLICATE KEY ANALYSIS:")
        print(f"   Unique combinations: {len(duplicate_keys)}")
        print(f"   Potential duplicates: {potential_duplicates}")
        
        if potential_duplicates > 0:
            print(f"   ⚠️  Some bills may be flagged as duplicates in Excel generation")
        else:
            print(f"   ✅ All bills have unique order_id + CPT combinations")
    
    else:
        print(f"\n❌ No bills ready for Excel generation")
        print(f"   Please resolve validation issues above")

print(f"\n🎯 Pre-Excel validation complete")

# Final check for required variables
if 'excel_ready_bills' in locals() and excel_ready_bills:
    print(f"📝 Variable 'excel_ready_bills' ready for Cell 5 ({len(excel_ready_bills)} bills)")
else:
    print(f"⚠️  No excel_ready_bills - resolve validation issues first")

# %%
# %%
# Cell 5: Excel Generation - SIMPLIFIED
# =============================================================================

from utils.excel_generator import generate_excel_batch
from pathlib import Path
from datetime import datetime

print("STEP 5: EXCEL GENERATION")
print("=" * 40)

def validate_bill_for_excel(bill):
    """Final validation before Excel generation"""
    required = ['order_id', 'FileMaker_Record_Number', 'PatientName', 'line_items']
    
    for field in required:
        if not bill.get(field):
            return False, f"Missing {field}"
    
    line_items = bill.get('line_items', [])
    if not line_items:
        return False, "No line items"
    
    for item in line_items:
        if not item.get('cpt_code'):
            return False, "Missing CPT code in line item"
        if item.get('allowed_amount') is None:
            return False, "Missing allowed amount in line item"
    
    return True, ""

if 'excel_ready_bills' not in locals() or not excel_ready_bills:
    print("❌ No excel_ready_bills available")
    print("   Please run Cells 1-4 first")
else:
    print(f"🚀 Generating Excel batch for {len(excel_ready_bills)} bills...")
    
    # Final validation
    validated_bills = []
    validation_failures = []
    
    for bill in excel_ready_bills:
        is_valid, error = validate_bill_for_excel(bill)
        if is_valid:
            validated_bills.append(bill)
        else:
            validation_failures.append({
                'bill_id': bill.get('id'),
                'error': error
            })
    
    if validation_failures:
        print(f"⚠️  {len(validation_failures)} bills failed final validation:")
        for failure in validation_failures[:3]:
            print(f"   {failure['bill_id']}: {failure['error']}")
    
    if not validated_bills:
        print("❌ No valid bills to process")
    else:
        print(f"✅ {len(validated_bills)} bills passed final validation")
        
        # Setup output directory
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = Path("batch_outputs") / f"batch_{timestamp}"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"📁 Output directory: {output_dir}")
        
        try:
            # Generate Excel batch
            batch_excel_path, summary = generate_excel_batch(
                bills=validated_bills,
                batch_output_dir=output_dir
            )
            
            print(f"\n✅ EXCEL GENERATION SUCCESSFUL!")
            print(f"📄 File: {batch_excel_path}")
            
            # Display summary
            print(f"\n📊 BATCH SUMMARY:")
            print(f"   Total records: {summary.get('total_records', 0)}")
            print(f"   New records: {summary.get('new_records', 0)}")
            print(f"   Exact duplicates: {summary.get('duplicate_records', 0)}")
            print(f"   Yellow warnings: {summary.get('yellow_warnings', 0)}")
            print(f"   Total amount: ${summary.get('total_amount', 0):,.2f}")
            print(f"   Release amount: ${summary.get('release_amount', 0):,.2f}")
            
            # File verification
            if batch_excel_path.exists():
                file_size = batch_excel_path.stat().st_size
                print(f"\n📋 FILE VERIFICATION:")
                print(f"   File size: {file_size:,} bytes")
                print(f"   Full path: {batch_excel_path}")
                
                # Quick Excel content check
                try:
                    import pandas as pd
                    df = pd.read_excel(batch_excel_path)
                    print(f"   Rows: {len(df)}")
                    print(f"   Columns: {len(df.columns)}")
                    
                    # Check key columns exist
                    expected_cols = ['Order ID', 'EOBR Number', 'Duplicate Check', 'Amount']
                    missing_cols = [col for col in expected_cols if col not in df.columns]
                    
                    if missing_cols:
                        print(f"   ⚠️  Missing columns: {missing_cols}")
                    else:
                        print(f"   ✅ All expected columns present")
                    
                    # Show duplicate breakdown
                    if 'Duplicate Check' in df.columns:
                        dup_counts = df['Duplicate Check'].value_counts()
                        print(f"\n🔍 DUPLICATE ANALYSIS:")
                        for status, count in dup_counts.items():
                            if status == 'Y':
                                print(f"   🔴 Exact duplicates (blocked): {count}")
                            elif status == 'YELLOW':
                                print(f"   🟡 Same order, different CPTs: {count}")
                            elif status == 'N':
                                print(f"   🟢 New records: {count}")
                            else:
                                print(f"   ❓ Other ({status}): {count}")
                    
                    # Show sample data
                    if len(df) > 0:
                        print(f"\n📄 SAMPLE EXCEL DATA:")
                        sample = df.iloc[0]
                        print(f"   Order ID: {sample.get('Order ID', 'N/A')}")
                        print(f"   EOBR Number: {sample.get('EOBR Number', 'N/A')}")
                        print(f"   Vendor: {sample.get('Vendor', 'N/A')}")
                        print(f"   Amount: ${sample.get('Amount', 0):,.2f}")
                        print(f"   Duplicate Check: {sample.get('Duplicate Check', 'N/A')}")
                        
                        # Show duplicate key format
                        dup_key = sample.get('Full Duplicate Key', '')
                        if dup_key:
                            if '|' in dup_key:
                                order_part, cpt_part = dup_key.split('|', 1)
                                if len(order_part) > 30 and '-' in order_part:
                                    print(f"   ✅ Using UUID-based duplicate keys")
                                else:
                                    print(f"   ⚠️  Using non-UUID duplicate keys")
                            print(f"   Key format: {dup_key}")
                    
                except Exception as e:
                    print(f"   ⚠️  Could not verify Excel content: {str(e)}")
            
            # Success indicators
            new_records = summary.get('new_records', 0)
            duplicates = summary.get('duplicate_records', 0)
            yellow_warnings = summary.get('yellow_warnings', 0)
            
            if duplicates == 0 and yellow_warnings == 0:
                print(f"\n🎉 PERFECT BATCH:")
                print(f"   All {new_records} records are unique and ready for processing")
            elif duplicates > 0:
                print(f"\n⚠️  DUPLICATE ALERT:")
                print(f"   {duplicates} exact duplicates found (will not be paid)")
                print(f"   Check historical data - these may be resubmissions")
            
            if yellow_warnings > 0:
                print(f"\n🟡 MANUAL REVIEW REQUIRED:")
                print(f"   {yellow_warnings} records have same Order ID with different CPTs")
                print(f"   Review yellow-highlighted rows in Excel file")
            
            # Next steps
            print(f"\n📋 NEXT STEPS:")
            print(f"   1. Review Excel file: {batch_excel_path.name}")
            print(f"   2. Check duplicate analysis above")
            if yellow_warnings > 0:
                print(f"   3. ⚠️  Review {yellow_warnings} yellow-highlighted rows")
            print(f"   4. Import into QuickBooks: ${summary.get('release_amount', 0):,.2f}")
            print(f"   5. Proceed to Cell 6 for EOBR generation")
            
            # Store results for next cell
            print(f"\n📝 Variables ready for Cell 6:")
            print(f"   batch_excel_path: {batch_excel_path}")
            print(f"   output_dir: {output_dir}")
            print(f"   summary: {len(summary)} metrics")
            
        except Exception as e:
            print(f"\n❌ EXCEL GENERATION FAILED:")
            print(f"   Error: {str(e)}")
            print(f"   Check validation issues above")
            
            # Debug info
            if validated_bills:
                print(f"\n🔍 DEBUG INFO:")
                sample_bill = validated_bills[0]
                print(f"   Sample order_id: {sample_bill.get('order_id')}")
                print(f"   Sample FM record: {sample_bill.get('FileMaker_Record_Number')}")
                print(f"   Sample patient: {sample_bill.get('PatientName')}")
                print(f"   Sample line items: {len(sample_bill.get('line_items', []))}")

print(f"\n🎯 Excel generation step complete")

# %%
# %%
# Cell 5B: Excel Post-Processing - TEMPORARY REQUIREMENT
# =============================================================================
# Normalize dates, rename column F, and insert EOBR_NUMBER as new column G

import pandas as pd
import re
import random
import string
from datetime import datetime
from openpyxl.styles import numbers

print("STEP 5B: EXCEL POST-PROCESSING (TEMPORARY)")
print("=" * 50)

if 'batch_excel_path' not in locals() or not batch_excel_path:
    print("❌ No batch_excel_path available from Cell 5")
    print("   Please run Cell 5 first")
else:
    print(f"📄 Processing Excel file: {batch_excel_path.name}")
    
    # === Date conversion functions ===
    patterns = [
        (re.compile(r"(\d{4})-(\d{1,2})-(\d{1,2})"), '%Y-%m-%d'),     # 2024-01-15
        (re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})"), '%m.%d.%Y')    # 01.15.2024
    ]
    
    def convert_dates_in_string(text):
        """Convert YYYY-MM-DD and DD.MM.YYYY to MM/DD/YYYY format"""
        if pd.isna(text):
            return text
        
        s = str(text)
        for pattern, fmt in patterns:
            for match in pattern.findall(s):
                raw = '-'.join(match) if '-' in fmt else '.'.join(match)
                try:
                    dt = datetime.strptime(raw, fmt)
                    s = s.replace(raw, dt.strftime('%m/%d/%Y'))
                except ValueError:
                    continue
        return s
    
    def remove_trailing_time(text):
        """Remove time stamps from dates (MM/DD/YYYY 00:00:00 → MM/DD/YYYY)"""
        if pd.isna(text):
            return text
        
        s = str(text).strip()
        if re.match(r"\d{2}/\d{2}/\d{4} 00:00:00", s):
            return s.split()[0]
        return s
    
    # === EOBR number generation ===
    def create_suffix(length=7):
        """Generate random alphanumeric suffix"""
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    
    def extract_and_generate_eobr(text):
        """Generate EOBR number from date: EOBR-YYMMDD-XXXXXXX"""
        if isinstance(text, str) and len(text) >= 10:
            raw = text[:10]  # Extract first 10 chars (MM/DD/YYYY)
            try:
                dt = pd.to_datetime(raw, format='%m/%d/%Y', errors='coerce')
                if pd.notna(dt):
                    return f"EOBR-{dt.strftime('%y%m%d')}-{create_suffix()}"
            except:
                pass
        return None
    
    try:
        # === Load Excel file ===
        print("🔄 Loading Excel file...")
        sheets = pd.read_excel(batch_excel_path, sheet_name=None, dtype=str)
        print(f"   Found {len(sheets)} sheet(s)")
        
        # === Clean dates in all sheets ===
        print("📅 Converting date formats...")
        cleaned_sheets = {}
        date_conversions = 0
        
        for name, df in sheets.items():
            print(f"   Processing sheet: {name}")
            
            # Count cells before conversion
            original_cells = df.size
            
            # Apply date conversions
            df_converted = df.applymap(convert_dates_in_string)
            df_final = df_converted.applymap(remove_trailing_time)
            
            # Count changes
            changes = (df != df_final).sum().sum()
            date_conversions += changes
            
            cleaned_sheets[name] = df_final
            print(f"     Converted {changes} date cells")
        
        print(f"✅ Total date conversions: {date_conversions}")
        
        # === Process the main sheet (first sheet) ===
        main_sheet_name = list(cleaned_sheets.keys())[0]
        main_df = cleaned_sheets[main_sheet_name]
        
        print(f"\n🔧 Processing main sheet: {main_sheet_name}")
        print(f"   Original columns: {len(main_df.columns)}")
        
        # Remove any unnamed columns
        original_cols = len(main_df.columns)
        main_df = main_df.loc[:, ~main_df.columns.str.match(r'^Unnamed')]
        unnamed_removed = original_cols - len(main_df.columns)
        
        if unnamed_removed > 0:
            print(f"   Removed {unnamed_removed} unnamed columns")
        
        # 1) Rename column F (index 5) to FileMaker_Record_No
        if len(main_df.columns) > 5:
            old_col_f = main_df.columns[5]
            main_df.rename(columns={old_col_f: 'FileMaker_Record_No'}, inplace=True)
            print(f"   Renamed column F: '{old_col_f}' → 'FileMaker_Record_No'")
        else:
            print("   ⚠️  Not enough columns to rename column F")
        
        # 2) Identify Bill Date column (index 9 = column J)
        if len(main_df.columns) > 9:
            bill_date_col = main_df.columns[9]
            print(f"   Bill Date column (J): '{bill_date_col}'")
            
            # 3) Generate EOBR numbers from Bill Date
            print("🎯 Generating EOBR numbers...")
            eobr_numbers = main_df[bill_date_col].apply(extract_and_generate_eobr)
            
            # Count successful generations
            successful_eobrs = eobr_numbers.notna().sum()
            print(f"   Generated {successful_eobrs}/{len(eobr_numbers)} EOBR numbers")
            
            # 4) Insert EOBR_NUMBER as new column G (index 6)
            main_df.insert(6, 'EOBR_NUMBER', eobr_numbers)
            print(f"   Inserted EOBR_NUMBER as column G")
            
            # Show sample EOBR numbers
            sample_eobrs = eobr_numbers.dropna().head(3).tolist()
            if sample_eobrs:
                print(f"   Sample EOBR numbers:")
                for eobr in sample_eobrs:
                    print(f"     {eobr}")
        else:
            print("   ⚠️  Not enough columns to process Bill Date (column J)")
        
        # Update the cleaned sheets with the modified main sheet
        cleaned_sheets[main_sheet_name] = main_df
        
        print(f"   Final columns: {len(main_df.columns)}")
        
        # === Save back to the same file ===
        print(f"\n💾 Saving changes to Excel file...")
        
        with pd.ExcelWriter(batch_excel_path, engine='openpyxl') as writer:
            for name, df in cleaned_sheets.items():
                df.to_excel(writer, sheet_name=name, index=False)
            
            # Force all cells to text format
            workbook = writer.book
            for worksheet in workbook.worksheets:
                for row in worksheet.iter_rows():
                    for cell in row:
                        if cell.value is not None:
                            cell.number_format = numbers.FORMAT_TEXT
        
        print(f"✅ Excel file updated successfully")
        
        # === Verification ===
        print(f"\n🔍 VERIFICATION:")
        
        # Re-read the file to verify changes
        verification_df = pd.read_excel(batch_excel_path, sheet_name=main_sheet_name, dtype=str)
        
        print(f"   File size: {batch_excel_path.stat().st_size:,} bytes")
        print(f"   Rows: {len(verification_df)}")
        print(f"   Columns: {len(verification_df.columns)}")
        
        # Check key columns
        key_columns = ['FileMaker_Record_No', 'EOBR_NUMBER']
        for col in key_columns:
            if col in verification_df.columns:
                non_null = verification_df[col].notna().sum()
                print(f"   {col}: {non_null}/{len(verification_df)} populated")
            else:
                print(f"   ❌ {col}: Column not found")
        
        # Show column order
        print(f"\n📋 UPDATED COLUMN ORDER:")
        for i, col in enumerate(verification_df.columns):
            marker = ""
            if col == 'FileMaker_Record_No':
                marker = " (renamed F)"
            elif col == 'EOBR_NUMBER':
                marker = " (new G)"
            print(f"   {chr(65+i)} ({i+1}): {col}{marker}")
        
        print(f"\n🎯 Post-processing complete")
        print(f"📝 Updated file: {batch_excel_path}")
        
    except Exception as e:
        print(f"\n❌ POST-PROCESSING FAILED:")
        print(f"   Error: {str(e)}")
        print(f"   File may be in use or corrupted")
        
        import traceback
        print(f"\n🔍 FULL ERROR:")
        traceback.print_exc()

print(f"\n✅ Cell 5B complete - Excel file updated with requirements")

# %%
# Cell 6: Database Updates & Audit Log - SIMPLIFIED
# =============================================================================

import pandas as pd
from datetime import datetime
from pathlib import Path

print("STEP 6: DATABASE UPDATES & AUDIT LOG")
print("=" * 45)

def get_db_connection():
    """Get database connection - simplified version"""
    try:
        # Try the main database connection method
        from utils.data_validation import get_db_connection as get_conn
        return get_conn()
    except ImportError:
        try:
            # Fallback to direct SQLite connection
            import sqlite3
            db_path = "monolith.db"  # Adjust path as needed
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            return conn
        except Exception as e:
            print(f"❌ Database connection failed: {str(e)}")
            return None

def create_simple_audit_log(processed_bills, output_dir, batch_info):
    """Create simplified audit log"""
    audit_records = []
    
    for bill in processed_bills:
        # Calculate bill total
        line_items = bill.get('line_items', [])
        total_amount = sum(float(item.get('allowed_amount', 0)) for item in line_items)
        
        audit_record = {
            'audit_timestamp': datetime.now().isoformat(),
            'bill_id': bill.get('id'),
            'order_id': bill.get('Order_ID'),
            'fm_record': bill.get('FileMaker_Record_Number'),
            'patient_name': bill.get('PatientName'),
            'provider_name': bill.get('provider_billing_name'),
            'provider_tin': bill.get('provider_tin'),
            'line_items_count': len(line_items),
            'total_amount': total_amount,
            'status_before': 'REVIEWED',
            'status_after': 'PAID',
            'excel_file': batch_info.get('excel_file', ''),
            'process_date': datetime.now().strftime('%Y-%m-%d')
        }
        audit_records.append(audit_record)
    
    # Create audit DataFrame
    audit_df = pd.DataFrame(audit_records)
    
    # Save audit log
    audit_dir = output_dir / "audit"
    audit_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    audit_file = audit_dir / f"audit_log_{timestamp}.xlsx"
    
    # Create audit workbook with summary
    with pd.ExcelWriter(audit_file, engine='openpyxl') as writer:
        # Main audit data
        audit_df.to_excel(writer, sheet_name='Audit_Log', index=False)
        
        # Summary sheet
        summary_data = {
            'Metric': [
                'Processing Date',
                'Total Bills Processed',
                'Total Amount Processed',
                'Excel File Generated',
                'Database Updated',
                'Audit Timestamp'
            ],
            'Value': [
                datetime.now().strftime('%Y-%m-%d'),
                len(processed_bills),
                f"${audit_df['total_amount'].sum():,.2f}",
                batch_info.get('excel_file', 'Generated'),
                'Yes',
                datetime.now().isoformat()
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
    
    return audit_file, audit_df

def update_bills_as_paid(bill_ids, mark_as_paid=True):
    """Update ProviderBill records to mark as paid"""
    if not mark_as_paid:
        return {
            'success': False,
            'message': 'Database updates disabled (mark_as_paid=False)',
            'updated_count': 0
        }
    
    if not bill_ids:
        return {
            'success': False,
            'message': 'No bill IDs provided',
            'updated_count': 0
        }
    
    try:
        conn = get_db_connection()
        if not conn:
            return {
                'success': False,
                'message': 'Could not connect to database',
                'updated_count': 0
            }
        
        cursor = conn.cursor()
        
        # Update bills as paid
        placeholders = ','.join(['?' for _ in bill_ids])
        update_query = f"""
            UPDATE ProviderBill 
            SET bill_paid = 'Y', 
                updated_at = CURRENT_TIMESTAMP
            WHERE id IN ({placeholders})
        """
        
        cursor.execute(update_query, bill_ids)
        updated_count = cursor.rowcount
        
        conn.commit()
        conn.close()
        
        return {
            'success': True,
            'message': f'Successfully updated {updated_count} bills',
            'updated_count': updated_count
        }
        
    except Exception as e:
        return {
            'success': False,
            'message': f'Database update failed: {str(e)}',
            'updated_count': 0
        }

# Main execution
if 'excel_ready_bills' not in locals() or not excel_ready_bills:
    print("❌ No excel_ready_bills available")
    print("   Please run previous cells first")
else:
    # Get batch information
    batch_info = {
        'excel_file': batch_excel_path.name if 'batch_excel_path' in locals() else 'Unknown',
        'output_dir': output_dir if 'output_dir' in locals() else None,
        'summary': summary if 'summary' in locals() else {}
    }
    
    print(f"📊 Processing {len(excel_ready_bills)} bills...")
    print(f"📄 Excel file: {batch_info['excel_file']}")
    
    # Create audit log
    try:
        output_directory = batch_info['output_dir'] or Path("batch_outputs") / f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        output_directory.mkdir(parents=True, exist_ok=True)
        
        print(f"📋 Creating audit log...")
        audit_file, audit_df = create_simple_audit_log(
            excel_ready_bills, 
            output_directory, 
            batch_info
        )
        
        print(f"✅ Audit log created: {audit_file}")
        print(f"   Records: {len(audit_df)}")
        print(f"   Total amount: ${audit_df['total_amount'].sum():,.2f}")
        
    except Exception as e:
        print(f"❌ Audit log creation failed: {str(e)}")
        audit_file = None
    
    # Database updates
    print(f"\n🔄 Database Updates...")
    
    # Set this to True when ready to actually update the database
    MARK_AS_PAID = True  # Change to False to disable database updates
    
    if MARK_AS_PAID:
        print(f"⚠️  WARNING: This will mark {len(excel_ready_bills)} bills as PAID in the database")
        print(f"   This action cannot be easily undone!")
        
        # Get bill IDs
        bill_ids = [bill.get('id') for bill in excel_ready_bills if bill.get('id')]
        
        if bill_ids:
            print(f"🚀 Updating {len(bill_ids)} bills...")
            
            update_result = update_bills_as_paid(bill_ids, mark_as_paid=True)
            
            if update_result['success']:
                print(f"✅ Database update successful!")
                print(f"   Updated: {update_result['updated_count']} bills")
                print(f"   Status: {update_result['message']}")
                
                # Verify updates
                try:
                    conn = get_db_connection()
                    if conn:
                        cursor = conn.cursor()
                        placeholders = ','.join(['?' for _ in bill_ids])
                        verify_query = f"""
                            SELECT COUNT(*) as paid_count 
                            FROM ProviderBill 
                            WHERE id IN ({placeholders}) AND bill_paid = 'Y'
                        """
                        cursor.execute(verify_query, bill_ids)
                        paid_count = cursor.fetchone()[0]
                        conn.close()
                        
                        print(f"✅ Verification: {paid_count}/{len(bill_ids)} bills marked as paid")
                        
                        if paid_count != len(bill_ids):
                            print(f"⚠️  Some bills may not have been updated correctly")
                    
                except Exception as e:
                    print(f"⚠️  Could not verify updates: {str(e)}")
                
            else:
                print(f"❌ Database update failed!")
                print(f"   Error: {update_result['message']}")
        else:
            print(f"❌ No valid bill IDs found")
    else:
        print(f"🔒 Database updates DISABLED")
        print(f"   Set MARK_AS_PAID = True to enable database updates")
        print(f"   {len(excel_ready_bills)} bills would be marked as paid")
    
    # Summary
    print(f"\n📊 CELL 6 SUMMARY:")
    print(f"   Bills processed: {len(excel_ready_bills)}")
    
    if 'audit_file' in locals() and audit_file:
        print(f"   Audit log: {audit_file.name}")
    else:
        print(f"   Audit log: Failed to create")
    
    if MARK_AS_PAID and 'update_result' in locals():
        if update_result['success']:
            print(f"   Database: ✅ {update_result['updated_count']} bills marked as paid")
        else:
            print(f"   Database: ❌ Update failed")
    else:
        print(f"   Database: 🔒 Updates disabled")
    
    # File structure summary
    if 'output_directory' in locals():
        print(f"\n📁 OUTPUT STRUCTURE:")
        print(f"   {output_directory}/")
        print(f"   ├── excel/")
        print(f"   │   └── {batch_info['excel_file']}")
        print(f"   └── audit/")
        if 'audit_file' in locals() and audit_file:
            print(f"       └── {audit_file.name}")
    
    # Next steps
    print(f"\n📋 NEXT STEPS:")
    print(f"   1. ✅ Excel file ready for QuickBooks import")
    print(f"   2. ✅ Audit trail created for compliance")
    
    if MARK_AS_PAID and 'update_result' in locals() and update_result['success']:
        print(f"   3. ✅ Database updated - bills marked as paid")
        print(f"   4. 🎯 Process complete - ready for EOBR generation (Cell 7)")
    elif not MARK_AS_PAID:
        print(f"   3. ⚠️  Enable database updates when ready")
        print(f"   4. 🎯 Proceed to EOBR generation (Cell 7)")
    else:
        print(f"   3. ❌ Fix database update issues")
        print(f"   4. ⚠️  Do not proceed until database is updated")

print(f"\n🎯 Cell 6 complete")

# Prepare variables for next cell
if 'excel_ready_bills' in locals():
    print(f"📝 Variables ready for Cell 7:")
    print(f"   excel_ready_bills: {len(excel_ready_bills)} bills")
    if 'output_directory' in locals():
        print(f"   output_directory: {output_directory}")
    if 'audit_file' in locals():
        print(f"   audit_file: {audit_file}")

# %%
# %%
# Cell 7: EOBR Document Generation - SIMPLIFIED
# =============================================================================

from utils.eobr_generator import EOBRGenerator
from pathlib import Path
from datetime import datetime
import re

print("STEP 7: EOBR DOCUMENT GENERATION")
print("=" * 40)

def validate_eobr_data(bill):
    """Check if bill has required data for EOBR generation"""
    required_fields = {
        'PatientName': bill.get('PatientName'),
        'Patient_DOB': bill.get('Patient_DOB'),
        'Order_ID': bill.get('Order_ID'),
        'FileMaker_Record_Number': bill.get('FileMaker_Record_Number'),
        'provider_tin': bill.get('provider_tin'),
        'provider_npi': bill.get('provider_npi'),
        'provider_billing_name': bill.get('provider_billing_name'),
        'provider_billing_address1': bill.get('provider_billing_address1'),
        'provider_billing_city': bill.get('provider_billing_city'),
        'provider_billing_state': bill.get('provider_billing_state'),
        'provider_billing_postal_code': bill.get('provider_billing_postal_code'),
        'line_items': bill.get('line_items', [])
    }
    
    missing = []
    for field, value in required_fields.items():
        if not value or (isinstance(value, list) and len(value) == 0):
            missing.append(field)
    
    # Check line items
    line_items = bill.get('line_items', [])
    if line_items:
        for i, item in enumerate(line_items):
            if not item.get('cpt_code'):
                missing.append(f"Line {i+1} CPT code")
            if not item.get('date_of_service'):
                missing.append(f"Line {i+1} date of service")
            if item.get('allowed_amount') is None:
                missing.append(f"Line {i+1} allowed amount")
    
    return len(missing) == 0, missing

def prepare_eobr_data(bill):
    """Prepare bill data for EOBR generation with any needed fixes"""
    eobr_bill = bill.copy()
    
    # Fix common date format issues
    patient_dob = eobr_bill.get('Patient_DOB', '')
    if patient_dob and isinstance(patient_dob, str):
        # Fix format like '07 28 1997' → '07/28/1997'
        if re.match(r'\d{2} \d{2} \d{4}', patient_dob.strip()):
            parts = patient_dob.strip().split()
            if len(parts) == 3:
                eobr_bill['Patient_DOB'] = f"{parts[0]}/{parts[1]}/{parts[2]}"
    
    # Remove timestamps from injury date
    injury_date = eobr_bill.get('Patient_Injury_Date', '')
    if injury_date and ' ' in str(injury_date):
        eobr_bill['Patient_Injury_Date'] = str(injury_date).split(' ')[0]
    
    # Ensure line items have required fields
    line_items = eobr_bill.get('line_items', [])
    for item in line_items:
        # Default place of service if missing
        if not item.get('place_of_service'):
            item['place_of_service'] = '11'
        
        # Ensure units is set
        if not item.get('units'):
            item['units'] = 1
    
    return eobr_bill

def get_eobr_filename_from_excel(bill, batch_excel_path):
    """Get EOBR filename from Excel EOBR_NUMBER column (column G)"""
    try:
        import pandas as pd
        
        # Read Excel file to find EOBR number
        df = pd.read_excel(batch_excel_path, dtype=str)
        
        bill_id = bill.get('id', '').strip()
        order_id = bill.get('order_id', '').strip()
        
        # Try to match by Input File first
        matching_row = None
        if 'Input File' in df.columns:
            df['Input File'] = df['Input File'].astype(str).str.strip()
            matching_row = df[df['Input File'] == bill_id]
        
        # Fallback: try Order ID
        if (matching_row is None or matching_row.empty) and 'Order ID' in df.columns:
            df['Order ID'] = df['Order ID'].astype(str).str.strip()
            matching_row = df[df['Order ID'] == order_id]
        
        # Get EOBR number from column G (EOBR_NUMBER)
        if matching_row is not None and not matching_row.empty:
            if 'EOBR_NUMBER' in df.columns:
                eobr_number = matching_row.iloc[0]['EOBR_NUMBER']
                if eobr_number and str(eobr_number).strip() not in ['nan', '', 'None']:
                    eobr_clean = str(eobr_number).strip()
                    return f"{eobr_clean}.docx"
        
        # Fallback to FileMaker record
        fm_record = bill.get('FileMaker_Record_Number', 'Unknown')
        return f"EOBR_{fm_record}.docx"
        
    except Exception as e:
        # Final fallback
        fm_record = bill.get('FileMaker_Record_Number', 'Unknown')
        return f"EOBR_{fm_record}.docx"

if 'excel_ready_bills' not in locals() or not excel_ready_bills:
    print("❌ No excel_ready_bills available")
    print("   Please run previous cells first")
else:
    print(f"🚀 Generating EOBRs for {len(excel_ready_bills)} bills...")
    
    # Validate bills for EOBR generation
    eobr_ready_bills = []
    validation_issues = []
    
    for bill in excel_ready_bills:
        is_ready, missing_fields = validate_eobr_data(bill)
        
        if is_ready:
            prepared_bill = prepare_eobr_data(bill)
            eobr_ready_bills.append(prepared_bill)
        else:
            validation_issues.append({
                'bill_id': bill.get('id'),
                'patient_name': bill.get('PatientName'),
                'missing_fields': missing_fields
            })
    
    print(f"✅ EOBR-ready bills: {len(eobr_ready_bills)}")
    print(f"❌ Failed validation: {len(validation_issues)}")
    
    # Show validation issues
    if validation_issues:
        print(f"\n⚠️  EOBR VALIDATION ISSUES:")
        for issue in validation_issues[:3]:  # Show first 3
            print(f"   {issue['bill_id']} ({issue['patient_name']}):")
            for field in issue['missing_fields'][:3]:  # Show first 3 fields
                print(f"     • Missing: {field}")
        
        if len(validation_issues) > 3:
            print(f"   ... and {len(validation_issues) - 3} more bills with issues")
    
    if not eobr_ready_bills:
        print(f"\n❌ No bills ready for EOBR generation")
        print(f"   Please fix validation issues above")
    else:
        # DEBUG: Check what data is being passed to EOBR
        print(f"\n🔍 DATA VALIDATION CHECK:")
        sample_bill = eobr_ready_bills[0]
        print(f"   PatientName: '{sample_bill.get('PatientName')}'")
        print(f"   provider_billing_city: '{sample_bill.get('provider_billing_city')}'")
        print(f"   provider_billing_name: '{sample_bill.get('provider_billing_name')}'")
        print(f"   provider_billing_state: '{sample_bill.get('provider_billing_state')}'")
        print(f"   provider_billing_postal_code: '{sample_bill.get('provider_billing_postal_code')}'")
        print(f"   Order_ID: '{sample_bill.get('Order_ID')}'")
        print(f"   FileMaker_Record_Number: '{sample_bill.get('FileMaker_Record_Number')}'")
        print(f"   Patient_Injury_Date: '{sample_bill.get('Patient_Injury_Date')}'")
        
        line_items = sample_bill.get('line_items', [])
        print(f"   Line items count: {len(line_items)}")
        if len(line_items) > 1:
            print(f"   Second line item units: '{line_items[1].get('units')}'")
        else:
            print(f"   Second line item: NOT AVAILABLE")
            
        # Setup EOBR output directory
        output_directory = output_directory if 'output_directory' in locals() else Path("batch_outputs") / f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        eobr_output_dir = output_directory / "eobrs"
        eobr_output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"\n📁 EOBR output directory: {eobr_output_dir}")
        
        try:
            # Initialize EOBR generator
            generator = EOBRGenerator()
            print(f"📋 Template: {generator.template_path}")
            
            generated_files = []
            total_amount = 0
            
            print(f"\n📄 Generating {len(eobr_ready_bills)} EOBR documents...")
            
            for i, bill in enumerate(eobr_ready_bills, 1):
                try:
                    # Get filename from Excel EOBR_NUMBER
                    filename = get_eobr_filename_from_excel(bill, batch_excel_path)
                    output_path = eobr_output_dir / filename
                    
                    # Generate EOBR
                    success = generator.generate_eobr(bill, output_path)
                    
                    if success:
                        generated_files.append(output_path)
                        
                        # Calculate amount
                        line_items = bill.get('line_items', [])
                        bill_amount = sum(float(item.get('allowed_amount', 0)) for item in line_items)
                        total_amount += bill_amount
                        
                except Exception as e:
                    pass  # Continue processing other files
            
            print(f"\n📊 RESULTS: {len(generated_files)}/{len(eobr_ready_bills)} generated, ${total_amount:,.2f}")
            
            if generated_files:
                total_size = sum(f.stat().st_size for f in generated_files if f.exists())
                print(f"📁 Location: {eobr_output_dir} ({total_size/1024/1024:.1f} MB)")
                
                print(f"\n✅ EOBR GENERATION COMPLETE!")
                print(f"🎯 Ready for Cell 7B (renaming) and Cell 8 (PDF conversion)")
                
            else:
                print(f"\n❌ NO EOBR FILES GENERATED")
                
        except Exception as e:
            print(f"\n❌ EOBR GENERATION FAILED: {str(e)}")

print(f"\n🎯 EOBR generation complete")

# %%
# Cell 8: PDF Conversion & File Organization - SIMPLIFIED
# =============================================================================

from pathlib import Path
import os
from datetime import datetime

print("STEP 8: PDF CONVERSION & FILE ORGANIZATION")
print("=" * 50)

def convert_docx_to_pdf(docx_path, pdf_path):
    """Convert DOCX to PDF using available methods"""
    
    # Method 1: Microsoft Word COM (Windows - best quality)
    try:
        import win32com.client
        
        word = win32com.client.Dispatch('Word.Application')
        word.Visible = False
        word.DisplayAlerts = False
        
        doc = word.Documents.Open(str(docx_path.absolute()))
        doc.ExportAsFixedFormat(
            OutputFileName=str(pdf_path.absolute()),
            ExportFormat=17,  # PDF format
            OptimizeFor=0,    # Optimize for print
            BitmapMissingFonts=True
        )
        
        doc.Close()
        word.Quit()
        return True
        
    except ImportError:
        pass
    except Exception as e:
        print(f"   ⚠️  Word COM failed: {str(e)}")
    
    # Method 2: docx2pdf (cross-platform alternative)
    try:
        from docx2pdf import convert
        convert(str(docx_path), str(pdf_path))
        return True
        
    except ImportError:
        print(f"   ❌ Install: pip install docx2pdf")
        return False
    except Exception as e:
        print(f"   ❌ docx2pdf failed: {str(e)}")
        return False

def organize_eobr_files(eobr_directory, test_mode=False):
    """Convert DOCX to PDF and organize files"""
    
    eobr_dir = Path(eobr_directory)
    
    if not eobr_dir.exists():
        return {
            'success': False,
            'message': f'EOBR directory not found: {eobr_dir}',
            'results': {}
        }
    
    # Find DOCX files
    docx_files = list(eobr_dir.glob("*.docx"))
    
    if not docx_files:
        return {
            'success': False,
            'message': f'No DOCX files found in {eobr_dir}',
            'results': {}
        }
    
    print(f"📂 EOBR Directory: {eobr_dir}")
    print(f"📋 Found {len(docx_files)} DOCX files")
    
    # Create organized directories
    pdf_dir = eobr_dir / "pdfs"
    docx_archive_dir = eobr_dir / "word_docx"
    
    pdf_dir.mkdir(exist_ok=True)
    docx_archive_dir.mkdir(exist_ok=True)
    
    print(f"📁 PDF Output: pdfs/")
    print(f"📁 DOCX Archive: word_docx/")
    
    # Results tracking
    converted_pdfs = []
    archived_docx = []
    failed_conversions = []
    
    # Process files
    files_to_process = [docx_files[0]] if test_mode else docx_files
    
    if test_mode:
        print(f"\n🧪 TEST MODE: Processing 1 file only")
    
    print(f"\n🔄 PROCESSING FILES:")
    print("=" * 30)
    
    for i, docx_file in enumerate(files_to_process, 1):
        print(f"\n{i:2d}. {docx_file.name}")
        
        try:
            # Convert to PDF
            pdf_filename = docx_file.stem + ".pdf"
            pdf_path = pdf_dir / pdf_filename
            
            print(f"    🔄 Converting to PDF...")
            success = convert_docx_to_pdf(docx_file, pdf_path)
            
            if success and pdf_path.exists():
                pdf_size = pdf_path.stat().st_size
                converted_pdfs.append(pdf_path)
                print(f"    ✅ PDF created ({pdf_size:,} bytes)")
                
                # Move DOCX to archive
                archive_path = docx_archive_dir / docx_file.name
                docx_file.rename(archive_path)
                archived_docx.append(archive_path)
                print(f"    📦 DOCX archived")
                
            else:
                failed_conversions.append(docx_file)
                print(f"    ❌ PDF conversion failed")
                
        except Exception as e:
            failed_conversions.append(docx_file)
            print(f"    ❌ Error: {str(e)}")
    
    # Summary
    print(f"\n📊 CONVERSION SUMMARY:")
    print("=" * 25)
    print(f"   Files processed: {len(files_to_process)}")
    print(f"   PDFs created: {len(converted_pdfs)}")
    print(f"   DOCX archived: {len(archived_docx)}")
    print(f"   Failed: {len(failed_conversions)}")
    
    if len(files_to_process) > 0:
        success_rate = len(converted_pdfs) / len(files_to_process) * 100
        print(f"   Success rate: {success_rate:.1f}%")
    
    if converted_pdfs:
        total_pdf_size = sum(f.stat().st_size for f in converted_pdfs)
        print(f"   Total PDF size: {total_pdf_size/1024/1024:.1f} MB")
    
    # Show failed files
    if failed_conversions:
        print(f"\n❌ FAILED CONVERSIONS:")
        for failed in failed_conversions[:3]:
            print(f"      {failed.name}")
        if len(failed_conversions) > 3:
            print(f"      ... and {len(failed_conversions) - 3} more")
    
    return {
        'success': len(converted_pdfs) > 0,
        'test_mode': test_mode,
        'results': {
            'total_files': len(docx_files),
            'processed_files': len(files_to_process),
            'converted_pdfs': converted_pdfs,
            'archived_docx': archived_docx,
            'failed_conversions': failed_conversions,
            'pdf_directory': pdf_dir,
            'docx_directory': docx_archive_dir
        }
    }

# Main execution
if 'output_dir' not in locals():
    print("❌ No output_dir from previous cells")
    print("   Please run previous cells first")
else:
    # Find EOBR directory
    eobr_directory = output_dir / "eobrs"
    
    if not eobr_directory.exists():
        print(f"❌ EOBR directory not found: {eobr_directory}")
        print("   Please run Cell 7 (EOBR Generation) first")
    else:
        # Configuration
        TEST_MODE = False  # Set to False to process all files
        
        print(f"⚙️  CONFIGURATION:")
        print(f"   Test mode: {TEST_MODE}")
        if TEST_MODE:
            print(f"   Will process 1 file only (for testing)")
        else:
            print(f"   Will process all DOCX files")
        
        # Check dependencies
        print(f"\n🔍 CHECKING DEPENDENCIES:")
        
        try:
            import win32com.client
            print(f"   ✅ Microsoft Word COM available (best quality)")
        except ImportError:
            print(f"   ⚠️  Word COM not available")
            
            try:
                import docx2pdf
                print(f"   ✅ docx2pdf available (alternative method)")
            except ImportError:
                print(f"   ❌ docx2pdf not available")
                print(f"      Install with: pip install docx2pdf")
                print(f"      Or install Microsoft Word for best quality")
        
        # Run conversion
        result = organize_eobr_files(eobr_directory, test_mode=TEST_MODE)
        
        if result['success']:
            results = result['results']
            
            if result['test_mode']:
                print(f"\n🎉 TEST SUCCESSFUL!")
                print(f"✅ PDF conversion and file organization working")
                print(f"📁 Test PDF: {results['pdf_directory']}")
                print(f"📦 Test archive: {results['docx_directory']}")
                
                print(f"\n🚀 TO PROCESS ALL FILES:")
                print(f"   1. Set TEST_MODE = False")
                print(f"   2. Re-run this cell")
                print(f"   3. Will process all {results['total_files']} files")
                
            else:
                print(f"\n🎉 CONVERSION COMPLETE!")
                print(f"✅ Processed {len(results['converted_pdfs'])} files")
                
                # Show final structure
                print(f"\n📂 FINAL FILE ORGANIZATION:")
                print(f"   eobrs/")
                print(f"   ├── pdfs/              ({len(results['converted_pdfs'])} PDF files)")
                print(f"   │   ├── [EOBR_Name].pdf")
                print(f"   │   └── ...")
                print(f"   └── word_docx/         ({len(results['archived_docx'])} DOCX files)")
                print(f"       ├── [EOBR_Name].docx")
                print(f"       └── ...")
                
                # Sample files
                if results['converted_pdfs']:
                    print(f"\n📄 SAMPLE PDF FILES:")
                    for i, pdf_file in enumerate(results['converted_pdfs'][:3], 1):
                        size = pdf_file.stat().st_size
                        print(f"   {i}. {pdf_file.name} ({size:,} bytes)")
                    
                    if len(results['converted_pdfs']) > 3:
                        remaining = len(results['converted_pdfs']) - 3
                        print(f"   ... and {remaining} more PDFs")
                
                print(f"\n✅ EOBR FILES READY FOR DISTRIBUTION!")
                print(f"   📧 Email PDFs to providers")
                print(f"   📁 Access files in: {results['pdf_directory']}")
        
        else:
            print(f"\n❌ CONVERSION FAILED!")
            print(f"   {result.get('message', 'Unknown error')}")
            
            print(f"\n🔧 TROUBLESHOOTING:")
            print(f"   1. Install Microsoft Word (best option)")
            print(f"   2. Or install: pip install docx2pdf")
            print(f"   3. Check file permissions")
            print(f"   4. Ensure DOCX files are not open in Word")

print(f"\n🎯 PDF conversion and organization complete")

# Show next steps
print(f"\n📋 WORKFLOW STATUS:")
print(f"   ✅ Step 1-7: Bills processed through EOBR generation")
if 'result' in locals() and result.get('success'):
    if result.get('test_mode'):
        print(f"   🧪 Step 8: PDF conversion tested (set TEST_MODE=False for full)")
    else:
        print(f"   ✅ Step 8: PDF conversion and organization complete")
        print(f"\n🎉 COMPLETE WORKFLOW FINISHED!")
        print(f"   📊 Excel file ready for QuickBooks")
        print(f"   📄 PDF EOBRs ready for provider distribution")
        print(f"   📋 Complete audit trail maintained")
        print(f"   🗂️  All files organized and archived")

# %%
# Path to the Excel file and PDF directory
excel_path = batch_excel_path
docx_dir = output_dir / "eobrs"

# Load the Excel file
df = pd.read_excel(excel_path)

# Loop through each row
for idx, row in df.iterrows():
    old_id = str(row[3]).strip()  # Column B
    new_name = str(row[6]).strip()  # Column D

    old_docx = os.path.join(docx_dir, f"{old_id}.docx")
    new_docx = os.path.join(docx_dir, f"{new_name}.docx")

    if os.path.exists(old_docx):
        os.rename(old_docx, new_docx)
        print(f"Renamed: {old_id}.docx → {new_name}.docx")
    else:
        print(f"Missing: {old_id}.docx not found")

print("Done.")



