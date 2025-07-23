# %% [markdown]
# ### Notebook to run Postprocess_main

# %% [markdown]
# -- Cell 1: Pull Data

# %%
import logging
import sys
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import data pulling functions
from utils.data_validation import (
    get_approved_unpaid_bills,
    inspect_bill_line_item_table
)

print("=" * 60)
print("STEP 1: PULLING DATA")
print("=" * 60)

# Optional: Inspect table structure first
print("Inspecting table structure...")
inspect_bill_line_item_table()

# Pull approved unpaid bills
print(f"\nPulling approved unpaid bills...")
bills = get_approved_unpaid_bills(limit=500)  # Adjust limit as needed

print(f"✅ Successfully pulled {len(bills)} bills ready for postprocessing")
if bills:
    print(f"📋 Sample bill: {bills[0].get('PatientName')} - Bill ID: {bills[0].get('id')}")


# %% [markdown]
# -- Cell 2: Validate Data

# %%
from utils.data_validation import (
    validate_bill_data,
    print_validation_summary
)

print("\n" + "=" * 60)
print("STEP 2: VALIDATING DATA")
print("=" * 60)

if bills:
    # Validate all the bills
    validation_report = validate_bill_data(bills)
    
    # Print detailed summary
    print_validation_summary(validation_report)
    
    # Extract valid bills for next steps
    valid_bills_data = validation_report.get('valid_bills', [])
    invalid_bills_data = validation_report.get('invalid_bills', [])
    
    print(f"\n✅ Validation complete:")
    print(f"   Valid bills: {len(valid_bills_data)}")
    print(f"   Invalid bills: {len(invalid_bills_data)}")
    
    if invalid_bills_data:
        print(f"\n⚠️  Invalid bills summary:")
        for invalid_bill in invalid_bills_data[:3]:  # Show first 3
            bill_id = invalid_bill.get('bill_id')
            issues = invalid_bill.get('bill_validation', {}).get('missing_fields', [])
            print(f"   Bill {bill_id}: Missing {', '.join(issues[:3])}")
else:
    print("❌ No bills to validate")
    valid_bills_data = []


# %% [markdown]
# -- Cell 3: Data Cleaning

# %%
# Cell 3: Enhanced Data Cleaning (Fixed None Values)
# =============================================================================

import re
from datetime import datetime

from datetime import datetime

def simple_standardize_date_format(date_str: str) -> str:
    """
    Standardize date strings to MM/DD/YYYY format.

    Args:
        date_str: Raw date string (like "11/25/24" or "2024-11-25")

    Returns:
        Standardized date string in MM/DD/YYYY format
    """
    if not date_str:
        return ""
    
    date_str = str(date_str).strip()

    # Handle date ranges - take the first date
    if ' - ' in date_str:
        date_str = date_str.split(' - ')[0].strip()

    formats_to_try = [
        '%Y-%m-%d',      # 2024-11-25
        '%m/%d/%y',      # 11/25/24
        '%m/%d/%Y',      # 11/25/2024
        '%Y/%m/%d',      # 2024/11/25
        '%m-%d-%y',      # 11-25-24
        '%m-%d-%Y',      # 11-25-2024
    ]
    
    for fmt in formats_to_try:
        try:
            parsed_date = datetime.strptime(date_str, fmt).date()
            
            # Normalize 2-digit years
            if parsed_date.year < 100:
                if parsed_date.year <= 30:
                    parsed_date = parsed_date.replace(year=parsed_date.year + 2000)
                else:
                    parsed_date = parsed_date.replace(year=parsed_date.year + 1900)

            if 1900 <= parsed_date.year <= 2030:
                return parsed_date.strftime('%m/%d/%Y')
        
        except ValueError:
            continue

    print(f"⚠️  Could not parse date: '{date_str}'")
    return date_str


def clean_none_value(value):
    """
    Clean 'None', 'null', empty strings and return empty string if invalid.
    
    Args:
        value: Any value that might be 'None' string
        
    Returns:
        Cleaned value or empty string
    """
    if value is None:
        return ''
    
    str_value = str(value).strip()
    if str_value.lower() in ['none', 'null', '']:
        return ''
    
    return str_value

def clean_bills_enhanced(valid_bills_data):
    """
    Enhanced cleaning with proper None value handling for Excel generation.
    """
    print("🧹 ENHANCED DATA CLEANING (Fixed None Values)")
    print("=" * 60)
    
    cleaned_bills = []
    cleaning_issues = []
    
    for bill_result in valid_bills_data:
        try:
            # Get the bill data and line items from validation result
            bill_data = bill_result['bill_data'].copy()
            bill_line_items = bill_result.get('line_items', [])
            
            bill_id = bill_data.get('id')
            print(f"\nCleaning bill {bill_id} ({bill_data.get('PatientName', 'Unknown')})...")
            
            # Clean bill line items
            cleaned_line_items = []
            for item in bill_line_items:
                cleaned_item = item.copy()
                
                # 1. Clean date of service
                if 'date_of_service' in cleaned_item and cleaned_item['date_of_service']:
                    original_date = cleaned_item['date_of_service']
                    cleaned_date = simple_standardize_date_format(original_date)
                    cleaned_item['date_of_service'] = cleaned_date
                    if original_date != cleaned_date:
                        print(f"   📅 Date: {original_date} → {cleaned_date}")
                
                # 2. Clean charge amounts
                if 'charge_amount' in cleaned_item and cleaned_item['charge_amount'] is not None:
                    try:
                        cleaned_item['charge_amount'] = float(cleaned_item['charge_amount'])
                    except (ValueError, TypeError):
                        print(f"   ⚠️  Invalid charge_amount: {cleaned_item['charge_amount']} → 0.0")
                        cleaned_item['charge_amount'] = 0.0
                
                # 3. Clean allowed amounts
                if 'allowed_amount' in cleaned_item and cleaned_item['allowed_amount'] is not None:
                    try:
                        cleaned_item['allowed_amount'] = float(cleaned_item['allowed_amount'])
                    except (ValueError, TypeError):
                        print(f"   ⚠️  Invalid allowed_amount: {cleaned_item['allowed_amount']} → 0.0")
                        cleaned_item['allowed_amount'] = 0.0
                
                # 4. Clean units
                if 'units' in cleaned_item and cleaned_item['units'] is not None:
                    try:
                        cleaned_item['units'] = int(cleaned_item['units'])
                        if cleaned_item['units'] <= 0:
                            cleaned_item['units'] = 1
                    except (ValueError, TypeError):
                        print(f"   ⚠️  Invalid units: {cleaned_item['units']} → 1")
                        cleaned_item['units'] = 1
                else:
                    cleaned_item['units'] = 1
                
                # 5. Clean CPT code
                if 'cpt_code' in cleaned_item:
                    cpt = str(cleaned_item['cpt_code']).strip().upper()
                    cleaned_item['cpt_code'] = cpt
                
                # 6. Clean modifier (keep only common valid ones)
                if 'modifier' in cleaned_item and cleaned_item['modifier']:
                    modifier = str(cleaned_item['modifier']).strip()
                    valid_modifiers = {'LT', 'RT', '26', 'TC'}
                    kept_modifiers = []
                    for mod in modifier.split(','):
                        mod = mod.strip().upper()
                        if mod in valid_modifiers:
                            kept_modifiers.append(mod)
                    cleaned_item['modifier'] = ','.join(kept_modifiers)
                else:
                    cleaned_item['modifier'] = ''
                
                # 7. Ensure place_of_service has a default
                if not cleaned_item.get('place_of_service'):
                    cleaned_item['place_of_service'] = '11'  # Default to office
                
                # 8. Clean reason_code (handle None values)
                cleaned_item['reason_code'] = clean_none_value(cleaned_item.get('reason_code'))
                
                cleaned_line_items.append(cleaned_item)
            
            # Add cleaned line items to bill
            bill_data['line_items'] = cleaned_line_items
            
            # Clean key bill fields for Excel generation
            
            # Patient name
            if 'PatientName' in bill_data:
                bill_data['PatientName'] = clean_none_value(bill_data['PatientName'])
            
            # Provider billing name
            if 'provider_billing_name' in bill_data:
                bill_data['provider_billing_name'] = clean_none_value(bill_data['provider_billing_name'])
            
            # FileMaker Record Number
            if 'FileMaker_Record_Number' in bill_data:
                bill_data['FileMaker_Record_Number'] = clean_none_value(bill_data['FileMaker_Record_Number'])
            
            # Provider TIN (format XX-XXXXXXX)
            if 'provider_tin' in bill_data and bill_data['provider_tin']:
                tin = re.sub(r'\D', '', str(bill_data['provider_tin']))
                if len(tin) == 9:
                    bill_data['provider_tin'] = f"{tin[0:2]}-{tin[2:9]}"
            
            # ENHANCED ADDRESS CLEANING (Fix None values)
            print(f"   🏠 Cleaning address fields...")
            
            # Clean individual address fields first
            bill_data['provider_billing_address1'] = clean_none_value(bill_data.get('provider_billing_address1'))
            bill_data['provider_billing_address2'] = clean_none_value(bill_data.get('provider_billing_address2'))
            bill_data['provider_billing_city'] = clean_none_value(bill_data.get('provider_billing_city'))
            bill_data['provider_billing_state'] = clean_none_value(bill_data.get('provider_billing_state'))
            
            # Clean ZIP code with None handling
            zip_raw = bill_data.get('provider_billing_postal_code')
            if zip_raw and clean_none_value(zip_raw):
                zip_str = clean_none_value(zip_raw)
                # Clean ZIP code format
                digits = re.sub(r'\D', '', zip_str)
                if len(digits) >= 5:
                    clean_zip = digits[:5]
                    if len(digits) >= 9:
                        clean_zip = f"{digits[:5]}-{digits[5:9]}"
                    bill_data['provider_billing_postal_code'] = clean_zip
                else:
                    bill_data['provider_billing_postal_code'] = zip_str
            else:
                bill_data['provider_billing_postal_code'] = ''
            
            # Build formatted address (only include non-empty parts)
            address_parts = []
            
            # Address Line 1
            if bill_data['provider_billing_address1']:
                address_parts.append(bill_data['provider_billing_address1'])
            
            # Address Line 2 (only if not empty)
            if bill_data['provider_billing_address2']:
                address_parts.append(bill_data['provider_billing_address2'])
            
            # City
            if bill_data['provider_billing_city']:
                address_parts.append(bill_data['provider_billing_city'])
            
            # State
            if bill_data['provider_billing_state']:
                address_parts.append(bill_data['provider_billing_state'])
            
            # ZIP
            if bill_data['provider_billing_postal_code']:
                address_parts.append(bill_data['provider_billing_postal_code'])
            
            # Store formatted address for easy access
            bill_data['formatted_address'] = ', '.join(address_parts)
            
            print(f"   📍 Address: {bill_data['formatted_address']}")
            
            # Add to cleaned bills
            cleaned_bills.append(bill_data)
            
            print(f"   ✅ Successfully cleaned: {len(cleaned_line_items)} line items")
            
        except Exception as e:
            print(f"   ❌ Error cleaning bill {bill_id}: {str(e)}")
            cleaning_issues.append({
                'bill_id': bill_id,
                'error': str(e)
            })
    
    # Print cleaning summary
    print(f"\n🧹 ENHANCED CLEANING SUMMARY:")
    print(f"   Total bills processed: {len(valid_bills_data)}")
    print(f"   Successfully cleaned: {len(cleaned_bills)}")
    print(f"   Cleaning errors: {len(cleaning_issues)}")
    
    if cleaning_issues:
        print(f"\n   ❌ Cleaning Issues:")
        for issue in cleaning_issues:
            print(f"      {issue['bill_id']}: {issue['error']}")
    
    return cleaned_bills

# Run the enhanced cleaning
print("STEP 3: ENHANCED DATA CLEANING (FIXED NONE VALUES)")
print("=" * 60)

if 'valid_bills_data' in locals() and valid_bills_data:
    cleaned_bills = clean_bills_enhanced(valid_bills_data)
    
    print(f"\n✅ Enhanced cleaning complete!")
    print(f"   Cleaned bills: {len(cleaned_bills)}")
    
    # Show sample of what we have
    if cleaned_bills:
        sample_bill = cleaned_bills[0]
        sample_line_items = sample_bill.get('line_items', [])
        print(f"\n📋 Sample cleaned data:")
        print(f"   Patient: '{sample_bill.get('PatientName')}'")
        print(f"   Provider: '{sample_bill.get('provider_billing_name')}'")
        print(f"   Address 1: '{sample_bill.get('provider_billing_address1')}'")
        print(f"   Address 2: '{sample_bill.get('provider_billing_address2')}'")
        print(f"   Formatted Address: '{sample_bill.get('formatted_address')}'")
        print(f"   Line items: {len(sample_line_items)}")
        if sample_line_items:
            sample_item = sample_line_items[0]
            print(f"   Sample CPT: {sample_item.get('cpt_code')} on {sample_item.get('date_of_service')}")
            print(f"   Amount: ${sample_item.get('allowed_amount'):.2f}")
    
else:
    print("❌ No valid bills available for enhanced cleaning")
    cleaned_bills = []

print(f"\n🎯 Ready for Cell 4 verification!")

# %% [markdown]
# -- Cell 4: validate for Excel Gen

# %%
# Cell 4: Data Verification & Excel Preparation (Enhanced for Spot Checking)
# =============================================================================
import uuid
from datetime import datetime

def format_dos(dos_str):
    try:
        return datetime.strptime(dos_str, "%Y-%m-%d").strftime("%y%m%d")
    except Exception:
        return "000000"

print("STEP 4: DATA VERIFICATION & EXCEL PREPARATION")
print("=" * 60)

if 'cleaned_bills' in locals() and cleaned_bills:
    print(f"📊 FINAL DATA SUMMARY:")
    print(f"   Total bills cleaned: {len(cleaned_bills)}")
    
    # Count total line items and amounts
    total_line_items = 0
    total_amount = 0.0
    bills_with_issues = []
    
    print(f"\n📋 DETAILED BREAKDOWN:")
    
    for i, bill in enumerate(cleaned_bills):
        line_items = bill.get('line_items', [])
        bill_total = sum(float(item.get('allowed_amount', 0)) for item in line_items)
        total_amount += bill_total
        total_line_items += len(line_items)
        
        print(f"   Bill {i+1}: {bill.get('PatientName', 'Unknown')}")
        print(f"      ID: {bill.get('id')}")
        print(f"      FM Record: {bill.get('FileMaker_Record_Number', 'N/A')}")
        print(f"      Line Items: {len(line_items)}")
        print(f"      Total Amount: ${bill_total:.2f}")
        print(f"      Provider: {bill.get('provider_billing_name', 'Unknown')}")
        print(f"      TIN: {bill.get('provider_tin', 'N/A')}")
        
        # Show sample line items
        if line_items:
            cpt_list = [item.get('cpt_code', 'N/A') for item in line_items[:3]]
            print(f"      CPTs: {', '.join(cpt_list)}")
        print()
        
        # Check for Excel generation requirements
        issues = []
        if not bill.get('PatientName'):
            issues.append("Missing patient name")
        if not bill.get('FileMaker_Record_Number'):
            issues.append("Missing FileMaker record")
        if not bill.get('provider_billing_name'):
            issues.append("Missing provider name")
        if not bill.get('provider_tin'):
            issues.append("Missing provider TIN")
        if not line_items:
            issues.append("No line items")
        
        if issues:
            bills_with_issues.append({
                'bill_id': bill.get('id'),
                'issues': issues
            })
    
    print(f"💰 TOTALS:")
    print(f"   Total Bills: {len(cleaned_bills)}")
    print(f"   Total Line Items: {total_line_items}")
    print(f"   Total Amount: ${total_amount:.2f}")
    
    # Excel generation readiness check
    print(f"\n🔍 EXCEL GENERATION READINESS:")
    
    if bills_with_issues:
        print(f"   ⚠️  {len(bills_with_issues)} bills have issues:")
        for issue_bill in bills_with_issues[:3]:  # Show first 3
            print(f"      {issue_bill['bill_id']}: {', '.join(issue_bill['issues'])}")
        if len(bills_with_issues) > 3:
            print(f"      ... and {len(bills_with_issues) - 3} more")
    else:
        print(f"   ✅ All bills ready for Excel generation!")
    
    # Prepare bills for Excel generation (filter out problematic ones if needed)
    excel_ready_bills = []
    for bill in cleaned_bills:
        if (bill.get('PatientName') and 
            bill.get('FileMaker_Record_Number') and 
            bill.get('provider_billing_name') and
            bill.get('line_items')):
            excel_ready_bills.append(bill)
    
    print(f"\n📊 EXCEL GENERATION SUMMARY:")
    print(f"   Bills ready for Excel: {len(excel_ready_bills)}")
    print(f"   Bills with issues: {len(cleaned_bills) - len(excel_ready_bills)}")
    
    if excel_ready_bills:
        excel_total_amount = sum(
            sum(float(item.get('allowed_amount', 0)) for item in bill.get('line_items', []))
            for bill in excel_ready_bills
        )
        print(f"   Excel batch amount: ${excel_total_amount:.2f}")
        
        # DETAILED EXCEL DATA PREVIEW FOR SPOT CHECKING
        print(f"\n" + "="*80)
        print(f"🔍 COMPLETE EXCEL DATA PREVIEW (FOR SPOT CHECKING)")
        print(f"="*80)
        
        print(f"\nThis is EXACTLY what will be sent to the Excel generator:")
        
        for i, bill in enumerate(excel_ready_bills):
            print(f"\n{'='*60}")
            print(f"BILL {i+1} - COMPLETE EXCEL GENERATION DATA")
            print(f"{'='*60}")
            
            # All bill-level data
            print(f"📋 BILL INFORMATION:")
            print(f"   Bill ID: '{bill.get('id', '')}'")
            print(f"   Patient Name: '{bill.get('PatientName', '')}'")
            print(f"   FileMaker Record: '{bill.get('FileMaker_Record_Number', '')}'")
            print(f"   Order ID: '{bill.get('Order_ID', '')}'")
            print(f"   Claim ID: '{bill.get('claim_id', '')}'")
            
            print(f"\n🏢 PROVIDER INFORMATION:")
            print(f"   Provider Name: '{bill.get('provider_billing_name', '')}'")
            print(f"   Provider TIN: '{bill.get('provider_tin', '')}'")
            print(f"   Provider NPI: '{bill.get('provider_npi', '')}'")
            
            print(f"\n📍 PROVIDER ADDRESS:")
            print(f"   Address 1: '{bill.get('provider_billing_address1', '')}'")
            print(f"   Address 2: '{bill.get('provider_billing_address2', '')}'")
            print(f"   City: '{bill.get('provider_billing_city', '')}'")
            print(f"   State: '{bill.get('provider_billing_state', '')}'")
            print(f"   ZIP: '{bill.get('provider_billing_postal_code', '')}'")
            print(f"   Formatted: '{bill.get('formatted_address', '')}'")
            
            # Line items details
            line_items = bill.get('line_items', [])
            print(f"\n💊 LINE ITEMS ({len(line_items)} items):")
            
            for j, item in enumerate(line_items):
                print(f"   --- Line Item {j+1} ---")
                print(f"   CPT Code: '{item.get('cpt_code', '')}'")
                print(f"   Date of Service: '{item.get('date_of_service', '')}'")
                print(f"   Charge Amount: ${item.get('charge_amount', 0):.2f}")
                print(f"   Allowed Amount: ${item.get('allowed_amount', 0):.2f}")
                print(f"   Units: {item.get('units', 1)}")
                print(f"   Modifier: '{item.get('modifier', '')}'")
                print(f"   Place of Service: '{item.get('place_of_service', '')}'")
                print(f"   Decision: '{item.get('decision', '')}'")
                print(f"   Reason Code: '{item.get('reason_code', '')}'")
                print()
            
            # Calculate what Excel will generate
            bill_total = sum(float(item.get('allowed_amount', 0)) for item in line_items)
            cpt_codes = [item.get('cpt_code', '') for item in line_items]
            earliest_dos = min([item.get('date_of_service', '') for item in line_items], default='')
            
            print(f"📊 EXCEL CALCULATIONS:")
            print(f"   Total Amount: ${bill_total:.2f}")
            print(f"   CPT Codes: {', '.join(cpt_codes)}")
            print(f"   Earliest DOS: {earliest_dos}")
            print(f"   Duplicate Key: {bill.get('FileMaker_Record_Number', '')}|{','.join(sorted(cpt_codes))}")
            
            # ✅ EOBR Number Generation
            formatted_dos = format_dos(earliest_dos)
            uuid_suffix = uuid.uuid4().hex[:7].upper()
            eobr_number = f"EOBR-{formatted_dos}-{uuid_suffix}"
            bill['EOBR_Number'] = eobr_number
            
            # Show what Excel row will look like
            print(f"\n📝 PREDICTED EXCEL ROW:")
            print(f"   Vendor: {bill.get('provider_billing_name', '')}")
            print(f"   EOBR Number: {eobr_number}")
            print(f"   Bill Date: {earliest_dos}")
            print(f"   Amount: ${bill_total:.2f}")
            print(f"   Description: {earliest_dos}, {', '.join(sorted(cpt_codes))}, {bill.get('PatientName', '')}, {bill.get('FileMaker_Record_Number', '')}")
            print(f"   Memo: {earliest_dos}, {bill.get('PatientName', '')}")
            
            if i < len(excel_ready_bills) - 1:
                print(f"\n{'─'*60}")
                print(f"(Bill {i+1} complete - {len(excel_ready_bills) - i - 1} more bills below)")
                print(f"{'─'*60}")
        
        print(f"\n" + "="*80)
        print(f"🎯 SPOT CHECK SUMMARY")
        print(f"="*80)
        print(f"✅ Ready to generate Excel with {len(excel_ready_bills)} bills")
        print(f"✅ Total amount: ${excel_total_amount:.2f}")
        print(f"✅ All required fields present")
        print(f"✅ All data cleaned and formatted")
        print(f"\n🚀 Proceed to Cell 5 for Excel generation!")
        
    else:
        print(f"\n❌ No bills ready for Excel generation")

else:
    print("❌ No cleaned bills available for verification")
    excel_ready_bills = []

print(f"\n✅ Verification complete - {len(excel_ready_bills) if 'excel_ready_bills' in locals() else 0} bills ready for Excel generation")


# %%
# Fix Order ID to Use UUID Field (Option B)
# =============================================================================

print("STEP 4C: FIXING ORDER_ID TO USE UUID FIELD")
print("=" * 60)

def fix_order_id_to_uuid(bills_list):
    """
    Fix the order_id field to use the UUID Order_ID field instead of filename.
    
    Args:
        bills_list: List of bill dictionaries
        
    Returns:
        List of bills with corrected order_id field
    """
    updated_bills = []
    
    for bill in bills_list:
        # Create a copy of the bill to avoid modifying original
        updated_bill = bill.copy()
        
        # Get the UUID Order_ID field
        uuid_order_id = bill.get('Order_ID')
        
        if uuid_order_id:
            # Use the UUID as order_id
            updated_bill['order_id'] = str(uuid_order_id)
            print(f"   ✅ Updated order_id to UUID '{uuid_order_id}' for bill {bill.get('id', 'unknown')}")
        else:
            print(f"   ❌ No Order_ID found for bill {bill.get('id', 'unknown')}")
        
        updated_bills.append(updated_bill)
    
    return updated_bills

# Check current state
if 'excel_ready_bills' in locals() and excel_ready_bills:
    print(f"📊 Fixing order_id for {len(excel_ready_bills)} bills...")
    
    # Show current vs desired
    sample_bill = excel_ready_bills[0]
    current_order_id = sample_bill.get('order_id')
    uuid_order_id = sample_bill.get('Order_ID')
    
    print(f"\n📄 Sample bill comparison:")
    print(f"   Current order_id: {current_order_id}")
    print(f"   UUID Order_ID: {uuid_order_id}")
    print(f"   FileMaker_Record_Number: {sample_bill.get('FileMaker_Record_Number')}")
    
    # Fix the order_id values
    print(f"\n🔄 Updating order_id to use UUID values...")
    excel_ready_bills_fixed = fix_order_id_to_uuid(excel_ready_bills)
    
    # Validate the results
    bills_with_uuid_order_id = 0
    for bill in excel_ready_bills_fixed:
        order_id = bill.get('order_id', '')
        # Check if it looks like a UUID (has dashes and is longer)
        if order_id and '-' in order_id and len(order_id) > 30:
            bills_with_uuid_order_id += 1
    
    print(f"\n📊 RESULTS:")
    print(f"   Total bills: {len(excel_ready_bills_fixed)}")
    print(f"   Bills with UUID order_id: {bills_with_uuid_order_id}")
    print(f"   Success rate: {(bills_with_uuid_order_id/len(excel_ready_bills_fixed)*100):.1f}%")
    
    # Update the variable for Excel generation
    excel_ready_bills = excel_ready_bills_fixed
    
    print(f"\n✅ Bills updated with UUID order_id field")
    print(f"   Variable 'excel_ready_bills' now uses UUID for order_id")
    
    # Show sample of updated bill
    if bills_with_uuid_order_id > 0:
        updated_sample = excel_ready_bills[0]
        print(f"\n📄 Sample updated bill:")
        print(f"   id: {updated_sample.get('id')}")
        print(f"   order_id: {updated_sample.get('order_id')}")
        print(f"   FileMaker_Record_Number: {updated_sample.get('FileMaker_Record_Number')}")
        print(f"   PatientName: {updated_sample.get('PatientName')}")
        
        # Show what the duplicate key will look like
        if 'line_items' in updated_sample and updated_sample['line_items']:
            sample_cpts = []
            for item in updated_sample['line_items'][:3]:  # First 3 CPTs
                cpt = item.get('cpt_code', '')
                if cpt:
                    sample_cpts.append(cpt)
            
            if sample_cpts:
                sample_dup_key = f"{updated_sample.get('order_id')}|{','.join(sample_cpts)}"
                print(f"   Sample duplicate key: {sample_dup_key}")

else:
    print("❌ No excel_ready_bills found - run previous steps first")

print(f"\n🎯 NEXT: Run the Excel generation step with UUID-based order_id")
print(f"📝 Duplicate keys will now use format: UUID|CPT1,CPT2,CPT3")

# %% [markdown]
# -- Cell 5: Excel Gen

# %%
# Step 5: Order ID-Based Excel Generation (FIXED VERSION WITH RELOAD)
# =============================================================================

# Force reload the excel_generator module to pick up latest changes
import importlib
import sys
if 'utils.excel_generator' in sys.modules:
    importlib.reload(sys.modules['utils.excel_generator'])

from utils.excel_generator import generate_excel_batch  # Import after reload
from pathlib import Path
from datetime import datetime

print("STEP 5: ORDER ID-BASED EXCEL GENERATION (WITH MODULE RELOAD)")
print("=" * 60)

# Simple inline validation function
def validate_bill_for_processing(bill):
    """Simple validation for required fields."""
    required_fields = ['order_id', 'FileMaker_Record_Number', 'PatientName', 'line_items']
    for field in required_fields:
        if field not in bill or not bill[field]:
            return False, f"Missing required field: {field}"
    return True, ""

if 'excel_ready_bills' in locals() and excel_ready_bills and len(excel_ready_bills) > 0:
    
    # FIRST: Debug the bill structure to confirm UUID order_id
    print(f"🔍 DEBUGGING BILL STRUCTURE:")
    sample_bill = excel_ready_bills[0]
    print(f"   Sample bill ID: {sample_bill.get('id')}")
    print(f"   Sample order_id: {sample_bill.get('order_id')}")
    print(f"   Sample FileMaker_Record_Number: {sample_bill.get('FileMaker_Record_Number')}")
    print(f"   Sample PatientName: {sample_bill.get('PatientName')}")
    
    # Check if order_id looks like a UUID
    order_id = sample_bill.get('order_id', '')
    if order_id and len(order_id) > 30 and '-' in order_id:
        print(f"   ✅ order_id appears to be UUID format")
    else:
        print(f"   ❌ order_id does NOT appear to be UUID format")
        print(f"   🔧 Expected UUID format like: 9A7546EF-D14D-46B2-8EE8-AB16255B4F12")
        print(f"   🔧 Got: {order_id}")
    
    # Show what the expected duplicate key should look like
    if 'line_items' in sample_bill and sample_bill['line_items']:
        sample_cpts = []
        for item in sample_bill['line_items'][:3]:  # First 3 CPTs
            cpt = item.get('cpt_code', '')
            if cpt:
                sample_cpts.append(cpt)
        
        if sample_cpts and order_id:
            expected_dup_key = f"{order_id}|{','.join(sorted(sample_cpts))}"
            print(f"   🔑 Expected duplicate key format:")
            print(f"      {expected_dup_key}")
    
    # Validate bills have required fields for new system
    print(f"\n🔍 Validating {len(excel_ready_bills)} bills for order_id requirements...")
    
    valid_bills = []
    validation_errors = []
    
    for i, bill in enumerate(excel_ready_bills):
        is_valid, error_msg = validate_bill_for_processing(bill)
        if is_valid:
            valid_bills.append(bill)
        else:
            validation_errors.append(f"Bill {i+1} ({bill.get('id', 'unknown')}): {error_msg}")
    
    print(f"   ✅ Valid bills: {len(valid_bills)}")
    print(f"   ❌ Invalid bills: {len(validation_errors)}")
    
    if validation_errors:
        print(f"\n⚠️  VALIDATION ERRORS:")
        for error in validation_errors[:10]:  # Show first 10 errors
            print(f"      {error}")
        if len(validation_errors) > 10:
            print(f"      ... and {len(validation_errors) - 10} more errors")
        
        print(f"\n🔧 REQUIRED FIELDS FOR NEW SYSTEM:")
        print(f"   - order_id (UUID format for duplicate checking)")
        print(f"   - FileMaker_Record_Number (for EOBR numbering)")
        print(f"   - PatientName")
        print(f"   - line_items (with cpt_code and allowed_amount)")
    
    if len(valid_bills) == 0:
        print(f"\n❌ No valid bills to process - fix validation errors first")
    else:
        # Setup output directory
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = Path("batch_outputs") / f"batch_{timestamp}"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"\n🚀 Generating Excel batch for {len(valid_bills)} valid bills...")
        print(f"📁 Output directory: {output_dir}")
        print(f"🔑 New system: UUID order_id + CPT duplicate checking")
        print(f"📋 EOBR numbering: Still based on FileMaker_Record_Number")
        print(f"⚠️  Module reloaded to use latest excel_generator.py")
        
        try:
            # Generate Excel batch
            batch_excel_path, summary = generate_excel_batch(
                bills=valid_bills,
                batch_output_dir=output_dir
            )
            
            print(f"\n✅ Excel generation successful!")
            print(f"📄 Batch file: {batch_excel_path}")
            
            # Display comprehensive summary
            print(f"\n📊 BATCH SUMMARY:")
            print(f"   Batch file: {batch_excel_path.name}")
            print(f"   Total records: {summary.get('total_records', 0)}")
            print(f"   New records: {summary.get('new_records', 0)}")
            print(f"   Exact duplicates (order_id + CPTs): {summary.get('duplicate_records', 0)}")
            print(f"   Same order, different CPTs warnings: {summary.get('yellow_warnings', 0)}")
            print(f"   Total amount: ${summary.get('total_amount', 0):.2f}")
            print(f"   Release amount: ${summary.get('release_amount', 0):.2f}")
            print(f"   Generation time: {summary.get('generation_time', 'Unknown')}")
            
            # File verification
            if batch_excel_path.exists():
                file_size = batch_excel_path.stat().st_size
                print(f"\n✅ FILE VERIFICATION:")
                print(f"   File exists: Yes")
                print(f"   File size: {file_size:,} bytes")
                print(f"   Full path: {batch_excel_path.absolute()}")
                
                if file_size > 1000:
                    print(f"   ✅ Excel file appears to have content")
                    
                    # Verify Excel content
                    try:
                        import pandas as pd
                        df = pd.read_excel(batch_excel_path)
                        print(f"   📋 Excel verification:")
                        print(f"      Rows: {len(df)}")
                        print(f"      Columns: {len(df.columns)}")
                        print(f"      Column names: {list(df.columns)}")
                        
                        # Verify Order ID column exists
                        if 'Order ID' in df.columns:
                            print(f"      ✅ Order ID column present")
                            non_null_order_ids = df['Order ID'].notna().sum()
                            print(f"      📋 Order IDs populated: {non_null_order_ids}/{len(df)}")
                            
                            # Show sample Order IDs
                            if non_null_order_ids > 0:
                                sample_order_ids = df['Order ID'].dropna().head(3).tolist()
                                print(f"      📄 Sample Order IDs:")
                                for oid in sample_order_ids:
                                    print(f"         {oid}")
                        else:
                            print(f"      ❌ Order ID column missing!")
                        
                        # Show sample data
                        if len(df) > 0:
                            print(f"   📄 Sample Excel data:")
                            sample_row = df.iloc[0]
                            print(f"      Order ID: {sample_row.get('Order ID', 'N/A')}")
                            print(f"      EOBR Number: {sample_row.get('EOBR Number', 'N/A')}")
                            print(f"      Amount: ${sample_row.get('Amount', 0):.2f}")
                            print(f"      Duplicate Check: {sample_row.get('Duplicate Check', 'N/A')}")
                            print(f"      Full Duplicate Key: {sample_row.get('Full Duplicate Key', 'N/A')}")
                            
                            # IMPORTANT: Check if duplicate key is using UUID or FileMaker record
                            dup_key = sample_row.get('Full Duplicate Key', '')
                            if dup_key:
                                key_parts = dup_key.split('|')
                                if len(key_parts) > 0:
                                    key_identifier = key_parts[0]
                                    if len(key_identifier) > 30 and '-' in key_identifier:
                                        print(f"      ✅ Duplicate key using UUID format")
                                    else:
                                        print(f"      ❌ Duplicate key using FileMaker format")
                                        print(f"         Key identifier: {key_identifier}")
                                        print(f"         Expected UUID format with dashes")
                            
                            # Show duplicate breakdown
                            duplicate_breakdown = df['Duplicate Check'].value_counts()
                            print(f"   📊 Duplicate Check Breakdown:")
                            for status, count in duplicate_breakdown.items():
                                if status == 'Y':
                                    print(f"      🔴 Exact duplicates (Y): {count}")
                                elif status == 'YELLOW':
                                    print(f"      🟡 Same order, different CPTs (YELLOW): {count}")
                                elif status == 'N':
                                    print(f"      🟢 New records (N): {count}")
                            
                            # Show sample Order ID → Duplicate Key mapping
                            if 'Order ID' in df.columns and 'Full Duplicate Key' in df.columns:
                                print(f"   🔑 Sample Order ID → Duplicate Key mapping:")
                                for i in range(min(3, len(df))):
                                    row = df.iloc[i]
                                    order_id_display = row.get('Order ID', 'N/A')
                                    if len(str(order_id_display)) > 15:
                                        order_id_display = str(order_id_display)[:12] + "..."
                                    dup_key = row.get('Full Duplicate Key', 'N/A')
                                    print(f"      {order_id_display} → {dup_key}")
                            
                    except Exception as e:
                        print(f"   ⚠️  Could not verify Excel content: {str(e)}")
                else:
                    print(f"   ⚠️  Excel file seems unusually small")
            else:
                print(f"   ❌ Excel file was not created")
            
            # Duplicate analysis
            duplicate_count = summary.get('duplicate_records', 0)
            yellow_count = summary.get('yellow_warnings', 0)
            
            if duplicate_count > 0:
                print(f"\n🔴 EXACT DUPLICATE ANALYSIS:")
                print(f"   Found {duplicate_count} exact duplicate records")
                print(f"   These records have identical order_id + CPT combinations")
                print(f"   Status: 'Release Payment: N' - Will NOT be processed for payment")
                print(f"   ⚠️  If all bills are duplicates, check duplicate key format above")
            
            if yellow_count > 0:
                print(f"\n🟡 SAME ORDER, DIFFERENT CPTs WARNING:")
                print(f"   Found {yellow_count} records with same order_id but different CPTs")
                print(f"   Status: 'Release Payment: Y' - Will be processed but FLAGGED for manual review")
                print(f"   Action required: Review yellow-highlighted rows in Excel file")
            
            if duplicate_count == 0 and yellow_count == 0:
                print(f"\n🎉 NO DUPLICATES FOUND:")
                print(f"   All records have unique order_id + CPT combinations")
            
            # Next steps
            print(f"\n📋 NEXT STEPS:")
            print(f"   1. Review the Excel file: {batch_excel_path}")
            print(f"   2. ✅ Verify Order ID column is populated with UUIDs")
            print(f"   3. ✅ Check that EOBR numbers are based on FileMaker Record Numbers")
            print(f"   4. ✅ Confirm duplicate keys use UUID format (not FileMaker format)")
            
            if yellow_count > 0:
                print(f"   5. ⚠️  IMPORTANT: Review {yellow_count} yellow-highlighted rows")
                print(f"      - Same order_id with different CPT combinations")
                print(f"      - Determine if these are legitimate additional services")
            
            print(f"   6. Import into QuickBooks if everything looks correct")
            print(f"   7. Total to be processed: ${summary.get('release_amount', 0):.2f}")
            
            # Processing summary
            total_records = summary.get('total_records', 0)
            if total_records > 0:
                print(f"\n📈 PROCESSING SUMMARY:")
                new_pct = (summary.get('new_records', 0) / total_records) * 100
                dup_pct = (duplicate_count / total_records) * 100 if duplicate_count > 0 else 0
                yellow_pct = (yellow_count / total_records) * 100 if yellow_count > 0 else 0
                
                print(f"   🟢 New order_id + CPT combinations: {summary.get('new_records', 0)} ({new_pct:.1f}%)")
                if duplicate_count > 0:
                    print(f"   🔴 Exact duplicates (blocked): {duplicate_count} ({dup_pct:.1f}%)")
                if yellow_count > 0:
                    print(f"   🟡 Same order, different CPTs (review): {yellow_count} ({yellow_pct:.1f}%)")
                
                # If 100% duplicates, provide troubleshooting
                if dup_pct == 100.0:
                    print(f"\n🚨 TROUBLESHOOTING - 100% DUPLICATES:")
                    print(f"   This suggests the duplicate key format might be wrong")
                    print(f"   Expected: UUID|CPT1,CPT2 (like 9A7546EF-...|72141,72148)")
                    print(f"   Check the 'Duplicate key using UUID/FileMaker format' message above")
                    print(f"   If using FileMaker format, the module reload may not have worked")
        
        except Exception as e:
            print(f"❌ Excel generation failed: {str(e)}")
            
            import traceback
            print(f"\n🔍 ERROR DETAILS:")
            print(f"   Error type: {type(e).__name__}")
            print(f"   Error message: {str(e)}")
            
            if valid_bills:
                print(f"\n🔍 DEBUGGING INFO:")
                first_bill = valid_bills[0]
                print(f"   Sample order_id: {first_bill.get('order_id', 'NOT_FOUND')}")
                print(f"   Sample FileMaker_Record_Number: {first_bill.get('FileMaker_Record_Number', 'NOT_FOUND')}")
                print(f"   Sample PatientName: {first_bill.get('PatientName', 'NOT_FOUND')}")
            
            print(f"\n📋 FULL TRACEBACK:")
            traceback.print_exc()

else:
    print("❌ No Excel-ready bills available for generation")
    print("   Please check the validation and cleaning steps above")

print(f"\n🎉 Order ID-based Excel generation step completed!")

# Final summary
if 'batch_excel_path' in locals() and batch_excel_path.exists():
    print(f"\n✅ SUCCESS: Order ID-based Excel batch generated!")
    print(f"📁 File: {batch_excel_path}")
    if 'summary' in locals():
        yellow_count = summary.get('yellow_warnings', 0)
        if yellow_count > 0:
            print(f"⚠️  ATTENTION: {yellow_count} records need manual review")
        print(f"🔑 Duplicate checking: UUID-based order_id + CPT combinations")
        print(f"📋 EOBR numbering: FileMaker Record Numbers")
else:
    print(f"\n❌ No Excel file generated - check errors above")

# %% [markdown]
# -- Cell 4: validate for Excel Gen

# %% [markdown]
# -- Cell 6: Audit Log and Database updates

# %%
# Cell 6: Audit Log Generation (AUDIT ONLY - DB Updates Moved to Cell 6B)
# =============================================================================

import pandas as pd
from datetime import datetime
from pathlib import Path

def create_comprehensive_audit_log(excel_ready_bills, batch_excel_path=None, summary=None):
    """
    Create comprehensive audit log with all IDs and relationships.
    
    Args:
        excel_ready_bills: List of cleaned bill dictionaries
        batch_excel_path: Path to the generated Excel file
        summary: Summary from Excel generation
        
    Returns:
        Dictionary with different audit DataFrames
    """
    print("📋 CREATING COMPREHENSIVE AUDIT LOG")
    print("=" * 60)
    
    # 1. BILL SUMMARY AUDIT
    bill_audit_records = []
    
    # 2. LINE ITEMS AUDIT  
    line_item_audit_records = []
    
    # 3. PROVIDER AUDIT
    provider_audit_records = []
    
    # 4. ORDER AUDIT
    order_audit_records = []
    
    # 5. PROCESSING AUDIT
    processing_audit_records = []
    
    for bill in excel_ready_bills:
        bill_id = bill.get('id')
        order_id = bill.get('Order_ID')
        claim_id = bill.get('claim_id')
        provider_id = bill.get('provider_id')
        fm_record = bill.get('FileMaker_Record_Number')
        
        print(f"   Auditing bill {bill_id}...")
        
        # === BILL SUMMARY AUDIT ===
        bill_audit = {
            'audit_timestamp': datetime.now().isoformat(),
            'bill_id': bill_id,
            'order_id': order_id,
            'claim_id': claim_id,
            'provider_id': provider_id,
            'fm_record_number': fm_record,
            'patient_name': bill.get('PatientName', ''),
            'patient_dob': bill.get('Patient_DOB', ''),
            'total_charge': bill.get('total_charge', 0),
            'bill_status_before': 'REVIEWED',
            'bill_action_before': 'apply_rate',
            'bill_paid_before': bill.get('bill_paid', 'N'),
            'bill_paid_after': 'Y',  # Will be updated after processing
            'assigning_company': bill.get('Assigning_Company', ''),
            'claim_number': bill.get('Claim_Number', ''),
            'referring_physician': bill.get('Referring_Physician', ''),
            'jurisdiction_state': bill.get('Jurisdiction_State', ''),
            'bundle_type': bill.get('bundle_type', '')
        }
        bill_audit_records.append(bill_audit)
        
        # === PROVIDER AUDIT ===
        provider_audit = {
            'audit_timestamp': datetime.now().isoformat(),
            'bill_id': bill_id,
            'provider_id': provider_id,
            'provider_name': bill.get('provider_name', ''),
            'provider_billing_name': bill.get('provider_billing_name', ''),
            'provider_tin': bill.get('provider_tin', ''),
            'provider_npi': bill.get('provider_npi', ''),
            'provider_network': bill.get('provider_network', ''),
            'billing_address1': bill.get('provider_billing_address1', ''),
            'billing_address2': bill.get('provider_billing_address2', ''),
            'billing_city': bill.get('provider_billing_city', ''),
            'billing_state': bill.get('provider_billing_state', ''),
            'billing_zip': bill.get('provider_billing_postal_code', ''),
            'formatted_address': bill.get('formatted_address', ''),
            'provider_phone': bill.get('provider_phone', ''),
            'provider_fax': bill.get('provider_fax', '')
        }
        provider_audit_records.append(provider_audit)
        
        # === ORDER AUDIT ===
        order_audit = {
            'audit_timestamp': datetime.now().isoformat(),
            'bill_id': bill_id,
            'order_id': order_id,
            'fm_record_number': fm_record,
            'patient_name': bill.get('PatientName', ''),
            'patient_first_name': bill.get('Patient_First_Name', ''),
            'patient_last_name': bill.get('Patient_Last_Name', ''),
            'patient_dob': bill.get('Patient_DOB', ''),
            'patient_address': bill.get('Patient_Address', ''),
            'patient_city': bill.get('Patient_City', ''),
            'patient_state': bill.get('Patient_State', ''),
            'patient_zip': bill.get('Patient_Zip', ''),
            'patient_phone': bill.get('PatientPhone', ''),
            'patient_injury_date': bill.get('Patient_Injury_Date', ''),
            'order_type': bill.get('Order_Type', ''),
            'bundle_type': bill.get('bundle_type', '')
        }
        order_audit_records.append(order_audit)
        
        # === LINE ITEMS AUDIT ===
        line_items = bill.get('line_items', [])
        total_line_amount = 0
        
        for i, item in enumerate(line_items):
            line_item_audit = {
                'audit_timestamp': datetime.now().isoformat(),
                'bill_id': bill_id,
                'line_item_id': item.get('id'),
                'line_number': i + 1,
                'provider_bill_id': item.get('provider_bill_id'),
                'cpt_code': item.get('cpt_code', ''),
                'modifier': item.get('modifier', ''),
                'units': item.get('units', 1),
                'date_of_service': item.get('date_of_service', ''),
                'place_of_service': item.get('place_of_service', ''),
                'charge_amount': item.get('charge_amount', 0),
                'allowed_amount': item.get('allowed_amount', 0),
                'decision': item.get('decision', ''),
                'reason_code': item.get('reason_code', ''),
                'diagnosis_pointer': item.get('diagnosis_pointer', ''),
                'proc_category': item.get('category', ''),
                'proc_subcategory': item.get('subcategory', ''),
                'proc_description': item.get('proc_desc', '')
            }
            line_item_audit_records.append(line_item_audit)
            total_line_amount += float(item.get('allowed_amount', 0))
        
        # === PROCESSING AUDIT ===
        # Calculate what will happen in Excel generation
        cpt_codes = [item.get('cpt_code', '') for item in line_items]
        earliest_dos = min([item.get('date_of_service', '') for item in line_items], default='')
        duplicate_key = f"{fm_record}|{','.join(sorted(cpt_codes))}"
        
        processing_audit = {
            'audit_timestamp': datetime.now().isoformat(),
            'bill_id': bill_id,
            'fm_record_number': fm_record,
            'duplicate_key': duplicate_key,
            'line_items_count': len(line_items),
            'total_amount': total_line_amount,
            'earliest_dos': earliest_dos,
            'cpt_codes': ', '.join(sorted(cpt_codes)),
            'predicted_vendor': bill.get('provider_billing_name', ''),
            'predicted_eobr_pattern': f"{fm_record}-X",
            'predicted_bill_date': earliest_dos,
            'predicted_description': f"{earliest_dos}, {', '.join(sorted(cpt_codes))}, {bill.get('PatientName', '')}, {fm_record}",
            'predicted_memo': f"{earliest_dos}, {bill.get('PatientName', '')}",
            'will_be_duplicate': 'TBD',  # Will be filled after Excel generation
            'excel_row_number': 'TBD'
        }
        processing_audit_records.append(processing_audit)
    
    # Create DataFrames
    audit_dfs = {
        'bill_summary': pd.DataFrame(bill_audit_records),
        'line_items': pd.DataFrame(line_item_audit_records),
        'providers': pd.DataFrame(provider_audit_records),
        'orders': pd.DataFrame(order_audit_records),
        'processing': pd.DataFrame(processing_audit_records)
    }
    
    print(f"   ✅ Created audit records:")
    print(f"      Bills: {len(bill_audit_records)}")
    print(f"      Line Items: {len(line_item_audit_records)}")
    print(f"      Providers: {len(provider_audit_records)}")
    print(f"      Orders: {len(order_audit_records)}")
    print(f"      Processing: {len(processing_audit_records)}")
    
    return audit_dfs

def save_audit_log_excel(audit_dfs, output_dir, batch_excel_path=None):
    """
    Save audit log to Excel file with multiple sheets.
    
    Args:
        audit_dfs: Dictionary of audit DataFrames
        output_dir: Output directory
        batch_excel_path: Path to main Excel file (for reference)
        
    Returns:
        Path to audit Excel file
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    audit_filename = f"audit_log_{timestamp}.xlsx"
    audit_path = output_dir / "audit" / audit_filename
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"💾 SAVING AUDIT LOG")
    print(f"   File: {audit_path}")
    
    with pd.ExcelWriter(audit_path, engine='openpyxl') as writer:
        # Main audit sheets
        audit_dfs['bill_summary'].to_excel(writer, sheet_name='Bill_Summary', index=False)
        audit_dfs['line_items'].to_excel(writer, sheet_name='Line_Items', index=False)
        audit_dfs['providers'].to_excel(writer, sheet_name='Providers', index=False)
        audit_dfs['orders'].to_excel(writer, sheet_name='Orders', index=False)
        audit_dfs['processing'].to_excel(writer, sheet_name='Processing_Log', index=False)
        
        # Summary sheet
        summary_data = {
            'metric': [
                'Audit Timestamp',
                'Total Bills Processed',
                'Total Line Items',
                'Total Amount',
                'Batch Excel File',
                'Processing Status'
            ],
            'value': [
                datetime.now().isoformat(),
                len(audit_dfs['bill_summary']),
                len(audit_dfs['line_items']),
                f"${audit_dfs['line_items']['allowed_amount'].sum():.2f}",
                str(batch_excel_path) if batch_excel_path else 'Not Generated',
                'AUDIT_COMPLETE'
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
    
    print(f"   ✅ Audit log saved with {len(audit_dfs)} sheets")
    return audit_path

# Main Cell 6 Execution
print("STEP 6: AUDIT LOG GENERATION (AUDIT ONLY)")
print("=" * 60)

if 'excel_ready_bills' in locals() and excel_ready_bills:
    
    # Get batch info from Cell 5
    batch_path = batch_excel_path if 'batch_excel_path' in locals() else None
    batch_summary = summary if 'summary' in locals() else None
    
    # Create comprehensive audit log
    audit_dfs = create_comprehensive_audit_log(
        excel_ready_bills, 
        batch_path, 
        batch_summary
    )
    
    # Save audit log to Excel
    if 'output_dir' in locals():
        audit_excel_path = save_audit_log_excel(audit_dfs, output_dir, batch_path)
    else:
        # Create output directory if not available
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        temp_output_dir = Path("batch_outputs") / f"batch_{timestamp}"
        temp_output_dir.mkdir(parents=True, exist_ok=True)
        audit_excel_path = save_audit_log_excel(audit_dfs, temp_output_dir, batch_path)
    
    print(f"\n📊 AUDIT LOG SUMMARY:")
    print(f"   Audit file: {audit_excel_path}")
    print(f"   Bills tracked: {len(audit_dfs['bill_summary'])}")
    print(f"   Line items tracked: {len(audit_dfs['line_items'])}")
    print(f"   Providers tracked: {len(audit_dfs['providers'])}")
    
    print(f"\n✅ CELL 6 COMPLETE!")
    print(f"   📄 Main Excel: {batch_path if batch_path else 'Not available'}")
    print(f"   📋 Audit Log: {audit_excel_path}")
    print(f"   🔄 Database updates moved to Cell 6B")

else:
    print("❌ No excel_ready_bills available from previous steps")
    print("   Please run Cells 1-5 first")

print(f"\n🎯 Next Steps:")
if 'audit_excel_path' in locals():
    print(f"   1. Review audit log: {audit_excel_path}")
else:
    print(f"   1. Audit log not generated")
print(f"   2. Verify Excel batch file")  
print(f"   3. Run Cell 6B to update database (when ready)")

# %% [markdown]
# -- Cell 4: validate for Excel Gen

# %% [markdown]
# --- detailed bill info

# %%
# Safe way to access validation results in your notebook

from billing.logic.postprocess.utils.data_validation import (
    get_approved_unpaid_bills, 
    validate_bill_data, 
    print_validation_summary
)

# Get bills ready for postprocessing
bills = get_approved_unpaid_bills(limit=10)
print(f"Found {len(bills)} bills")

if bills:
    # Validate and get matching analysis
    validation_report = validate_bill_data(bills)
    
    # Print detailed summary
    print_validation_summary(validation_report)
    
    # Safe access to the data
    valid_bills = validation_report.get('valid_bills', [])
    print(f"\nProcessing {len(valid_bills)} valid bills:")
    
    for i, bill_result in enumerate(valid_bills):
        print(f"\n--- Bill {i+1} ---")
        
        # Safe access to all fields
        bill_id = bill_result.get('bill_id', 'Unknown')
        is_valid = bill_result.get('is_valid', False)
        bill_data = bill_result.get('bill_data', {})
        bill_line_items = bill_result.get('line_items', [])
        order_line_items = bill_result.get('order_line_items', [])
        matching_analysis = bill_result.get('matching_result', {})
        
        print(f"Bill ID: {bill_id}")
        print(f"Valid: {is_valid}")
        print(f"Patient: {bill_data.get('PatientName', 'Unknown')}")
        print(f"Bill Line Items: {len(bill_line_items)}")
        print(f"Order Line Items: {len(order_line_items)}")
        
        # Check if matching analysis exists
        if matching_analysis:
            match_summary = matching_analysis.get('match_summary', {})
            print(f"Exact Matches: {match_summary.get('exact_matches', 0)}")
            print(f"Partial Matches: {match_summary.get('partial_matches', 0)}")
            print(f"Unmatched: {match_summary.get('unmatched_bill', 0)} bill, {match_summary.get('unmatched_order', 0)} order")
        else:
            print("No matching analysis available")
        
        # Show sample line items
        if bill_line_items:
            print("Sample Bill Line Items:")
            for item in bill_line_items[:2]:  # Show first 2
                print(f"  CPT: {item.get('cpt_code')} | DOS: {item.get('date_of_service')} | Amount: ${item.get('allowed_amount', 0):.2f}")
        
        if order_line_items:
            print("Sample Order Line Items:")
            for item in order_line_items[:2]:  # Show first 2
                charge = item.get('charge_amount', '0')
                try:
                    charge_float = float(charge) if charge else 0.0
                except (ValueError, TypeError):
                    charge_float = 0.0
                print(f"  CPT: {item.get('cpt_code')} | DOS: {item.get('date_of_service')} | Charge: ${charge_float:.2f}")

else:
    print("No bills found for validation")

# Debug: Check if there are any issues with the validation process
if 'validation_report' in locals():
    invalid_bills = validation_report.get('invalid_bills', [])
    if invalid_bills:
        print(f"\n{len(invalid_bills)} invalid bills found:")
        for invalid_bill in invalid_bills[:3]:  # Show first 3
            bill_id = invalid_bill.get('bill_id', 'Unknown')
            issues = invalid_bill.get('bill_validation', {}).get('missing_fields', [])
            print(f"  Bill {bill_id}: {', '.join(issues[:3]) if issues else 'Unknown issues'}")

# %% [markdown]
# -- check data format for eobr gen

# %%
# %%
# Helper Cell: Data Structure Analysis for EOBR Mapping
# =============================================================================
# Place this ABOVE Cell 7 to confirm your data field names and structure

print("🔍 DATA STRUCTURE ANALYSIS FOR EOBR MAPPING")
print("=" * 60)

def analyze_data_structure(cleaned_bills):
    """
    Analyze the data structure of cleaned_bills to confirm field names
    and help with EOBR placeholder mapping.
    """
    if not cleaned_bills:
        print("❌ No cleaned_bills data available")
        return
    
    print(f"📊 ANALYZING {len(cleaned_bills)} BILLS")
    print("=" * 40)
    
    # 1. BILL-LEVEL FIELD ANALYSIS
    print("\n1️⃣ BILL-LEVEL FIELDS:")
    print("-" * 30)
    
    sample_bill = cleaned_bills[0]
    all_bill_keys = list(sample_bill.keys())
    
    print(f"Total bill fields: {len(all_bill_keys)}")
    print(f"Bill field names:")
    for i, key in enumerate(sorted(all_bill_keys), 1):
        value = sample_bill.get(key, '')
        # Truncate long values for display
        if isinstance(value, str) and len(str(value)) > 50:
            display_value = f"{str(value)[:47]}..."
        elif isinstance(value, list):
            display_value = f"[LIST with {len(value)} items]"
        elif isinstance(value, dict):
            display_value = f"[DICT with {len(value)} keys]"
        else:
            display_value = str(value)
        
        print(f"   {i:2d}. {key}: '{display_value}'")
    
    # 2. REQUIRED EOBR FIELDS CHECK
    print("\n2️⃣ EOBR REQUIRED FIELDS CHECK:")
    print("-" * 35)
    
    # Fields that EOBR generator expects
    eobr_required_fields = [
        'PatientName', 'Patient_DOB', 'Order_ID', 'FileMaker_Record_Number', 
        'Patient_Injury_Date', 'provider_tin', 'provider_npi', 'provider_billing_name',
        'provider_billing_address1', 'provider_billing_address2', 'provider_billing_city',
        'provider_billing_state', 'provider_billing_postal_code'
    ]
    
    print("Checking for EOBR generator required fields:")
    missing_fields = []
    
    for field in eobr_required_fields:
        if field in sample_bill:
            value = sample_bill[field]
            status = "✅ FOUND" if value else "⚠️ EMPTY"
            # Truncate value for display
            if isinstance(value, str) and len(value) > 30:
                display_val = f"{value[:27]}..."
            else:
                display_val = str(value)
            print(f"   {field}: {status} - '{display_val}'")
        else:
            missing_fields.append(field)
            print(f"   {field}: ❌ MISSING")
    
    if missing_fields:
        print(f"\n⚠️ MISSING FIELDS: {missing_fields}")
    else:
        print(f"\n✅ All required bill fields found!")
    
    # 3. LINE ITEMS ANALYSIS
    print("\n3️⃣ LINE ITEMS STRUCTURE:")
    print("-" * 25)
    
    line_items = sample_bill.get('line_items', [])
    
    if not line_items:
        print("❌ No line_items found in bill data")
        return
    
    print(f"Number of line items: {len(line_items)}")
    print(f"Line item field names:")
    
    sample_line_item = line_items[0]
    line_item_keys = list(sample_line_item.keys())
    
    for i, key in enumerate(sorted(line_item_keys), 1):
        value = sample_line_item.get(key, '')
        # Truncate long values
        if isinstance(value, str) and len(str(value)) > 40:
            display_value = f"{str(value)[:37]}..."
        else:
            display_value = str(value)
        
        print(f"   {i:2d}. {key}: '{display_value}'")
    
    # 4. LINE ITEM REQUIRED FIELDS CHECK
    print("\n4️⃣ LINE ITEM REQUIRED FIELDS CHECK:")
    print("-" * 38)
    
    eobr_line_item_fields = [
        'date_of_service', 'place_of_service', 'cpt_code', 'modifier',
        'units', 'charge_amount', 'allowed_amount'
    ]
    
    print("Checking for EOBR generator required line item fields:")
    missing_line_fields = []
    
    for field in eobr_line_item_fields:
        if field in sample_line_item:
            value = sample_line_item[field]
            status = "✅ FOUND" if value else "⚠️ EMPTY"
            print(f"   {field}: {status} - '{value}'")
        else:
            missing_line_fields.append(field)
            print(f"   {field}: ❌ MISSING")
    
    if missing_line_fields:
        print(f"\n⚠️ MISSING LINE ITEM FIELDS: {missing_line_fields}")
    else:
        print(f"\n✅ All required line item fields found!")
    
    # 5. SAMPLE DATA PREVIEW
    print("\n5️⃣ SAMPLE DATA PREVIEW:")
    print("-" * 25)
    
    print(f"Sample Bill #{sample_bill.get('id', 'Unknown')}:")
    print(f"   Patient: {sample_bill.get('PatientName', 'Unknown')}")
    print(f"   Provider: {sample_bill.get('provider_billing_name', 'Unknown')}")
    print(f"   FM Record: {sample_bill.get('FileMaker_Record_Number', 'Unknown')}")
    print(f"   Order ID: {sample_bill.get('Order_ID', 'Unknown')}")
    
    if line_items:
        print(f"\n   Line Items ({len(line_items)}):")
        for i, item in enumerate(line_items[:3], 1):  # Show first 3
            cpt = item.get('cpt_code', 'Unknown')
            dos = item.get('date_of_service', 'Unknown') 
            charge = item.get('charge_amount', 0)
            allowed = item.get('allowed_amount', 0)
            print(f"      {i}. CPT {cpt} on {dos}: Charge ${charge}, Allowed ${allowed}")
        
        if len(line_items) > 3:
            print(f"      ... and {len(line_items) - 3} more line items")
    
    # 6. FIELD MAPPING RECOMMENDATIONS
    print("\n6️⃣ FIELD MAPPING RECOMMENDATIONS:")
    print("-" * 38)
    
    print("Based on your data, here's the recommended mapping for Cell 7:")
    print("\nrequired_bill_fields = {")
    for field in eobr_required_fields:
        if field in sample_bill:
            # Map to expected EOBR placeholder names
            placeholder_map = {
                'PatientName': 'PatientName',
                'Patient_DOB': 'dob',
                'Order_ID': 'order_no', 
                'FileMaker_Record_Number': 'provider_ref',
                'Patient_Injury_Date': 'doi',
                'provider_tin': 'TIN',
                'provider_npi': 'NPI',
                'provider_billing_name': 'billing_name',
                'provider_billing_address1': 'billing_address1',
                'provider_billing_address2': 'billing_address2',
                'provider_billing_city': 'billing_city',
                'provider_billing_state': 'billing_state',
                'provider_billing_postal_code': 'billing_zip'
            }
            placeholder = placeholder_map.get(field, field.lower())
            print(f"    '{field}': '{placeholder}',")
    print("}")
    
    print("\nrequired_line_item_fields = {")
    for field in eobr_line_item_fields:
        if field in sample_line_item:
            placeholder_map = {
                'date_of_service': 'dos1-6',
                'place_of_service': 'pos1-6',
                'cpt_code': 'cpt1-6',
                'modifier': 'modifier1-6', 
                'units': 'units1-6',
                'charge_amount': 'charge1-6',
                'allowed_amount': 'alwd1-6'
            }
            placeholder = placeholder_map.get(field, f"{field}1-6")
            print(f"    '{field}': '{placeholder}',")
    print("}")
    
    # 7. SUMMARY
    print("\n7️⃣ SUMMARY:")
    print("-" * 12)
    
    total_missing = len(missing_fields) + len(missing_line_fields)
    
    if total_missing == 0:
        print("✅ Your data structure is ready for EOBR generation!")
        print("✅ All required fields are present")
        print("🚀 You can proceed with Cell 7")
    else:
        print(f"⚠️ {total_missing} required fields are missing")
        print("🔧 You may need to update your data cleaning (Cell 3)")
        print("🔧 Or check your field names in the database query")
    
    print(f"\n📊 Data Structure Analysis Complete!")

# Execute the analysis
if 'cleaned_bills' in locals() and cleaned_bills:
    analyze_data_structure(cleaned_bills)
else:
    print("❌ Variable 'cleaned_bills' not found")
    print("💡 Please run your data cleaning cells (1-6) first")
    print("💡 Make sure the variable is named 'cleaned_bills'")
    
    # Check for alternative variable names
    possible_names = ['bills', 'valid_bills', 'eobr_bills', 'processed_bills']
    found_alternatives = []
    
    for name in possible_names:
        if name in locals():
            found_alternatives.append(name)
    
    if found_alternatives:
        print(f"\n🔍 Found these similar variables: {found_alternatives}")
        print("💡 You can run: analyze_data_structure(your_variable_name)")

# %%
# Import Path Diagnostic - Run this first to understand your project structure
# =============================================================================

import os
import sys
from pathlib import Path

print("🔍 PROJECT STRUCTURE DIAGNOSTIC")
print("=" * 60)

# 1. Show current working directory
current_dir = Path.cwd()
print(f"📁 Current working directory: {current_dir}")

# 2. Show Python path
print(f"\n🐍 Python path (sys.path):")
for i, path in enumerate(sys.path):
    print(f"   {i+1:2d}. {path}")

# 3. Look for project structure
print(f"\n📂 PROJECT STRUCTURE SCAN:")
print("Looking for billing-related directories and files...")

def scan_directory(path, max_depth=3, current_depth=0):
    """Recursively scan directory for relevant files"""
    items = []
    if current_depth >= max_depth:
        return items
    
    try:
        for item in sorted(path.iterdir()):
            if item.is_dir():
                # Look for billing, utils, or similar directories
                if any(keyword in item.name.lower() for keyword in ['billing', 'utils', 'logic', 'postprocess']):
                    items.append(('DIR', item, current_depth))
                    # Recursively scan important directories
                    items.extend(scan_directory(item, max_depth, current_depth + 1))
            elif item.suffix == '.py':
                # Look for Python files with relevant names
                if any(keyword in item.name.lower() for keyword in ['db_utils', 'database', 'db', 'utils']):
                    items.append(('FILE', item, current_depth))
    except PermissionError:
        pass
    
    return items

# Scan current directory and parents
scan_paths = [
    current_dir,
    current_dir.parent,
    current_dir.parent.parent
]

relevant_items = []
for scan_path in scan_paths:
    if scan_path.exists():
        print(f"\n📁 Scanning: {scan_path}")
        items = scan_directory(scan_path, max_depth=4)
        relevant_items.extend(items)

# Show results
if relevant_items:
    print(f"\n🎯 RELEVANT FILES AND DIRECTORIES FOUND:")
    for item_type, path, depth in relevant_items:
        indent = "  " * depth
        icon = "📁" if item_type == "DIR" else "📄"
        print(f"   {indent}{icon} {path.relative_to(current_dir) if path.is_relative_to(current_dir) else path}")
else:
    print(f"\n❌ No billing/utils directories found")

# 4. Check for specific files we need
print(f"\n🔍 SEARCHING FOR SPECIFIC FILES:")
search_patterns = [
    "**/db_utils.py",
    "**/database*.py", 
    "**/utils.py",
    "billing/**/*.py",
    "utils/**/*.py"
]

found_files = []
for pattern in search_patterns:
    matches = list(current_dir.glob(pattern))
    if matches:
        print(f"   Pattern '{pattern}':")
        for match in matches[:10]:  # Limit to first 10 matches
            print(f"      📄 {match}")
            found_files.append(match)
        if len(matches) > 10:
            print(f"      ... and {len(matches) - 10} more files")

# 5. Check what's in the immediate directory
print(f"\n📂 CURRENT DIRECTORY CONTENTS:")
try:
    items = list(current_dir.iterdir())
    dirs = [item for item in items if item.is_dir()]
    files = [item for item in items if item.is_file() and item.suffix == '.py']
    
    if dirs:
        print(f"   📁 Directories:")
        for d in sorted(dirs)[:15]:  # Show first 15
            print(f"      {d.name}")
        if len(dirs) > 15:
            print(f"      ... and {len(dirs) - 15} more directories")
    
    if files:
        print(f"   📄 Python files:")
        for f in sorted(files)[:15]:  # Show first 15
            print(f"      {f.name}")
        if len(files) > 15:
            print(f"      ... and {len(files) - 15} more files")

except Exception as e:
    print(f"   ❌ Error scanning directory: {e}")

# 6. Try to determine project root
print(f"\n🎯 PROJECT ROOT DETECTION:")
possible_roots = []

# Look for common project indicators
root_indicators = [
    '.git',
    'requirements.txt',
    'setup.py',
    'pyproject.toml',
    'manage.py',  # Django
    'app.py',     # Flask
    'main.py'
]

current_check = current_dir
for _ in range(5):  # Check up to 5 levels up
    for indicator in root_indicators:
        if (current_check / indicator).exists():
            possible_roots.append((current_check, indicator))
            break
    current_check = current_check.parent
    if current_check == current_check.parent:  # Reached filesystem root
        break

if possible_roots:
    print(f"   Possible project roots:")
    for root, indicator in possible_roots:
        print(f"      📁 {root} (found {indicator})")
else:
    print(f"   ❌ No clear project root indicators found")

# 7. RECOMMENDATIONS
print(f"\n💡 IMPORT PATH RECOMMENDATIONS:")

# Look for the most likely location of db_utils.py
db_utils_candidates = [f for f in found_files if 'db_utils' in f.name]

if db_utils_candidates:
    print(f"   Found db_utils.py files:")
    for candidate in db_utils_candidates:
        rel_path = candidate.relative_to(current_dir) if candidate.is_relative_to(current_dir) else candidate
        
        # Generate possible import statements
        parts = rel_path.with_suffix('').parts
        import_path = '.'.join(parts)
        
        print(f"      📄 {rel_path}")
        print(f"         Try: from {import_path} import get_db_connection")
        
        # Check if file contains get_db_connection
        try:
            with open(candidate, 'r') as f:
                content = f.read()
            if 'def get_db_connection' in content:
                print(f"         ✅ Contains get_db_connection function")
            else:
                print(f"         ❌ No get_db_connection function found")
        except:
            print(f"         ⚠️  Could not read file")
else:
    print(f"   ❌ No db_utils.py files found")
    print(f"   💡 You may need to:")
    print(f"      1. Create db_utils.py in utils/ directory")
    print(f"      2. Implement get_db_connection() function")
    print(f"      3. Or use a different database connection method")

print(f"\n" + "=" * 60)
print(f"🎯 NEXT STEPS:")
print(f"1. Run this diagnostic cell first")
print(f"2. Copy the working import statement from above")
print(f"3. Update Cell 6B with the correct import path")
print(f"4. If no db_utils.py exists, we'll create one")

# %% [markdown]
# -- Database Updates to Mark Bills Paid

# %%
# Cell 6B: Database Updates (SEPARATE FROM AUDIT LOG)
# =============================================================================

from datetime import datetime
import sys
from pathlib import Path

# Cell 6B: Database Updates (DIRECT SQLITE CONNECTION)
# =============================================================================

from datetime import datetime
import sqlite3
from pathlib import Path
import pytz  # For timezone handling

def get_current_est_timestamp():
    """Get current timestamp in EST timezone in YYYY-MM-DD HH:MM:SS format."""
    try:
        # Get EST timezone
        est = pytz.timezone('America/New_York')  # This handles EST/EDT automatically
        current_time = datetime.now(est)
        return current_time.strftime('%Y-%m-%d %H:%M:%S')
    except ImportError:
        # Fallback if pytz not available - use local time
        print("   ⚠️  pytz not available, using local system time")
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def get_db_connection():
    """Connect directly to your monolith.db SQLite database."""
    # Your specific database path
    db_path = r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\monolith\monolith.db"
    
    try:
        print(f"🔗 Connecting to: {db_path}")
        
        if not Path(db_path).exists():
            print(f"❌ Database file not found at: {db_path}")
            return None
        
        conn = sqlite3.connect(db_path)
        
        # Test the connection
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        
        print(f"✅ Successfully connected to monolith.db")
        return conn
        
    except Exception as e:
        print(f"❌ Database connection failed: {str(e)}")
        return None
def update_database_bill_status(excel_ready_bills, mark_as_paid=True):
    """
    Update ProviderBill table to mark bills as paid.
    
    Args:
        excel_ready_bills: List of processed bills
        mark_as_paid: Whether to actually mark bills as paid
        
    Returns:
        Update report with success/failure details
    """
    print(f"🔄 DATABASE UPDATE")
    print(f"   Mark as paid: {mark_as_paid}")
    
    if not mark_as_paid:
        print(f"   ⚠️  Database updates disabled - bills not marked as paid")
        print(f"   📋 To enable: set mark_as_paid=True")
        return {
            'updated': False,
            'bill_count': len(excel_ready_bills),
            'message': 'Database updates disabled'
        }
    
    # Get database connection
    conn = get_db_connection()
    if conn is None:
        return {
            'updated': False,
            'bill_count': 0,
            'message': 'Database connection failed'
        }
    # Get bill IDs to update
    bill_ids = [bill.get('id') for bill in excel_ready_bills]
    
    print(f"   🚀 Updating {len(bill_ids)} bills in ProviderBill table...")
    
    cursor = conn.cursor()
    
    updated_count = 0
    failed_updates = []
    
    try:
        print(f"   📋 Processing bill updates:")
        
        # Show timezone information
        sample_timestamp = get_current_est_timestamp()
        print(f"   🕒 Using EST timezone timestamp format: {sample_timestamp}")
        
        for bill_id in bill_ids:
            try:
                # Get current timestamp in EST
                current_timestamp = get_current_est_timestamp()
                
                # Update the ProviderBill table with both bill_paid and updated_at
                cursor.execute("""
                    UPDATE ProviderBill 
                    SET bill_paid = 'Y',
                        updated_at = ?
                    WHERE id = ?
                """, (current_timestamp, bill_id))
                
                # Check if the update actually affected a row
                if cursor.rowcount > 0:
                    updated_count += 1
                    if updated_count <= 10:  # Only show first 10 for brevity
                        print(f"      ✅ {bill_id}: bill_paid = 'Y', updated_at = '{current_timestamp}' (EST)")
                    elif updated_count == 11:
                        print(f"      ... (showing first 10, continuing silently)")
                else:
                    failed_updates.append({
                        'bill_id': bill_id,
                        'error': 'Bill ID not found in database'
                    })
                    print(f"      ❌ {bill_id}: Bill not found")
                
            except Exception as e:
                failed_updates.append({
                    'bill_id': bill_id,
                    'error': str(e)
                })
                print(f"      ❌ {bill_id}: {str(e)}")
        
        # Commit all changes
        conn.commit()
        print(f"   ✅ Database commit successful")
        
        # Verify updates
        print(f"   🔍 Verifying updates...")
        placeholders = ','.join(['?' for _ in bill_ids])
        verification_sql = f"""
            SELECT id, bill_paid, updated_at 
            FROM ProviderBill 
            WHERE id IN ({placeholders})
        """
        cursor.execute(verification_sql, bill_ids)
        verification_results = cursor.fetchall()
        
        verified_paid = 0
        sample_timestamps = []
        for row in verification_results:
            if row[1] == 'Y':  # bill_paid column
                verified_paid += 1
                if len(sample_timestamps) < 3:  # Collect first 3 timestamps as samples
                    sample_timestamps.append(row[2])  # updated_at column
        
        print(f"   ✅ Verification: {verified_paid}/{len(bill_ids)} bills marked as paid")
        
        if sample_timestamps:
            print(f"   📅 Sample updated_at timestamps (EST):")
            for i, timestamp in enumerate(sample_timestamps, 1):
                print(f"      {i}. {timestamp}")
        
        # Show the exact timestamp format used
        final_sample = get_current_est_timestamp()
        print(f"   🕒 Final timestamp format: {final_sample} (EST)")
        
    except Exception as e:
        conn.rollback()
        print(f"   ❌ Database error: {str(e)}")
        print(f"   🔄 All changes rolled back")
        return {
            'updated': False,
            'bill_count': 0,
            'updated_count': 0,
            'failed_count': len(bill_ids),
            'message': f'Database update failed: {str(e)}',
            'failed_updates': [{'bill_id': bid, 'error': str(e)} for bid in bill_ids]
        }
    
    finally:
        conn.close()
    
    # Return comprehensive update report
    return {
        'updated': True,
        'bill_count': len(bill_ids),
        'updated_count': updated_count,
        'failed_count': len(failed_updates),
        'bill_ids_updated': [bid for bid in bill_ids if bid not in [f['bill_id'] for f in failed_updates]],
        'failed_updates': failed_updates,
        'message': f'Successfully updated {updated_count}/{len(bill_ids)} bills'
    }

# Main Cell 6B Execution
print("STEP 6B: DATABASE UPDATES")
print("=" * 60)

if 'excel_ready_bills' in locals() and excel_ready_bills:
    
    print(f"📊 READY TO UPDATE DATABASE:")
    print(f"   Bills to mark as paid: {len(excel_ready_bills)}")
    
    # Show sample bills that will be updated
    print(f"\n📋 Sample bills to update:")
    for i, bill in enumerate(excel_ready_bills[:5]):
        bill_id = bill.get('id')
        patient_name = bill.get('PatientName', 'Unknown')
        fm_record = bill.get('FileMaker_Record_Number', 'Unknown')
        line_items = bill.get('line_items', [])
        total_amount = sum(float(item.get('allowed_amount', 0)) for item in line_items)
        
        print(f"   {i+1}. Bill {bill_id}: {patient_name} (FM: {fm_record}) - ${total_amount:.2f}")
    
    if len(excel_ready_bills) > 5:
        print(f"   ... and {len(excel_ready_bills) - 5} more bills")
    
    # Calculate total amount
    total_amount = sum(
        sum(float(item.get('allowed_amount', 0)) for item in bill.get('line_items', []))
        for bill in excel_ready_bills
    )
    print(f"\n💰 Total amount to be marked as paid: ${total_amount:.2f}")
    
    # ⚠️ SAFETY CHECK - Set to False initially for safety
    PERFORM_DB_UPDATE = True  # Change to True when ready to update database
    
    if not PERFORM_DB_UPDATE:
        print(f"\n⚠️  DATABASE UPDATE DISABLED FOR SAFETY")
        print(f"   To enable database updates:")
        print(f"   1. Review the bills above")
        print(f"   2. Set PERFORM_DB_UPDATE = True")
        print(f"   3. Re-run this cell")
        
        # Still show what would happen
        update_report = update_database_bill_status(
            excel_ready_bills, 
            mark_as_paid=False
        )
    else:
        print(f"\n🚀 PERFORMING DATABASE UPDATES...")
        print(f"   This will mark {len(excel_ready_bills)} bills as paid")
        
        # Perform actual database update
        update_report = update_database_bill_status(
            excel_ready_bills, 
            mark_as_paid=True
        )
    
    # Display results
    print(f"\n🔄 DATABASE UPDATE RESULTS:")
    print(f"   Bills processed: {update_report['bill_count']}")
    
    if update_report['updated']:
        print(f"   ✅ Successfully updated: {update_report['updated_count']}")
        print(f"   ❌ Failed updates: {update_report['failed_count']}")
        
        if update_report['failed_count'] > 0:
            print(f"\n   Failed bills:")
            for failure in update_report['failed_updates'][:3]:  # Show first 3
                print(f"      {failure['bill_id']}: {failure['error']}")
            
            if len(update_report['failed_updates']) > 3:
                print(f"      ... and {len(update_report['failed_updates']) - 3} more failures")
        
        if update_report['updated_count'] > 0:
            print(f"\n   ✅ SUCCESSFULLY MARKED AS PAID:")
            print(f"      {update_report['updated_count']} bills")
            print(f"      ${total_amount:.2f} total amount")
            print(f"      Status change: bill_paid = 'N' → 'Y'")
    else:
        print(f"   Status: {update_report['message']}")
    
    # Summary for next steps
    print(f"\n✅ CELL 6B COMPLETE!")
    
    if update_report['updated'] and update_report['updated_count'] > 0:
        print(f"   🎉 Database successfully updated!")
        print(f"   📊 {update_report['updated_count']} bills marked as paid")
        print(f"   💰 ${total_amount:.2f} processed")
        
        # Show what was changed in the database
        print(f"\n📝 DATABASE CHANGES MADE:")
        print(f"   Table: ProviderBill")
        print(f"   Field: bill_paid")
        print(f"   Change: 'N' → 'Y'")
        print(f"   Records affected: {update_report['updated_count']}")
        print(f"   Timestamp: {datetime.now().isoformat()}")
        
    elif not PERFORM_DB_UPDATE:
        print(f"   ⚠️  No database changes made (safety mode)")
        print(f"   💡 Set PERFORM_DB_UPDATE = True when ready")
    else:
        print(f"   ❌ Database update failed")
        print(f"   📋 Check error messages above")

else:
    print("❌ No excel_ready_bills available from previous steps")
    print("   Please run Cells 1-6 first")

print(f"\n🎯 Next Steps:")
if 'update_report' in locals() and update_report.get('updated') and update_report.get('updated_count', 0) > 0:
    print(f"   1. ✅ Database updated - bills marked as paid")
    print(f"   2. ✅ Ready for final processing steps")
    print(f"   3. ✅ Excel and EOBR generation can proceed")
elif 'PERFORM_DB_UPDATE' in locals() and not PERFORM_DB_UPDATE:
    print(f"   1. Review bills to be updated above")
    print(f"   2. Set PERFORM_DB_UPDATE = True when ready")
    print(f"   3. Re-run this cell to update database")
else:
    print(f"   1. Check database connection and utilities")
    print(f"   2. Ensure bills are available from previous steps")
    print(f"   3. Retry database update")

# %% [markdown]
# -- Cell 7: EOBR Data Mapping and Validation

# %%
# %%
# Cell 7: EOBR Data Mapping and Validation (FIXED TO MATCH ACTUAL GENERATOR)
# =============================================================================

print("STEP 7: EOBR DATA MAPPING AND VALIDATION")
print("=" * 60)

def validate_and_fix_pos_code(pos_code, line_item_number, bill_id):
    """
    Validate POS code and restrict to only '11' (Office).
    Organization policy: Only office visits allowed.
    
    Args:
        pos_code: The place of service code to validate
        line_item_number: Line item number for logging
        bill_id: Bill ID for logging
        
    Returns:
        Tuple of (validated_pos_code, warning_message)
    """
    # Convert to string and clean
    if pos_code is None or pos_code == '':
        return '11', f"Empty POS code defaulted to '11' (Office)"
    
    pos_str = str(pos_code).strip()
    
    # Pad single digits with leading zero if needed
    if pos_str.isdigit() and len(pos_str) == 1:
        pos_str = '0' + pos_str
    
    # POLICY: Only allow '11' (Office) - reject all others
    if pos_str == '11':
        return '11', None  # Valid, no warning
    else:
        return '11', f"POS '{pos_code}' → '11' (POLICY: Office visits only)"

def validate_eobr_data_mapping_fixed(cleaned_bills):
    """
    Validate and map the cleaned bills data to EOBR template requirements.
    FIXED to match the exact mapping in eobr_generator.py prepare_bill_data() method.
    
    Args:
        cleaned_bills: List of cleaned bill dictionaries from previous steps
        
    Returns:
        Tuple of (eobr_ready_bills, mapping_report)
    """
    print("🔍 VALIDATING DATA MAPPING FOR EOBR GENERATION (FIXED TO MATCH GENERATOR)")
    print("🏥 POS CODE POLICY: Only '11' (Office) allowed - all others will be converted")
    print("=" * 50)
    
    eobr_ready_bills = []
    mapping_issues = []
    pos_conversions = []  # Track POS code conversions for reporting
    
    # EXACT mapping from eobr_generator.py prepare_bill_data() method:
    required_bill_fields = {
        # Header section - EXACT field names from generator
        'PatientName': 'PatientName',  # str(bill.get('PatientName', ''))
        'Patient_DOB': 'dob',          # self.format_date(bill.get('Patient_DOB'))
        'Order_ID': 'order_no',        # str(bill.get('Order_ID', ''))
        'FileMaker_Record_Number': 'provider_ref',  # str(bill.get('FileMaker_Record_Number', ''))
        'Patient_Injury_Date': 'doi',  # self.format_date(bill.get('Patient_Injury_Date'))
        
        # Provider section - EXACT field names from generator
        'provider_tin': 'TIN',         # str(bill.get('provider_tin', ''))
        'provider_npi': 'NPI',         # str(bill.get('provider_npi', ''))
        'provider_billing_name': 'billing_name',     # str(bill.get('provider_billing_name', ''))
        'provider_billing_address1': 'billing_address1',  # str(bill.get('provider_billing_address1', ''))
        'provider_billing_address2': 'billing_address2',  # str(bill.get('provider_billing_address2', ''))
        'provider_billing_city': 'billing_city',     # str(bill.get('provider_billing_city', ''))
        'provider_billing_state': 'billing_state',   # str(bill.get('provider_billing_state', ''))
        'provider_billing_postal_code': 'billing_zip'  # str(bill.get('provider_billing_postal_code', ''))
    }
    
    # Line item fields - EXACT field names from generator
    required_line_item_fields = {
        'date_of_service': 'dos1-6',    # self.format_date(item.get('date_of_service'))
        'place_of_service': 'pos1-6',   # str(item.get('place_of_service', '11'))
        'cpt_code': 'cpt1-6',          # str(item.get('cpt_code', ''))
        'modifier': 'modifier1-6',      # modifier handling logic
        'units': 'units1-6',           # str(item.get('units', 1))
        'charge_amount': 'charge1-6',   # self.format_currency(charge_amount)
        'allowed_amount': 'alwd1-6',    # self.format_currency(allowed_amount)
        # 'paid1-6' and 'code1-6' are calculated by the generator
    }
    
    for i, bill in enumerate(cleaned_bills):
        bill_id = bill.get('id')
        patient_name = bill.get('PatientName', '')
        
        print(f"\n--- Bill {i+1}: {patient_name} (ID: {bill_id}) ---")
        
        bill_issues = []
        
        # 1. VALIDATE EXACT BILL FIELDS FROM GENERATOR
        print("   📋 Bill Fields (matching eobr_generator.py):")
        
        for source_field, placeholder in required_bill_fields.items():
            value = bill.get(source_field, '')
            
            # Special handling for optional fields
            if source_field in ['Patient_Injury_Date', 'provider_npi', 'provider_billing_address2']:
                status = "✅" if value else "⚪"  # Optional
                if not value:
                    print(f"      {source_field} → <{placeholder}>: EMPTY (optional) {status}")
                else:
                    print(f"      {source_field} → <{placeholder}>: '{value}' {status}")
            else:
                status = "✅" if value else "❌"
                print(f"      {source_field} → <{placeholder}>: '{value}' {status}")
                if not value:
                    bill_issues.append(f"Missing {source_field}")
        
        # 2. VALIDATE LINE ITEMS - EXACT STRUCTURE FROM GENERATOR
        line_items = bill.get('line_items', [])
        print(f"   💊 Line Items ({len(line_items)} items) - Generator expects up to 6:")
        
        if not line_items:
            bill_issues.append("No line items found")
            print("      ❌ No line items found")
        elif len(line_items) > 6:
            print(f"      ⚠️  WARNING: {len(line_items)} line items, but EOBR template only supports 6")
            print(f"         Only first 6 will be included in EOBR")
        
        if line_items:
            valid_line_items = 0
            total_allowed = 0.0
            
            for j, item in enumerate(line_items[:6]):  # Generator only processes first 6
                item_issues = []
                print(f"      Line Item {j+1} (will be slot {j+1} in EOBR):")
                
                for source_field, placeholder_pattern in required_line_item_fields.items():
                    value = item.get(source_field, '')
                    placeholder = placeholder_pattern.replace('1-6', str(j+1))
                    
                    # Special handling for optional/calculated fields
                    if source_field == 'modifier':
                        status = "⚪" if not value else "✅"  # Optional
                        print(f"         {source_field} → <{placeholder}>: '{value}' {status}")
                    elif source_field == 'place_of_service':
                        # NEW POS VALIDATION WITH POLICY ENFORCEMENT
                        validated_pos, warning = validate_and_fix_pos_code(value, j+1, bill_id)
                        
                        if warning:
                            print(f"         {source_field} → <{placeholder}>: '{validated_pos}' 🔧")
                            print(f"           WARNING: {warning}")
                            # Track conversion for reporting
                            pos_conversions.append({
                                'bill_id': bill_id,
                                'patient_name': patient_name,
                                'line_item': j+1,
                                'original_pos': value,
                                'converted_pos': validated_pos,
                                'reason': warning
                            })
                            # Update the item with the corrected value
                            item[source_field] = validated_pos
                        else:
                            print(f"         {source_field} → <{placeholder}>: '{validated_pos}' ✅")
                        
                        value = validated_pos
                    else:
                        status = "✅" if value else "❌"
                        print(f"         {source_field} → <{placeholder}>: '{value}' {status}")
                        if not value:
                            item_issues.append(f"Missing {source_field}")
                
                # Validate numeric fields (like generator does)
                try:
                    charge_amount = float(item.get('charge_amount', 0)) if item.get('charge_amount') is not None else 0.0
                    allowed_amount = float(item.get('allowed_amount', 0)) if item.get('allowed_amount') is not None else 0.0
                    units = int(item.get('units', 1))
                    
                    total_allowed += allowed_amount
                    
                    # Generator calculates paid amount and reason code
                    if allowed_amount == 0:
                        reason_code = "125"  # Denied
                        paid_amount = 0.0
                    elif allowed_amount < charge_amount:
                        reason_code = "85"   # Reduced payment
                        paid_amount = allowed_amount
                    else:
                        reason_code = "85"   # Full payment
                        paid_amount = allowed_amount
                    
                    print(f"         CALCULATED: paid{j+1} = ${paid_amount:.2f}, code{j+1} = {reason_code}")
                    
                    if charge_amount < 0: item_issues.append("Negative charge amount")
                    if allowed_amount < 0: item_issues.append("Negative allowed amount")
                    if units <= 0: item_issues.append("Invalid units")
                        
                except (ValueError, TypeError) as e:
                    item_issues.append(f"Invalid numeric values: {str(e)}")
                
                if not item_issues:
                    valid_line_items += 1
                    print(f"         ✅ Line item valid")
                else:
                    print(f"         ❌ Issues: {', '.join(item_issues)}")
                    bill_issues.extend([f"Line {j+1}: {issue}" for issue in item_issues])
            
            print(f"      📊 Valid line items: {valid_line_items}/{min(len(line_items), 6)}")
            print(f"      💰 Total allowed (for <total_paid>): ${total_allowed:.2f}")
        
        # 3. SHOW FINAL EOBR MAPPING PREVIEW
        print(f"   🎯 FINAL EOBR PLACEHOLDER MAPPING:")
        
        # Process date (current date)
        from datetime import datetime
        process_date = datetime.now().strftime('%m/%d/%Y')
        print(f"      CALCULATED: <process_date> = {process_date}")
        
        # Calculate total paid
        if line_items:
            total_paid = sum(float(item.get('allowed_amount', 0)) for item in line_items if item.get('allowed_amount') is not None)
            print(f"      CALCULATED: <total_paid> = ${total_paid:.2f}")
        
        # 4. DETERMINE EOBR READINESS
        if not bill_issues:
            eobr_ready_bills.append(bill)
            print(f"   ✅ EOBR READY - Will generate successfully")
        else:
            mapping_issues.append({
                'bill_id': bill_id,
                'patient_name': patient_name,
                'issues': bill_issues
            })
            print(f"   ❌ NOT READY - {len(bill_issues)} issues must be fixed")
    
    # 6. SUMMARY REPORT
    mapping_report = {
        'total_bills': len(cleaned_bills),
        'eobr_ready_count': len(eobr_ready_bills),
        'issues_count': len(mapping_issues),
        'mapping_issues': mapping_issues,
        'pos_conversions': pos_conversions,  # NEW: Track POS conversions
        'validation_timestamp': datetime.now().isoformat()
    }
    
    print(f"\n📊 EOBR MAPPING SUMMARY:")
    print(f"   Total bills: {mapping_report['total_bills']}")
    print(f"   EOBR ready: {mapping_report['eobr_ready_count']}")
    print(f"   With issues: {mapping_report['issues_count']}")
    print(f"   POS conversions: {len(pos_conversions)}")
    
    # Show POS conversion summary
    if pos_conversions:
        print(f"\n🏥 POS CODE CONVERSIONS APPLIED:")
        print(f"   Policy: Only POS '11' (Office) allowed")
        
        # Group by original POS code
        from collections import defaultdict
        pos_groups = defaultdict(int)
        for conversion in pos_conversions:
            orig_pos = conversion['original_pos'] if conversion['original_pos'] else 'EMPTY'
            pos_groups[orig_pos] += 1
        
        for orig_pos, count in pos_groups.items():
            print(f"   '{orig_pos}' → '11': {count} line items")
        
        # Show sample conversions
        print(f"\n   📋 Sample conversions:")
        for conversion in pos_conversions[:5]:  # Show first 5
            bill_short = conversion['bill_id'][:10] + "..." if len(conversion['bill_id']) > 10 else conversion['bill_id']
            print(f"   {bill_short} Line {conversion['line_item']}: '{conversion['original_pos']}' → '11'")
        
        if len(pos_conversions) > 5:
            print(f"   ... and {len(pos_conversions) - 5} more conversions")
    
    if mapping_issues:
        print(f"\n❌ ISSUES TO RESOLVE:")
        for issue_bill in mapping_issues:
            print(f"   {issue_bill['bill_id']} ({issue_bill['patient_name']}):")
            for issue in issue_bill['issues'][:3]:  # Show first 3 issues per bill
                print(f"      - {issue}")
            if len(issue_bill['issues']) > 3:
                print(f"      - ... and {len(issue_bill['issues']) - 3} more issues")
    
    return eobr_ready_bills, mapping_report

# Execute the validation with FIXED mapping
if 'cleaned_bills' in locals() and cleaned_bills:
    print(f"Starting EOBR mapping validation for {len(cleaned_bills)} cleaned bills...")
    
    # FIRST: Diagnose what we actually have in our data vs what generator expects
    print(f"\n🔍 DATA STRUCTURE DIAGNOSIS (vs eobr_generator.py requirements):")
    print(f"=" * 70)
    
    if cleaned_bills:
        sample_bill = cleaned_bills[0]
        print(f"Sample bill keys: {list(sample_bill.keys())}")
        
        sample_line_items = sample_bill.get('line_items', [])
        if sample_line_items:
            print(f"Sample line item keys: {list(sample_line_items[0].keys())}")
        
        # Check EXACT fields that eobr_generator.py expects
        print(f"\n🔍 CHECKING EXACT GENERATOR REQUIREMENTS:")
        print(f"Generator expects these EXACT field names:")
        
        generator_bill_fields = [
            'PatientName', 'Patient_DOB', 'Order_ID', 'FileMaker_Record_Number', 'Patient_Injury_Date',
            'provider_tin', 'provider_npi', 'provider_billing_name', 'provider_billing_address1',
            'provider_billing_address2', 'provider_billing_city', 'provider_billing_state', 
            'provider_billing_postal_code'
        ]
        
        for field in generator_bill_fields:
            value = sample_bill.get(field, 'FIELD_NOT_FOUND')
            status = "✅" if field in sample_bill else "❌ MISSING FIELD"
            print(f"   {field}: '{value}' {status}")
        
        if sample_line_items:
            print(f"\nGenerator expects these line item fields:")
            generator_line_fields = [
                'date_of_service', 'place_of_service', 'cpt_code', 'modifier', 
                'units', 'charge_amount', 'allowed_amount'
            ]
            
            sample_item = sample_line_items[0]
            for field in generator_line_fields:
                value = sample_item.get(field, 'FIELD_NOT_FOUND')
                status = "✅" if field in sample_item else "❌ MISSING FIELD"
                print(f"   {field}: '{value}' {status}")
    
    eobr_ready_bills, mapping_report = validate_eobr_data_mapping_fixed(cleaned_bills)
    
    print(f"\n✅ EOBR MAPPING VALIDATION COMPLETE!")
    print(f"   Bills ready for EOBR generation: {len(eobr_ready_bills)}")
    
    if eobr_ready_bills:
        total_eobr_amount = sum(
            sum(float(item.get('allowed_amount', 0)) for item in bill.get('line_items', []))
            for bill in eobr_ready_bills
        )
        print(f"   Total EOBR amount: ${total_eobr_amount:.2f}")
        
        # Show exact generator mapping preview
        print(f"\n🎯 GENERATOR WILL USE THIS EXACT MAPPING:")
        print(f"=" * 60)
        
        sample_eobr_bill = eobr_ready_bills[0]
        
        # Show the exact prepare_bill_data() mapping
        print(f"Header section (from prepare_bill_data()):")
        print(f"   'PatientName': str(bill.get('PatientName', '')) = '{sample_eobr_bill.get('PatientName', '')}'")
        print(f"   'dob': self.format_date(bill.get('Patient_DOB')) = '{sample_eobr_bill.get('Patient_DOB', '')}'")
        print(f"   'process_date': datetime.now().strftime('%m/%d/%Y') = '{datetime.now().strftime('%m/%d/%Y')}'")
        print(f"   'order_no': str(bill.get('Order_ID', '')) = '{sample_eobr_bill.get('Order_ID', '')}'")
        print(f"   'provider_ref': str(bill.get('FileMaker_Record_Number', '')) = '{sample_eobr_bill.get('FileMaker_Record_Number', '')}'")
        print(f"   'doi': self.format_date(bill.get('Patient_Injury_Date')) = '{sample_eobr_bill.get('Patient_Injury_Date', '')}'")
        
        print(f"\nProvider section (from prepare_bill_data()):")
        print(f"   'TIN': str(bill.get('provider_tin', '')) = '{sample_eobr_bill.get('provider_tin', '')}'")
        print(f"   'NPI': str(bill.get('provider_npi', '')) = '{sample_eobr_bill.get('provider_npi', '')}'")
        print(f"   'billing_name': str(bill.get('provider_billing_name', '')) = '{sample_eobr_bill.get('provider_billing_name', '')}'")
        print(f"   'billing_address1': str(bill.get('provider_billing_address1', '')) = '{sample_eobr_bill.get('provider_billing_address1', '')}'")
        print(f"   'billing_city': str(bill.get('provider_billing_city', '')) = '{sample_eobr_bill.get('provider_billing_city', '')}'")
        print(f"   'billing_state': str(bill.get('provider_billing_state', '')) = '{sample_eobr_bill.get('provider_billing_state', '')}'")
        print(f"   'billing_zip': str(bill.get('provider_billing_postal_code', '')) = '{sample_eobr_bill.get('provider_billing_postal_code', '')}'")
        
        sample_line_items = sample_eobr_bill.get('line_items', [])
        if sample_line_items:
            print(f"\nLine items section (first item):")
            item = sample_line_items[0]
            print(f"   'dos1': self.format_date(item.get('date_of_service')) = '{item.get('date_of_service', '')}'")
            print(f"   'pos1': str(item.get('place_of_service', '11')) = '{item.get('place_of_service', '11')}'")
            print(f"   'cpt1': str(item.get('cpt_code', '')) = '{item.get('cpt_code', '')}'")
            print(f"   'modifier1': modifier logic = '{item.get('modifier', '')}'")
            print(f"   'units1': str(item.get('units', 1)) = '{item.get('units', 1)}'")
            print(f"   'charge1': self.format_currency(charge_amount) = '${float(item.get('charge_amount', 0)):.2f}'")
            print(f"   'alwd1': self.format_currency(allowed_amount) = '${float(item.get('allowed_amount', 0)):.2f}'")
            
            # Show calculated fields
            allowed = float(item.get('allowed_amount', 0))
            charge = float(item.get('charge_amount', 0))
            if allowed == 0:
                reason_code = "125"
                paid_amount = 0.0
            elif allowed < charge:
                reason_code = "85"
                paid_amount = allowed
            else:
                reason_code = "85"
                paid_amount = allowed
            
            print(f"   'paid1': CALCULATED = '${paid_amount:.2f}'")
            print(f"   'code1': CALCULATED = '{reason_code}'")
        
        total_paid = sum(float(item.get('allowed_amount', 0)) for item in sample_line_items)
        print(f"\nFooter:")
        print(f"   'total_paid': CALCULATED = '${total_paid:.2f}'")
        
        # Show POS policy enforcement summary
        pos_conversion_count = len(mapping_report.get('pos_conversions', []))
        if pos_conversion_count > 0:
            print(f"\n🏥 POS CODE POLICY ENFORCED:")
            print(f"   ✅ {pos_conversion_count} line items converted to POS '11' (Office)")
            print(f"   ✅ All EOBRs will show consistent 'Office' place of service")
        else:
            print(f"\n🏥 POS CODE POLICY:")
            print(f"   ✅ All line items already have POS '11' (Office)")
        
        print(f"\n🚀 Ready for Cell 8: EOBR Generation!")
        
    else:
        print(f"\n❌ NO BILLS READY FOR EOBR GENERATION")
        print(f"   Please fix the field mapping issues above")
        
        # Show what fields are missing
        if mapping_report.get('mapping_issues'):
            print(f"\n🔧 TO FIX THE ISSUES:")
            all_issues = []
            for issue_bill in mapping_report['mapping_issues']:
                all_issues.extend(issue_bill['issues'])
            
            unique_issues = list(set(all_issues))
            print(f"   Common issues found:")
            for issue in unique_issues[:10]:  # Show first 10 unique issues
                print(f"      - {issue}")
    
else:
    print("❌ No cleaned_bills available from previous steps")
    print("   Please run Cells 1-6 first")
    eobr_ready_bills = []
    mapping_report = {}

# %% [markdown]
# -- Cell 8: EOBR Document Generation

# %%
# Cell 8: EOBR Document Generation (FINAL - with ProviderBill ID Filenames)
# =============================================================================

from utils.eobr_generator import EOBRGenerator
from pathlib import Path
from datetime import datetime
import re
from docx import Document

print("STEP 8: EOBR DOCUMENT GENERATION (FINAL - PROVIDERBILL ID FILENAMES)")
print("=" * 60)

class FinalEOBRGenerator(EOBRGenerator):
    """
    Final EOBR Generator with:
    1. Fixed placeholder replacement (handles split placeholders)
    2. Synchronized EOBR numbering with Excel generator
    3. ProviderBill ID as filename (instead of EOBR control number)
    """
    
    def get_eobr_control_number(self, bill):
        """
        Generate EOBR control number using same logic as Excel generator.
        Format: FileMaker_Record_Number-X
        
        Args:
            bill: Bill dictionary
            
        Returns:
            EOBR control number (e.g., "2024122212201-1")
        """
        fm_record = bill.get('FileMaker_Record_Number', 'UNKNOWN')
        bill_id = bill.get('id', '')
        
        # Use consistent hash-based sequence for the same bill
        # This ensures the same bill always gets the same EOBR number
        sequence = (abs(hash(f"{fm_record}{bill_id}")) % 999) + 1
        
        eobr_number = f"{fm_record}-{sequence}"
        return eobr_number
    
    def replace_placeholders_in_paragraph_fixed(self, paragraph, data: dict):
        """
        Fixed placeholder replacement that handles placeholders split across runs.
        """
        if not paragraph.text or '<' not in paragraph.text:
            return
        
        # Get the complete paragraph text
        full_text = paragraph.text
        
        # Find all placeholders in the complete text
        placeholders = re.findall(r'<([^>]+)>', full_text)
        
        if not placeholders:
            return
        
        # Replace each placeholder in the complete text
        for placeholder in placeholders:
            placeholder_text = f'<{placeholder}>'
            replacement_text = data.get(placeholder, placeholder_text)
            full_text = full_text.replace(placeholder_text, replacement_text)
        
        # Rebuild the paragraph with replaced text
        for run in paragraph.runs:
            run.text = ""
        
        if paragraph.runs:
            paragraph.runs[0].text = full_text
            # Remove extra runs
            for i in range(len(paragraph.runs) - 1, 0, -1):
                paragraph._element.remove(paragraph.runs[i]._element)
        else:
            paragraph.add_run(full_text)
    
    def replace_placeholders_in_document_fixed(self, doc: Document, data: dict):
        """
        Fixed placeholder replacement throughout the document.
        """
        try:
            # Replace in paragraphs
            for paragraph in doc.paragraphs:
                if '<' in paragraph.text:
                    self.replace_placeholders_in_paragraph_fixed(paragraph, data)
            
            # Replace in tables
            for table in doc.tables:
                for row in table.rows:
                    for cell in row.cells:
                        for paragraph in cell.paragraphs:
                            if '<' in paragraph.text:
                                self.replace_placeholders_in_paragraph_fixed(paragraph, data)
            
            # Replace in headers and footers
            for section in doc.sections:
                if section.header:
                    for paragraph in section.header.paragraphs:
                        if '<' in paragraph.text:
                            self.replace_placeholders_in_paragraph_fixed(paragraph, data)
                
                if section.footer:
                    for paragraph in section.footer.paragraphs:
                        if '<' in paragraph.text:
                            self.replace_placeholders_in_paragraph_fixed(paragraph, data)
            
        except Exception as e:
            print(f"❌ Error in placeholder replacement: {str(e)}")
            raise
    
    def generate_eobr_final(self, bill: dict, output_dir: Path) -> tuple:
        """
        Generate EOBR with ProviderBill ID as filename.
        
        Args:
            bill: Bill dictionary
            output_dir: Output directory
            
        Returns:
            Tuple of (success: bool, output_path: Path, bill_id: str)
        """
        try:
            bill_id = bill.get('id', 'unknown')
            patient_name = bill.get('PatientName', 'Unknown')
            
            # Generate EOBR control number (for document content)
            eobr_number = self.get_eobr_control_number(bill)
            
            print(f"📄 Generating EOBR for {patient_name} (Bill: {bill_id})")
            print(f"🎯 EOBR Control Number: {eobr_number}")
            print(f"📁 Filename will be: {bill_id}.docx")
            
            # Fix dates for the generator
            fixed_bill = self.fix_dates_for_generator(bill)
            
            # Prepare data for replacement
            data = self.prepare_bill_data(fixed_bill)
            
            # OVERRIDE order_no with our synchronized EOBR number
            data['order_no'] = eobr_number
            
            print(f"📋 Document <order_no>: {eobr_number}")
            
            # Load template
            doc = Document(str(self.template_path))
            
            # Use FIXED placeholder replacement
            self.replace_placeholders_in_document_fixed(doc, data)
            
            # Create filename using ProviderBill ID
            filename = f"{bill_id}.docx"
            output_path = output_dir / filename
            
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save document
            doc.save(str(output_path))
            
            # Verify file was created
            if output_path.exists() and output_path.stat().st_size > 0:
                file_size = output_path.stat().st_size
                print(f"✅ EOBR saved as: {filename} ({file_size:,} bytes)")
                return True, output_path, bill_id
            else:
                print(f"❌ EOBR file not created or empty")
                return False, output_path, bill_id
            
        except Exception as e:
            print(f"❌ Error generating EOBR: {str(e)}")
            import traceback
            traceback.print_exc()
            return False, None, ""
    
    def fix_dates_for_generator(self, bill):
        """
        Fix date formats that cause generator warnings.
        """
        fixed_bill = bill.copy()
        
        # Fix Patient_DOB: '07 28 1997' -> '07/28/1997'
        patient_dob = bill.get('Patient_DOB', '')
        if patient_dob and isinstance(patient_dob, str):
            if re.match(r'\d{2} \d{2} \d{4}', patient_dob.strip()):
                parts = patient_dob.strip().split()
                if len(parts) == 3:
                    month, day, year = parts
                    fixed_bill['Patient_DOB'] = f"{month}/{day}/{year}"
        
        # Fix Patient_Injury_Date: remove timestamp
        injury_date = bill.get('Patient_Injury_Date', '')
        if injury_date and isinstance(injury_date, str) and ' ' in injury_date:
            fixed_bill['Patient_Injury_Date'] = injury_date.split(' ')[0]
        
        return fixed_bill

def generate_final_eobrs(eobr_ready_bills, output_dir):
    """
    Generate EOBRs with ProviderBill ID filenames.
    
    Args:
        eobr_ready_bills: List of validated bills
        output_dir: Base output directory
        
    Returns:
        Dictionary with generation results
    """
    if not eobr_ready_bills:
        return {
            'success': False,
            'message': 'No bills ready for EOBR generation',
            'generated_files': []
        }
    
    # Setup EOBR output directory
    eobr_output_dir = output_dir / "eobrs"
    eobr_output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"🚀 GENERATING FINAL EOBRS WITH PROVIDERBILL ID FILENAMES")
    print(f"   Bills to process: {len(eobr_ready_bills)}")
    print(f"   Output directory: {eobr_output_dir}")
    print(f"   Filename format: [ProviderBill_ID].docx")
    
    try:
        # Initialize final EOBR generator
        generator = FinalEOBRGenerator()
        print(f"   📋 Template: {generator.template_path}")
        
        generated_files = []
        bill_ids_generated = []
        total_amount = 0.0
        
        print(f"\n📄 PROCESSING BILLS:")
        print("=" * 50)
        
        for i, bill in enumerate(eobr_ready_bills):
            print(f"\n--- Bill {i+1}/{len(eobr_ready_bills)} ---")
            
            # Generate EOBR
            success, output_path, bill_id = generator.generate_eobr_final(bill, eobr_output_dir)
            
            if success:
                generated_files.append(output_path)
                bill_ids_generated.append(bill_id)
                
                # Calculate amount for summary
                line_items = bill.get('line_items', [])
                bill_amount = sum(float(item.get('allowed_amount', 0)) for item in line_items)
                total_amount += bill_amount
                
                print(f"💰 Amount: ${bill_amount:.2f}")
            else:
                print(f"❌ Generation failed for bill {bill.get('id')}")
        
        print(f"\n📊 FINAL GENERATION SUMMARY:")
        print("=" * 40)
        print(f"   Total bills processed: {len(eobr_ready_bills)}")
        print(f"   Successfully generated: {len(generated_files)}")
        print(f"   Success rate: {len(generated_files)/len(eobr_ready_bills)*100:.1f}%")
        print(f"   Total amount: ${total_amount:.2f}")
        
        # Show generated files
        if generated_files:
            print(f"\n📋 GENERATED EOBR FILES (ProviderBill ID Filenames):")
            print("-" * 30)
            for i, (file_path, bill_id) in enumerate(zip(generated_files[:10], bill_ids_generated[:10])):
                file_size = file_path.stat().st_size if file_path.exists() else 0
                print(f"   {i+1:2d}. {file_path.name} ({file_size:,} bytes)")
            
            if len(generated_files) > 10:
                print(f"   ... and {len(generated_files) - 10} more files")
            
            # Show file verification
            total_size = sum(f.stat().st_size for f in generated_files if f.exists())
            print(f"\n📁 FILE SUMMARY:")
            print(f"   Total files: {len(generated_files)}")
            print(f"   Total size: {total_size:,} bytes ({total_size/1024/1024:.1f} MB)")
            print(f"   Location: {eobr_output_dir}")
        
        return {
            'success': True,
            'generated_files': generated_files,
            'bill_ids': bill_ids_generated,
            'output_directory': eobr_output_dir,
            'bills_processed': len(eobr_ready_bills),
            'files_generated': len(generated_files),
            'total_amount': total_amount
        }
        
    except Exception as e:
        print(f"❌ EOBR generation failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'message': str(e),
            'generated_files': []
        }

# Execute final EOBR generation
if 'eobr_ready_bills' in locals() and eobr_ready_bills:
    
    # Use existing output directory or create new one
    if 'output_dir' not in locals():
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = Path("batch_outputs") / f"batch_{timestamp}"
        output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Starting FINAL EOBR generation for {len(eobr_ready_bills)} bills...")
    print(f"📁 Filename = ProviderBill ID")
    print(f"🎯 EOBR Control Number = Document content only")
    
    eobr_result = generate_final_eobrs(eobr_ready_bills, output_dir)
    
    if eobr_result['success']:
        print(f"\n🎉 FINAL EOBR GENERATION SUCCESS!")
        print("=" * 45)
        print(f"   📁 Output Directory: {eobr_result['output_directory']}")
        print(f"   📄 Files Generated: {eobr_result['files_generated']}")
        print(f"   💰 Total Amount: ${eobr_result['total_amount']:.2f}")
        print(f"   📊 Success Rate: {eobr_result['files_generated']/eobr_result['bills_processed']*100:.1f}%")
        
        print(f"\n📁 FILENAME STRUCTURE:")
        print(f"   ✅ Filenames use ProviderBill ID (easy database lookup)")
        print(f"   ✅ EOBR Control Numbers are in document content")
        print(f"   ✅ Perfect for matching files to database records")
        
        print(f"\n📁 FILE STRUCTURE:")
        print(f"   {output_dir}/")
        print(f"   ├── excel/")
        print(f"   │   └── batch_payment_data.xlsx")
        print(f"   ├── eobrs/")
        print(f"   │   ├── BILL_12345.docx")
        print(f"   │   ├── BILL_12346.docx")
        print(f"   │   └── ... ({eobr_result['files_generated']} files)")
        print(f"   └── audit/")
        print(f"       └── audit_log_[timestamp].xlsx")
        
        print(f"\n🎯 BENEFITS OF PROVIDERBILL ID FILENAMES:")
        print(f"   1. ✅ Easy to find specific bill's EOBR document")
        print(f"   2. ✅ Perfect for database-driven file lookups")
        print(f"   3. ✅ Consistent with your Input File field in Excel")
        print(f"   4. ✅ No complex EOBR numbering needed for filenames")
        
    else:
        print(f"\n❌ EOBR GENERATION FAILED!")
        print(f"   Error: {eobr_result.get('message', 'Unknown error')}")

else:
    print("❌ No EOBR-ready bills available")
    print("   Please run Cell 7 first to validate data mapping")

print(f"\n✅ FINAL EOBR GENERATION COMPLETE!")

# Show what the filenames will look like
if 'eobr_result' in locals() and eobr_result.get('success') and eobr_result.get('bill_ids'):
    print(f"\n🔍 SAMPLE EOBR FILENAMES (ProviderBill IDs):")
    for i, bill_id in enumerate(eobr_result['bill_ids'][:5]):
        print(f"   {i+1}. {bill_id}.docx")
    if len(eobr_result['bill_ids']) > 5:
        print(f"   ... and {len(eobr_result['bill_ids']) - 5} more")

# %% [markdown]
# -- Save EOBR docx as PDF

# %%
# Cell 9: Convert EOBR DOCX Files to PDF
# =============================================================================

from pathlib import Path
import os
import sys
from datetime import datetime

print("STEP 9: CONVERT EOBR DOCX FILES TO PDF")
print("=" * 60)

def convert_docx_to_pdf_windows(docx_path, pdf_path):
    """
    Convert DOCX to PDF using Microsoft Word COM interface (Windows only).
    This maintains exact formatting and layout.
    
    Args:
        docx_path: Path to input DOCX file
        pdf_path: Path to output PDF file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        import win32com.client
        
        # Create Word application
        word = win32com.client.Dispatch('Word.Application')
        word.Visible = False  # Don't show Word window
        word.DisplayAlerts = False  # Don't show dialog boxes
        
        # Open the document
        doc = word.Documents.Open(str(docx_path.absolute()))
        
        # Export as PDF (17 = PDF format)
        doc.ExportAsFixedFormat(
            OutputFileName=str(pdf_path.absolute()),
            ExportFormat=17,  # PDF format
            OptimizeFor=0,    # Optimize for print
            BitmapMissingFonts=True,
            DocStructureTags=False,
            CreateBookmarks=False,
            UseISO19005_1=False
        )
        
        # Close document and Word
        doc.Close()
        word.Quit()
        
        return True
        
    except ImportError:
        print(f"   ❌ win32com not available - try: pip install pywin32")
        return False
    except Exception as e:
        print(f"   ❌ Word COM error: {str(e)}")
        return False

def convert_docx_to_pdf_alternative(docx_path, pdf_path):
    """
    Convert DOCX to PDF using python-docx2pdf library.
    Fallback method if Word COM is not available.
    
    Args:
        docx_path: Path to input DOCX file
        pdf_path: Path to output PDF file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        from docx2pdf import convert
        
        convert(str(docx_path), str(pdf_path))
        return True
        
    except ImportError:
        print(f"   ❌ docx2pdf not available - try: pip install docx2pdf")
        return False
    except Exception as e:
        print(f"   ❌ docx2pdf error: {str(e)}")
        return False

def convert_single_eobr_to_pdf(docx_path, output_dir, test_mode=False):
    """
    Convert a single EOBR DOCX file to PDF.
    
    Args:
        docx_path: Path to DOCX file
        output_dir: Directory for PDF output
        test_mode: If True, adds detailed logging for testing
        
    Returns:
        tuple: (success: bool, pdf_path: Path)
    """
    try:
        if test_mode:
            print(f"\n🧪 TEST MODE - Converting single file:")
            print(f"   📄 Input: {docx_path.name}")
        
        # Check if input file exists
        if not docx_path.exists():
            print(f"   ❌ Input file not found: {docx_path}")
            return False, None
        
        # Create output PDF path
        pdf_filename = docx_path.stem + ".pdf"  # Replace .docx with .pdf
        pdf_path = output_dir / pdf_filename
        
        if test_mode:
            print(f"   📄 Output: {pdf_filename}")
            print(f"   📁 Full path: {pdf_path}")
            print(f"   📊 Input size: {docx_path.stat().st_size:,} bytes")
        
        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Try Word COM method first (best quality)
        if test_mode:
            print(f"   🔄 Attempting Word COM conversion...")
        
        success = convert_docx_to_pdf_windows(docx_path, pdf_path)
        
        if not success:
            if test_mode:
                print(f"   🔄 Word COM failed, trying docx2pdf...")
            success = convert_docx_to_pdf_alternative(docx_path, pdf_path)
        
        if success and pdf_path.exists():
            pdf_size = pdf_path.stat().st_size
            if test_mode:
                print(f"   ✅ Conversion successful!")
                print(f"   📊 Output size: {pdf_size:,} bytes")
                print(f"   📁 PDF saved: {pdf_path}")
            return True, pdf_path
        else:
            if test_mode:
                print(f"   ❌ Conversion failed - PDF not created")
            return False, None
            
    except Exception as e:
        if test_mode:
            print(f"   ❌ Error during conversion: {str(e)}")
        return False, None

def convert_all_eobrs_to_pdf(eobr_directory, test_first=False, max_files=None):
    """
    Convert all DOCX files in EOBR directory to PDF and organize files.
    
    Args:
        eobr_directory: Directory containing DOCX files
        test_first: If True, test on one file first
        max_files: Maximum number of files to convert (None = all)
        
    Returns:
        Dictionary with conversion results
    """
    try:
        eobr_dir = Path(eobr_directory)
        
        if not eobr_dir.exists():
            return {
                'success': False,
                'message': f'EOBR directory not found: {eobr_dir}',
                'converted_files': []
            }
        
        # Find all DOCX files
        docx_files = list(eobr_dir.glob("*.docx"))
        
        if not docx_files:
            return {
                'success': False,
                'message': f'No DOCX files found in {eobr_dir}',
                'converted_files': []
            }
        
        print(f"📂 EOBR Directory: {eobr_dir}")
        print(f"📋 Found {len(docx_files)} DOCX files")
        
        # Create organized output directories
        pdf_output_dir = eobr_dir / "pdfs"
        docx_archive_dir = eobr_dir / "word_docx"
        pdf_output_dir.mkdir(exist_ok=True)
        docx_archive_dir.mkdir(exist_ok=True)
        
        print(f"📁 PDF Output: {pdf_output_dir}")
        print(f"📁 DOCX Archive: {docx_archive_dir}")
        
        converted_files = []
        failed_files = []
        moved_files = []
        
        # TEST MODE: Convert just one file first
        if test_first:
            print(f"\n🧪 TEST MODE: Converting and organizing first file only")
            print("=" * 50)
            
            test_file = docx_files[0]
            success, pdf_path = convert_single_eobr_to_pdf(test_file, pdf_output_dir, test_mode=True)
            
            if success:
                converted_files.append(pdf_path)
                
                # Test moving the DOCX file
                print(f"\n📦 Testing DOCX file organization...")
                try:
                    new_docx_path = docx_archive_dir / test_file.name
                    test_file.rename(new_docx_path)
                    moved_files.append(new_docx_path)
                    print(f"   ✅ Moved DOCX: {test_file.name} → word_docx/{test_file.name}")
                except Exception as e:
                    print(f"   ❌ Failed to move DOCX: {str(e)}")
                
                print(f"\n✅ TEST SUCCESSFUL!")
                print(f"   📄 PDF Created: {pdf_path.name}")
                print(f"   📦 DOCX Organized: word_docx/{test_file.name}")
                print(f"\n💡 To convert ALL files, set test_first=False")
                
                return {
                    'success': True,
                    'test_mode': True,
                    'converted_files': converted_files,
                    'moved_files': moved_files,
                    'failed_files': failed_files,
                    'total_files': len(docx_files),
                    'pdf_directory': pdf_output_dir,
                    'docx_directory': docx_archive_dir
                }
            else:
                failed_files.append(test_file)
                print(f"\n❌ TEST FAILED!")
                print(f"   📄 Failed: {test_file.name}")
                print(f"\n🔧 TROUBLESHOOTING:")
                print(f"   1. Install Microsoft Word (for best quality)")
                print(f"   2. Or install: pip install docx2pdf")
                print(f"   3. Check file permissions")
                return {
                    'success': False,
                    'test_mode': True,
                    'converted_files': converted_files,
                    'moved_files': moved_files,
                    'failed_files': failed_files,
                    'total_files': len(docx_files),
                    'pdf_directory': pdf_output_dir,
                    'docx_directory': docx_archive_dir
                }
        
        # FULL CONVERSION MODE
        else:
            print(f"\n🚀 FULL CONVERSION AND ORGANIZATION MODE")
            print("=" * 50)
            
            files_to_process = docx_files
            if max_files:
                files_to_process = docx_files[:max_files]
                print(f"📊 Processing first {max_files} files")
            
            for i, docx_file in enumerate(files_to_process, 1):
                print(f"\n--- File {i}/{len(files_to_process)}: {docx_file.name} ---")
                
                # Convert to PDF
                success, pdf_path = convert_single_eobr_to_pdf(docx_file, pdf_output_dir, test_mode=False)
                
                if success:
                    converted_files.append(pdf_path)
                    print(f"   ✅ PDF converted successfully")
                    
                    # Move DOCX to archive directory
                    try:
                        new_docx_path = docx_archive_dir / docx_file.name
                        docx_file.rename(new_docx_path)
                        moved_files.append(new_docx_path)
                        print(f"   📦 DOCX moved to word_docx/")
                    except Exception as e:
                        print(f"   ⚠️  Warning: Could not move DOCX file: {str(e)}")
                else:
                    failed_files.append(docx_file)
                    print(f"   ❌ PDF conversion failed - DOCX file left in place")
            
            # Summary
            print(f"\n📊 CONVERSION AND ORGANIZATION SUMMARY:")
            print("=" * 45)
            print(f"   Total DOCX files: {len(docx_files)}")
            print(f"   Files processed: {len(files_to_process)}")
            print(f"   Successfully converted to PDF: {len(converted_files)}")
            print(f"   DOCX files organized: {len(moved_files)}")
            print(f"   Failed conversions: {len(failed_files)}")
            print(f"   Success rate: {len(converted_files)/len(files_to_process)*100:.1f}%")
            
            if converted_files:
                total_pdf_size = sum(f.stat().st_size for f in converted_files if f.exists())
                print(f"   Total PDF size: {total_pdf_size:,} bytes ({total_pdf_size/1024/1024:.1f} MB)")
            
            if moved_files:
                total_docx_size = sum(f.stat().st_size for f in moved_files if f.exists())
                print(f"   Total DOCX size: {total_docx_size:,} bytes ({total_docx_size/1024/1024:.1f} MB)")
            
            print(f"\n📁 FINAL ORGANIZATION:")
            print(f"   📄 PDFs: {pdf_output_dir}")
            print(f"   📦 Original DOCX: {docx_archive_dir}")
            
            if failed_files:
                print(f"\n❌ Failed files (left in original location):")
                for failed_file in failed_files[:5]:  # Show first 5 failures
                    print(f"      {failed_file.name}")
                if len(failed_files) > 5:
                    print(f"      ... and {len(failed_files) - 5} more")
            
            return {
                'success': len(converted_files) > 0,
                'test_mode': False,
                'converted_files': converted_files,
                'moved_files': moved_files,
                'failed_files': failed_files,
                'total_files': len(docx_files),
                'processed_files': len(files_to_process),
                'pdf_directory': pdf_output_dir,
                'docx_directory': docx_archive_dir
            }
            
    except Exception as e:
        print(f"❌ Error in PDF conversion: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'message': str(e),
            'converted_files': []
        }

# Main Cell 9 Execution
print("🔍 CHECKING FOR EOBR DIRECTORY...")

# Try to find the EOBR directory from previous steps
eobr_directory = None

# Check if we have eobr_result from Cell 8
if 'eobr_result' in locals() and eobr_result.get('success'):
    eobr_directory = eobr_result.get('output_directory')
    print(f"✅ Found EOBR directory from Cell 8: {eobr_directory}")

# Fallback: Look for recent batch directories
if not eobr_directory:
    batch_dirs = list(Path("batch_outputs").glob("batch_*"))
    if batch_dirs:
        # Sort by modification time, get newest
        newest_batch = max(batch_dirs, key=lambda p: p.stat().st_mtime)
        potential_eobr_dir = newest_batch / "eobrs"
        if potential_eobr_dir.exists():
            eobr_directory = potential_eobr_dir
            print(f"✅ Found EOBR directory in latest batch: {eobr_directory}")

# Manual override option
if not eobr_directory:
    print(f"❌ No EOBR directory found automatically")
    print(f"💡 You can manually set the directory:")
    print(f"   eobr_directory = Path('path/to/your/eobr/folder')")
    print(f"   Then re-run this cell")
else:
    print(f"\n🚀 STARTING PDF CONVERSION")
    print("=" * 60)
    
    # CONFIGURATION
    TEST_FIRST = False  # Set to False to convert all files
    MAX_FILES = None   # Set to number to limit conversion (e.g., 10)
    
    print(f"⚙️  SETTINGS:")
    print(f"   Test first file only: {TEST_FIRST}")
    print(f"   Max files to convert: {MAX_FILES if MAX_FILES else 'All'}")
    
    # Convert files
    pdf_result = convert_all_eobrs_to_pdf(
        eobr_directory=eobr_directory,
        test_first=TEST_FIRST,
        max_files=MAX_FILES
    )
    
    # Show results
    if pdf_result['success']:
        if pdf_result.get('test_mode'):
            print(f"\n🎉 TEST CONVERSION AND ORGANIZATION COMPLETE!")
            print(f"✅ Successfully tested PDF conversion and file organization")
            print(f"📁 Test PDF created in: {pdf_result['pdf_directory']}")
            print(f"📦 Test DOCX moved to: {pdf_result['docx_directory']}")
            
            print(f"\n🚀 TO CONVERT AND ORGANIZE ALL FILES:")
            print(f"   1. Change TEST_FIRST = False")
            print(f"   2. Re-run this cell")
            print(f"   3. Will convert all {pdf_result['total_files']} DOCX files")
            print(f"   4. Will organize files into proper directories")
        else:
            print(f"\n🎉 FULL CONVERSION AND ORGANIZATION COMPLETE!")
            print(f"✅ Successfully converted {len(pdf_result['converted_files'])} files to PDF")
            print(f"✅ Successfully organized {len(pdf_result['moved_files'])} DOCX files")
            print(f"📁 PDFs saved in: {pdf_result['pdf_directory']}")
            print(f"📦 DOCX archived in: {pdf_result['docx_directory']}")
            
            # Show final directory structure
            print(f"\n📂 FINAL DIRECTORY STRUCTURE:")
            print(f"   eobrs/")
            print(f"   ├── pdfs/")
            print(f"   │   ├── BILL_12345.pdf")
            print(f"   │   ├── BILL_12346.pdf")
            print(f"   │   └── ... ({len(pdf_result['converted_files'])} PDF files)")
            print(f"   └── word_docx/")
            print(f"       ├── BILL_12345.docx")
            print(f"       ├── BILL_12346.docx")
            print(f"       └── ... ({len(pdf_result['moved_files'])} DOCX files)")
    else:
        print(f"\n❌ PDF CONVERSION FAILED!")
        print(f"Error: {pdf_result.get('message', 'Unknown error')}")

print(f"\n✅ CELL 9 COMPLETE!")
print(f"\n💡 NEXT STEPS:")
print(f"   1. First run with TEST_FIRST = True (test 1 file)")
print(f"   2. If test works, set TEST_FIRST = False (convert all)")
print(f"   3. PDFs will be in eobrs/pdfs/ directory")
print(f"   4. Original DOCX files will be in eobrs/word_docx/ directory")
print(f"   5. Clean, organized file structure for easy access!")

# %% [markdown]
# -- Cell 4: validate for Excel Gen

# %% [markdown]
# -- Cell 4: validate for Excel Gen

# %% [markdown]
# -- Cell 4: validate for Excel Gen

# %% [markdown]
# -- Cell 4: validate for Excel Gen


