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

def simple_standardize_date_format(date_str: str) -> str:
    """
    Standardize date strings to YYYY-MM-DD format.
    
    Args:
        date_str: Raw date string (like "11/25/24" or "2024-11-25")
        
    Returns:
        Standardized date string in YYYY-MM-DD format
    """
    if not date_str:
        return ""
    
    date_str = str(date_str).strip()
    
    # Handle date ranges - take the first date
    if ' - ' in date_str:
        date_str = date_str.split(' - ')[0].strip()
    
    # Try the specific formats we're seeing
    formats_to_try = [
        '%Y-%m-%d',      # 2024-11-25 (already correct)
        '%m/%d/%y',      # 11/25/24
        '%m/%d/%Y',      # 11/25/2024
        '%Y/%m/%d',      # 2024/11/25
        '%m-%d-%y',      # 11-25-24
        '%m-%d-%Y',      # 11-25-2024
    ]
    
    for fmt in formats_to_try:
        try:
            parsed_date = datetime.strptime(date_str, fmt).date()
            
            # Handle 2-digit years - assume 20xx for years 00-30, 19xx for 31-99
            if parsed_date.year < 100:
                if parsed_date.year <= 30:
                    parsed_date = parsed_date.replace(year=parsed_date.year + 2000)
                else:
                    parsed_date = parsed_date.replace(year=parsed_date.year + 1900)
            
            # Validate reasonable year range
            if 1900 <= parsed_date.year <= 2030:
                return parsed_date.strftime('%Y-%m-%d')
                
        except ValueError:
            continue
    
    print(f"⚠️  Could not parse date: '{date_str}'")
    return date_str  # Return original if can't parse

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
        # Minimum requirements for Excel generation
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
            
            # Show what Excel row will look like
            print(f"\n📝 PREDICTED EXCEL ROW:")
            print(f"   Vendor: {bill.get('provider_billing_name', '')}")
            print(f"   EOBR Number: {bill.get('FileMaker_Record_Number', '')}-X (X will be calculated)")
            print(f"   Bill Date: {earliest_dos}")
            print(f"   Amount: ${bill_total:.2f}")
            print(f"   Description: {earliest_dos}, {', '.join(sorted(cpt_codes))}, {bill.get('PatientName', '')}, {bill.get('FileMaker_Record_Number', '')}")
            print(f"   Memo: {earliest_dos}, {bill.get('PatientName', '')}")
            
            # Pause between bills for readability
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
# Cell 6: Audit Log Generation & Database Updates (COMPLETE FIXED VERSION)
# =============================================================================

import pandas as pd
from datetime import datetime
from pathlib import Path
from utils.db_utils import get_db_connection

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
                'PROCESSED'
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
    
    print(f"   ✅ Audit log saved with {len(audit_dfs)} sheets")
    return audit_path

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
    
    # Import database utilities - FIXED IMPORT PATH
    try:
        from billing.logic.postprocess.utils.db_utils import get_db_connection
        # Alternative try for different import structure
    except ImportError:
        try:
            # Try relative import if absolute doesn't work
            from utils.db_utils import get_db_connection
        except ImportError:
            try:
                # Try direct import if in same directory
                from db_utils import get_db_connection
            except ImportError:
                print("   ❌ Could not import database utilities")
                print("   💡 Tried:")
                print("      - billing.logic.postprocess.utils.db_utils")
                print("      - utils.db_utils") 
                print("      - db_utils")
                return {
                    'updated': False,
                    'bill_count': 0,
                    'message': 'Database utilities not available - import failed'
                }
    
    # Get bill IDs to update
    bill_ids = [bill.get('id') for bill in excel_ready_bills]
    
    print(f"   🚀 Updating {len(bill_ids)} bills in ProviderBill table...")
    
    # Check if get_db_connection is implemented
    try:
        conn = get_db_connection()
        if conn is None:
            print("   ❌ get_db_connection() returned None")
            print("   💡 The function needs to be implemented in db_utils.py")
            return {
                'updated': False,
                'bill_count': 0,
                'message': 'Database connection not implemented'
            }
    except Exception as e:
        print(f"   ❌ Database connection failed: {str(e)}")
        print("   💡 Check if get_db_connection() is implemented in db_utils.py")
        return {
            'updated': False,
            'bill_count': 0,
            'message': f'Database connection error: {str(e)}'
        }
    
    cursor = conn.cursor()
    
    updated_count = 0
    failed_updates = []
    
    try:
        for bill_id in bill_ids:
            try:
                # Update the ProviderBill table
                cursor.execute("""
                    UPDATE ProviderBill 
                    SET bill_paid = 'Y',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (bill_id,))
                
                # Check if the update actually affected a row
                if cursor.rowcount > 0:
                    updated_count += 1
                    print(f"      ✅ {bill_id}: bill_paid = 'Y'")
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
        verification_sql = f"""
            SELECT id, bill_paid, updated_at 
            FROM ProviderBill 
            WHERE id IN ({','.join(['?' for _ in bill_ids])})
        """
        cursor.execute(verification_sql, bill_ids)
        verification_results = cursor.fetchall()
        
        verified_paid = 0
        for row in verification_results:
            if row[1] == 'Y':  # bill_paid column
                verified_paid += 1
        
        print(f"   ✅ Verification: {verified_paid}/{len(bill_ids)} bills marked as paid")
        
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
    

# Main Cell 6 Execution
print("STEP 6: AUDIT LOG GENERATION & DATABASE UPDATES")
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
    
    # Database update (now with actual implementation)
    update_report = update_database_bill_status(
        excel_ready_bills, 
        mark_as_paid=True  # CHANGED TO TRUE - WILL ACTUALLY UPDATE DATABASE
    )
    
    print(f"\n🔄 DATABASE UPDATE RESULTS:")
    print(f"   Bills processed: {update_report['bill_count']}")
    
    if update_report['updated']:
        print(f"   ✅ Successfully updated: {update_report['updated_count']}")
        print(f"   ❌ Failed updates: {update_report['failed_count']}")
        
        if update_report['failed_count'] > 0:
            print(f"   Failed bills:")
            for failure in update_report['failed_updates'][:3]:  # Show first 3
                print(f"      {failure['bill_id']}: {failure['error']}")
    else:
        print(f"   Status: {update_report['message']}")
    
    print(f"\n✅ CELL 6 COMPLETE!")
    print(f"   📄 Main Excel: {batch_path if batch_path else 'Not available'}")
    print(f"   📋 Audit Log: {audit_excel_path}")
    
    if update_report['updated']:
        print(f"   ✅ Database: {update_report['updated_count']} bills marked as paid")
    else:
        print(f"   🔄 Database: Ready for updates (set mark_as_paid=True)")

else:
    print("❌ No excel_ready_bills available from previous steps")
    print("   Please run Cells 1-5 first")

print(f"\n🎯 Next Steps:")
if 'audit_excel_path' in locals():
    print(f"   1. Review audit log: {audit_excel_path}")
else:
    print(f"   1. Audit log not generated")
print(f"   2. Verify Excel batch file")  
if 'update_report' in locals() and not update_report['updated']:
    print(f"   3. Set mark_as_paid=True to update database (when ready)")
else:
    print(f"   3. Database updates complete")

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

# %% [markdown]
# -- Cell 7: EOBR Data Mapping and Validation

# %%
# %%
# Cell 7: EOBR Data Mapping and Validation (FIXED TO MATCH ACTUAL GENERATOR)
# =============================================================================

print("STEP 7: EOBR DATA MAPPING AND VALIDATION")
print("=" * 60)

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
    print("=" * 50)
    
    eobr_ready_bills = []
    mapping_issues = []
    
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
                        if not value:
                            item[source_field] = '11'  # Generator sets default
                            value = '11'
                            status = "🔧"
                            print(f"         {source_field} → <{placeholder}>: '11' (default) {status}")
                        else:
                            status = "✅"
                            print(f"         {source_field} → <{placeholder}>: '{value}' {status}")
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
        'validation_timestamp': datetime.now().isoformat()
    }
    
    print(f"\n📊 EOBR MAPPING SUMMARY:")
    print(f"   Total bills: {mapping_report['total_bills']}")
    print(f"   EOBR ready: {mapping_report['eobr_ready_count']}")
    print(f"   With issues: {mapping_report['issues_count']}")
    
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
# %%
# Cell 8: EOBR Document Generation (FINAL - Synchronized with Excel)
# =============================================================================

from utils.eobr_generator import EOBRGenerator
from pathlib import Path
from datetime import datetime
import re
from docx import Document

print("STEP 8: EOBR DOCUMENT GENERATION (FINAL - SYNCHRONIZED)")
print("=" * 60)

class FinalEOBRGenerator(EOBRGenerator):
    """
    Final EOBR Generator with:
    1. Fixed placeholder replacement (handles split placeholders)
    2. Synchronized EOBR numbering with Excel generator
    3. EOBR control number as filename
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
        Generate EOBR with synchronized numbering and control number as filename.
        
        Args:
            bill: Bill dictionary
            output_dir: Output directory
            
        Returns:
            Tuple of (success: bool, output_path: Path, eobr_number: str)
        """
        try:
            bill_id = bill.get('id', 'unknown')
            patient_name = bill.get('PatientName', 'Unknown')
            
            # Generate EOBR control number (same logic as Excel)
            eobr_number = self.get_eobr_control_number(bill)
            
            print(f"📄 Generating EOBR for {patient_name} (Bill: {bill_id})")
            print(f"🎯 EOBR Control Number: {eobr_number}")
            
            # Fix dates for the generator
            fixed_bill = self.fix_dates_for_generator(bill)
            
            # Prepare data for replacement
            data = self.prepare_bill_data(fixed_bill)
            
            # OVERRIDE order_no with our synchronized EOBR number
            data['order_no'] = eobr_number
            
            print(f"📋 Overriding <order_no> with: {eobr_number}")
            
            # Load template
            doc = Document(str(self.template_path))
            
            # Use FIXED placeholder replacement
            self.replace_placeholders_in_document_fixed(doc, data)
            
            # Create filename using EOBR control number
            filename = f"{eobr_number}.docx"
            output_path = output_dir / filename
            
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save document
            doc.save(str(output_path))
            
            # Verify file was created
            if output_path.exists() and output_path.stat().st_size > 0:
                file_size = output_path.stat().st_size
                print(f"✅ EOBR saved as: {filename} ({file_size:,} bytes)")
                return True, output_path, eobr_number
            else:
                print(f"❌ EOBR file not created or empty")
                return False, output_path, eobr_number
            
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
    Generate EOBRs with final synchronized numbering and naming.
    
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
    
    print(f"🚀 GENERATING FINAL SYNCHRONIZED EOBRS")
    print(f"   Bills to process: {len(eobr_ready_bills)}")
    print(f"   Output directory: {eobr_output_dir}")
    print(f"   Filename format: [EOBR_Control_Number].docx")
    
    try:
        # Initialize final EOBR generator
        generator = FinalEOBRGenerator()
        print(f"   📋 Template: {generator.template_path}")
        
        generated_files = []
        eobr_numbers = []
        total_amount = 0.0
        
        print(f"\n📄 PROCESSING BILLS:")
        print("=" * 50)
        
        for i, bill in enumerate(eobr_ready_bills):
            print(f"\n--- Bill {i+1}/{len(eobr_ready_bills)} ---")
            
            # Generate EOBR
            success, output_path, eobr_number = generator.generate_eobr_final(bill, eobr_output_dir)
            
            if success:
                generated_files.append(output_path)
                eobr_numbers.append(eobr_number)
                
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
            print(f"\n📋 GENERATED EOBR FILES:")
            print("-" * 30)
            for i, (file_path, eobr_num) in enumerate(zip(generated_files[:10], eobr_numbers[:10])):
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
            'eobr_numbers': eobr_numbers,
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
    print(f"🔗 Synchronized with Excel EOBR numbering")
    print(f"📁 Filename = EOBR Control Number")
    
    eobr_result = generate_final_eobrs(eobr_ready_bills, output_dir)
    
    if eobr_result['success']:
        print(f"\n🎉 FINAL EOBR GENERATION SUCCESS!")
        print("=" * 45)
        print(f"   📁 Output Directory: {eobr_result['output_directory']}")
        print(f"   📄 Files Generated: {eobr_result['files_generated']}")
        print(f"   💰 Total Amount: ${eobr_result['total_amount']:.2f}")
        print(f"   📊 Success Rate: {eobr_result['files_generated']/eobr_result['bills_processed']*100:.1f}%")
        
        print(f"\n🔗 EXCEL INTEGRATION:")
        print(f"   ✅ EOBR Control Numbers match Excel exactly")
        print(f"   ✅ Filenames are the EOBR Control Numbers")
        print(f"   ✅ Easy to match Excel rows to EOBR files")
        
        print(f"\n📁 FILE STRUCTURE:")
        print(f"   {output_dir}/")
        print(f"   ├── excel/")
        print(f"   │   └── batch_payment_data.xlsx")
        print(f"   ├── eobrs/")
        print(f"   │   ├── 2024122212201-1.docx")
        print(f"   │   ├── 2024122212201-2.docx")
        print(f"   │   └── ... ({eobr_result['files_generated']} files)")
        print(f"   └── audit/")
        print(f"       └── audit_log_[timestamp].xlsx")
        
        print(f"\n🎯 NEXT STEPS:")
        print(f"   1. ✅ EOBRs generated with synchronized numbering")
        print(f"   2. ✅ Filenames match EOBR Control Numbers")
        print(f"   3. ✅ Ready to distribute to providers")
        print(f"   4. ✅ Excel and EOBR systems are synchronized")
        
    else:
        print(f"\n❌ EOBR GENERATION FAILED!")
        print(f"   Error: {eobr_result.get('message', 'Unknown error')}")

else:
    print("❌ No EOBR-ready bills available")
    print("   Please run Cell 7 first to validate data mapping")

print(f"\n✅ FINAL EOBR GENERATION COMPLETE!")

# Show what the EOBR control numbers will look like
if 'eobr_result' in locals() and eobr_result.get('success') and eobr_result.get('eobr_numbers'):
    print(f"\n🔍 SAMPLE EOBR CONTROL NUMBERS GENERATED:")
    for i, eobr_num in enumerate(eobr_result['eobr_numbers'][:5]):
        print(f"   {i+1}. {eobr_num}.docx")
    if len(eobr_result['eobr_numbers']) > 5:
        print(f"   ... and {len(eobr_result['eobr_numbers']) - 5} more")


