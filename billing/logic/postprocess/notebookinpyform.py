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
bills = get_approved_unpaid_bills(limit=20)  # Adjust limit as needed

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

# %% [markdown]
# -- Cell 5: Excel Gen

# %%
# Cell 5: Excel Generation using excel_generator.py utils
# =============================================================================

from billing.logic.postprocess.utils.excel_generator import (
    ExcelBatchGenerator,
    generate_excel_batch
)
from pathlib import Path
from datetime import datetime

print("STEP 5: EXCEL GENERATION")
print("=" * 60)

if 'excel_ready_bills' in locals() and excel_ready_bills and len(excel_ready_bills) > 0:
    
    # Setup output directory
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = Path("batch_outputs") / f"batch_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"🚀 Generating Excel batch for {len(excel_ready_bills)} bills...")
    print(f"📁 Output directory: {output_dir}")
    
    try:
        # Use the excel_generator utility function
        batch_excel_path, summary = generate_excel_batch(
            bills=excel_ready_bills,
            batch_output_dir=output_dir
        )
        
        print(f"\n✅ Excel generation successful!")
        print(f"📄 Batch file: {batch_excel_path}")
        
        # Display comprehensive summary
        print(f"\n📊 BATCH SUMMARY:")
        print(f"   Batch file: {batch_excel_path.name}")
        print(f"   Total records: {summary.get('total_records', 0)}")
        print(f"   New records: {summary.get('new_records', 0)}")
        print(f"   Duplicate records: {summary.get('duplicate_records', 0)}")
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
            
            if file_size > 1000:  # Reasonable size check
                print(f"   ✅ Excel file appears to have content")
                
                # Try to read a sample of the Excel to verify content
                try:
                    import pandas as pd
                    df = pd.read_excel(batch_excel_path)
                    print(f"   📋 Excel verification:")
                    print(f"      Rows: {len(df)}")
                    print(f"      Columns: {len(df.columns)}")
                    
                    # Show sample data
                    if len(df) > 0:
                        print(f"   📄 Sample Excel data:")
                        sample_row = df.iloc[0]
                        print(f"      Vendor: {sample_row.get('Vendor', 'N/A')}")
                        print(f"      EOBR Number: {sample_row.get('EOBR Number', 'N/A')}")
                        print(f"      Amount: ${sample_row.get('Amount', 0):.2f}")
                        print(f"      Bill Date: {sample_row.get('Bill Date', 'N/A')}")
                        print(f"      Due Date: {sample_row.get('Due Date', 'N/A')}")
                        print(f"      Duplicate Check: {sample_row.get('Duplicate Check', 'N/A')}")
                        print(f"      Release Payment: {sample_row.get('Release Payment', 'N/A')}")
                        
                except ImportError:
                    print(f"   (pandas not available for Excel verification)")
                except Exception as e:
                    print(f"   ⚠️  Could not verify Excel content: {str(e)}")
            else:
                print(f"   ⚠️  Excel file seems unusually small")
        else:
            print(f"   ❌ Excel file was not created")
        
        # Show duplicate analysis if any
        if summary.get('duplicate_records', 0) > 0:
            print(f"\n🔍 DUPLICATE ANALYSIS:")
            print(f"   Found {summary.get('duplicate_records')} duplicate records")
            print(f"   These records were marked as 'Release Payment: N'")
            print(f"   Only new records will be processed for payment")
        
        # Provide next steps
        print(f"\n📋 NEXT STEPS:")
        print(f"   1. Review the Excel file: {batch_excel_path}")
        print(f"   2. Import into QuickBooks if everything looks correct")
        print(f"   3. New records total: ${summary.get('release_amount', 0):.2f}")
        
        # Historical file info
        if summary.get('new_records', 0) > 0:
            print(f"   4. Historical data updated with {summary.get('new_records')} new records")
        
    except Exception as e:
        print(f"❌ Excel generation failed: {str(e)}")
        
        # Detailed error information
        import traceback
        print(f"\n🔍 ERROR DETAILS:")
        print(f"   Error type: {type(e).__name__}")
        print(f"   Error message: {str(e)}")
        
        # Check if it's a data issue
        if excel_ready_bills:
            print(f"\n🔍 DEBUGGING INFO:")
            print(f"   Bills to process: {len(excel_ready_bills)}")
            
            # Check first bill structure
            first_bill = excel_ready_bills[0]
            print(f"   Sample bill keys: {list(first_bill.keys())}")
            
            line_items = first_bill.get('line_items', [])
            print(f"   Sample line items: {len(line_items)}")
            
            if line_items:
                first_item = line_items[0]
                print(f"   Sample line item keys: {list(first_item.keys())}")
        
        # Show traceback for debugging
        print(f"\n📋 FULL TRACEBACK:")
        traceback.print_exc()

else:
    print("❌ No Excel-ready bills available for generation")
    print("   Please check the validation and cleaning steps above")

print(f"\n🎉 Excel generation step completed!")

# Final summary
if 'batch_excel_path' in locals() and batch_excel_path.exists():
    print(f"\n✅ SUCCESS: Excel batch generated at {batch_excel_path}")
else:
    print(f"\n❌ No Excel file generated - check errors above")

# %%


# %% [markdown]
# -- Cell 4: validate for Excel Gen

# %% [markdown]
# -- Cell 4: validate for Excel Gen

# %% [markdown]
# -- Cell 4: validate for Excel Gen

# %% [markdown]
# -- Cell 4: validate for Excel Gen

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

def update_database_bill_status(excel_ready_bills, mark_as_paid=False):
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
    
    # Import database utilities
    try:
        from billing.logic.postprocess.utils.data_validation import get_db_connection
    except ImportError:
        print("   ❌ Could not import database utilities")
        return {
            'updated': False,
            'bill_count': 0,
            'message': 'Database utilities not available'
        }
    
    # Get bill IDs to update
    bill_ids = [bill.get('id') for bill in excel_ready_bills]
    
    print(f"   🚀 Updating {len(bill_ids)} bills in ProviderBill table...")
    
    # Perform database update
    conn = get_db_connection()
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
# -- Run Excel Generator

# %%
# Generate Excel batch files
from pathlib import Path

batch_output_dir = Path("batch_outputs") / f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
historical_excel_path = Path("data/Historical_EOBR_Data.xlsx")

batch_excel_path, excel_summary = generate_excel_batch(
    cleaned_bills, 
    batch_output_dir, 
    historical_excel_path
)

print(f"📊 Excel batch generated: {batch_excel_path}")
print(f"Summary: {excel_summary}")

# %% [markdown]
# -- Mark Bills as Paid

# %%
# Update database to mark bills as paid
from payment_updater import mark_bills_as_paid, update_payment_status

# Extract bill IDs that were successfully processed
processed_bill_ids = [bill.get('id') for bill in cleaned_bills]

# Mark as paid in database
payment_update_result = mark_bills_as_paid(processed_bill_ids)

print(f"💰 Marked {payment_update_result['updated_count']} bills as paid")
print(f"Updated tables: ProviderBill, BillLineItem, ReimbursementLog")


