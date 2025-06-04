# billing/logic/postprocess/utils/excel_generator.py

import pandas as pd
import holidays
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import logging
import re
from decimal import Decimal

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Get the absolute path to the monolith root directory
DB_ROOT = Path(r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\monolith")

class ExcelBatchGenerator:
    """
    Generator for Excel batch files and historical log updates.
    Handles duplicate detection, EOBR numbering, and business day calculations.
    """
    
    def __init__(self, historical_excel_path: Path = None):
        """
        Initialize the Excel batch generator.
        
        Args:
            historical_excel_path: Path to the Historical_EOBR_Data.xlsx file
        """
        if historical_excel_path is None:
            historical_excel_path = Path("data/batch_outputs/Historical_EOBR_Data.xlsx")
        
        self.historical_excel_path = historical_excel_path
        self.historical_df = None
        self.us_holidays = None
        self.current_batch_keys = set()  # Track keys from current batch
        self._load_historical_data()
        self._init_holidays()
    
    def _load_historical_data(self):
        """Load historical Excel data or create empty DataFrame."""
        try:
            if self.historical_excel_path.exists():
                self.historical_df = pd.read_excel(self.historical_excel_path)
                logger.info(f"Loaded historical data: {len(self.historical_df)} records")
            else:
                logger.info("Historical file not found, creating new DataFrame")
                self._create_empty_historical_df()
        except Exception as e:
            logger.error(f"Error loading historical data: {str(e)}")
            self._create_empty_historical_df()
    
    def _create_empty_historical_df(self):
        """Create empty DataFrame with required columns."""
        columns = [
            'Release Payment', 'Duplicate Check', 'Full Duplicate Key',
            'Input File', 'EOBR Number', 'Vendor', 'Mailing Address',
            'Terms', 'Bill Date', 'Due Date', 'Category', 'Description',
            'Amount', 'Memo', 'Total'
        ]
        self.historical_df = pd.DataFrame(columns=columns)
    
    def _init_holidays(self):
        """Initialize US federal holidays for current and next year."""
        current_year = datetime.now().year
        self.us_holidays = holidays.UnitedStates(years=range(current_year, current_year + 2))
        logger.debug(f"Initialized holidays for years {current_year}-{current_year + 1}")
    
    def create_duplicate_key(self, bill: Dict[str, Any]) -> str:
        """
        Create Full Duplicate Key from FileMaker_Record_Number and CPT codes.
        
        Args:
            bill: Bill dictionary with line_items
            
        Returns:
            Full Duplicate Key in format: "FM_RECORD|CPT1,CPT2,CPT3"
        """
        try:
            # Get FileMaker Record Number from order data
            fm_record = bill.get('FileMaker_Record_Number', '')
            if not fm_record:
                logger.warning(f"No FileMaker_Record_Number found for bill {bill.get('id')}")
                fm_record = 'UNKNOWN'
            
            # Get all CPT codes from line items
            line_items = bill.get('line_items', [])
            cpts = []
            
            for item in line_items:
                cpt = item.get('cpt_code', '').strip()
                if cpt:
                    cpts.append(cpt)
            
            # Sort CPTs for consistent key generation
            cpts.sort()
            cpt_string = ','.join(cpts)
            
            full_key = f"{fm_record}|{cpt_string}"
            
            # Log detailed information about key creation
            logger.info(f"Creating duplicate key for bill {bill.get('id')}:")
            logger.info(f"  FileMaker Record: {fm_record}")
            logger.info(f"  CPT codes: {cpts}")
            logger.info(f"  Generated key: {full_key}")
            
            return full_key
            
        except Exception as e:
            logger.error(f"Error creating duplicate key for bill {bill.get('id')}: {str(e)}")
            return f"ERROR_{bill.get('id', 'UNKNOWN')}|"
    
    def check_duplicate(self, full_duplicate_key: str) -> bool:
        """
        Check if Full Duplicate Key exists in historical data or current batch.
        
        Args:
            full_duplicate_key: The duplicate key to check
            
        Returns:
            True if duplicate exists, False otherwise
        """
        if self.historical_df.empty and not self.current_batch_keys:
            return False
        
        # Check if key exists in historical data
        existing_keys = self.historical_df['Full Duplicate Key'].fillna('').tolist()
        
        # Log detailed information about the check
        logger.info(f"Checking duplicate key: {full_duplicate_key}")
        logger.info(f"Current batch keys: {list(self.current_batch_keys)}")
        logger.info(f"Historical keys count: {len(existing_keys)}")
        
        # Check historical data
        historical_match = full_duplicate_key in existing_keys
        if historical_match:
            # Find the matching row in historical data
            matching_row = self.historical_df[self.historical_df['Full Duplicate Key'] == full_duplicate_key].iloc[0]
            row_num = self.historical_df[self.historical_df['Full Duplicate Key'] == full_duplicate_key].index[0] + 1  # +1 for 1-based indexing
            logger.info(f"Found match in historical data at row {row_num}:")
            logger.info(f"  EOBR Number: {matching_row['EOBR Number']}")
            logger.info(f"  Bill Date: {matching_row['Bill Date']}")
            logger.info(f"  Amount: ${matching_row['Amount']:.2f}")
            logger.info(f"  Description: {matching_row['Description']}")
        
        # Check current batch
        current_batch_match = full_duplicate_key in self.current_batch_keys
        if current_batch_match:
            logger.info(f"Found match in current batch: {full_duplicate_key}")
        
        is_duplicate = historical_match or current_batch_match
        
        if is_duplicate:
            logger.info(f"Duplicate found: {full_duplicate_key}")
        else:
            logger.debug(f"No duplicate found for: {full_duplicate_key}")
            # Add to current batch keys for future checks
            self.current_batch_keys.add(full_duplicate_key)
        
        return is_duplicate
    
    def get_next_eobr_number(self, fm_record_number: str) -> str:
        """
        Get the next EOBR number for a FileMaker_Record_Number across ALL history.
        
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
    
    def calculate_due_date(self, bill_date_str: str) -> str:
        """
        Calculate due date: bill_date + 45 business days (excluding federal holidays).
        
        Args:
            bill_date_str: Bill date in YYYY-MM-DD format
            
        Returns:
            Due date in YYYY-MM-DD format
        """
        try:
            # Parse bill date
            bill_date = datetime.strptime(bill_date_str, '%Y-%m-%d').date()
            
            current_date = bill_date
            business_days_added = 0
            
            while business_days_added < 45:
                current_date += timedelta(days=1)
                
                # Skip weekends (Saturday=5, Sunday=6)
                if current_date.weekday() >= 5:
                    continue
                
                # Skip federal holidays
                if current_date in self.us_holidays:
                    logger.debug(f"Skipping holiday: {current_date} ({self.us_holidays.get(current_date)})")
                    continue
                
                business_days_added += 1
            
            due_date_str = current_date.strftime('%Y-%m-%d')
            logger.debug(f"Due date calculated: {bill_date_str} + 45 business days = {due_date_str}")
            
            return due_date_str
            
        except Exception as e:
            logger.error(f"Error calculating due date for {bill_date_str}: {str(e)}")
            # Fallback: add 65 calendar days
            try:
                fallback_date = datetime.strptime(bill_date_str, '%Y-%m-%d').date() + timedelta(days=65)
                return fallback_date.strftime('%Y-%m-%d')
            except:
                return bill_date_str
    
    def get_earliest_service_date(self, line_items: List[Dict[str, Any]]) -> str:
        """
        Get the earliest date of service from line items.
        
        Args:
            line_items: List of line item dictionaries
            
        Returns:
            Earliest date in YYYY-MM-DD format
        """
        try:
            dates = []
            
            for item in line_items:
                date_str = item.get('date_of_service', '')
                if not date_str:
                    continue
                
                # Handle date ranges (take first date)
                if ' - ' in date_str:
                    date_str = date_str.split(' - ')[0].strip()
                
                # Try to parse date
                try:
                    # If already in YYYY-MM-DD format
                    if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                        dates.append(datetime.strptime(date_str, '%Y-%m-%d').date())
                    # If in MM/DD/YYYY format
                    elif re.match(r'\d{1,2}/\d{1,2}/\d{4}', date_str):
                        dates.append(datetime.strptime(date_str, '%m/%d/%Y').date())
                    # If in MM/DD/YY format
                    elif re.match(r'\d{1,2}/\d{1,2}/\d{2}', date_str):
                        dates.append(datetime.strptime(date_str, '%m/%d/%y').date())
                except ValueError as e:
                    logger.warning(f"Could not parse date: {date_str} - {str(e)}")
                    continue
            
            if not dates:
                logger.warning("No valid dates found in line items")
                return datetime.now().strftime('%Y-%m-%d')
            
            earliest_date = min(dates)
            return earliest_date.strftime('%Y-%m-%d')
            
        except Exception as e:
            logger.error(f"Error getting earliest service date: {str(e)}")
            return datetime.now().strftime('%Y-%m-%d')
    
    def format_mailing_address(self, bill: Dict[str, Any]) -> str:
        """
        Format provider mailing address from provider data.
        
        Args:
            bill: Bill dictionary with provider information
            
        Returns:
            Formatted mailing address string
        """
        try:
            address_parts = []
            
            # Billing Address 1
            addr1 = bill.get('provider_billing_address1', '').strip()
            if addr1:
                address_parts.append(addr1)
            
            # Billing Address 2
            addr2 = bill.get('provider_billing_address2', '').strip()
            if addr2:
                address_parts.append(addr2)
            
            # City, State ZIP
            city = bill.get('provider_billing_city', '').strip()
            state = bill.get('provider_billing_state', '').strip()
            zip_code = bill.get('provider_billing_postal_code', '').strip()
            
            city_state_zip_parts = []
            if city:
                city_state_zip_parts.append(city)
            if state:
                city_state_zip_parts.append(state)
            if zip_code:
                city_state_zip_parts.append(zip_code)
            
            if city_state_zip_parts:
                city_state_zip = ', '.join(city_state_zip_parts[:2])  # City, State
                if len(city_state_zip_parts) > 2:  # Add ZIP
                    city_state_zip += f' {city_state_zip_parts[2]}'
                address_parts.append(city_state_zip)
            
            formatted_address = ', '.join(address_parts)
            logger.debug(f"Formatted mailing address: {formatted_address}")
            
            return formatted_address
            
        except Exception as e:
            logger.error(f"Error formatting mailing address: {str(e)}")
            return "Address formatting error"
    
    def calculate_total_amount(self, line_items: List[Dict[str, Any]]) -> float:
        """
        Calculate total allowed amount from line items.
        
        Args:
            line_items: List of line item dictionaries
            
        Returns:
            Total allowed amount
        """
        try:
            total = 0.0
            
            for item in line_items:
                allowed_amount = item.get('allowed_amount')
                if allowed_amount is not None:
                    try:
                        total += float(allowed_amount)
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse allowed_amount: {allowed_amount}")
                        continue
            
            logger.debug(f"Calculated total amount: ${total:.2f}")
            return total
            
        except Exception as e:
            logger.error(f"Error calculating total amount: {str(e)}")
            return 0.0
    
    def format_description(self, bill: Dict[str, Any], bill_date: str) -> str:
        """
        Format description field: dates, CPTs, patient name, FM record.
        
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
            
            # Add FileMaker Record Number
            fm_record = bill.get('FileMaker_Record_Number', '').strip()
            if fm_record:
                description_parts.append(fm_record)
            
            description = ', '.join(description_parts)
            logger.debug(f"Formatted description: {description}")
            
            return description
            
        except Exception as e:
            logger.error(f"Error formatting description: {str(e)}")
            return "Description formatting error"
    
    def format_memo(self, bill: Dict[str, Any], bill_date: str) -> str:
        """
        Format memo field: dates, patient name.
        
        Args:
            bill: Bill dictionary
            bill_date: Formatted bill date
            
        Returns:
            Formatted memo string
        """
        try:
            memo_parts = []
            
            # Add bill date
            memo_parts.append(bill_date)
            
            # Add patient name
            patient_name = bill.get('PatientName', '').strip()
            if patient_name:
                memo_parts.append(patient_name)
            
            memo = ', '.join(memo_parts)
            logger.debug(f"Formatted memo: {memo}")
            
            return memo
            
        except Exception as e:
            logger.error(f"Error formatting memo: {str(e)}")
            return "Memo formatting error"
    
    def create_excel_row(self, bill: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a single Excel row for a bill.
        
        Args:
            bill: Bill dictionary with all required data
            
        Returns:
            Dictionary representing one Excel row
        """
        try:
            logger.info(f"Processing bill {bill.get('id')} for Excel generation")
            
            # Create duplicate key
            full_duplicate_key = self.create_duplicate_key(bill)
            
            # Check for duplicate
            is_duplicate = self.check_duplicate(full_duplicate_key)
            
            # Get EOBR number
            fm_record = bill.get('FileMaker_Record_Number', '')
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
            
            # Create Excel row
            excel_row = {
                'Release Payment': 'N' if is_duplicate else 'Y',
                'Duplicate Check': 'Y' if is_duplicate else 'N',
                'Full Duplicate Key': full_duplicate_key,
                'Input File': bill.get('id', ''),
                'EOBR Number': eobr_number,
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
                       f"Amount=${total_amount:.2f}, Duplicate={is_duplicate}")
            
            return excel_row
            
        except Exception as e:
            logger.error(f"Error creating Excel row for bill {bill.get('id')}: {str(e)}")
            raise
    
    def generate_batch_excel(self, 
                           bills: List[Dict[str, Any]], 
                           batch_output_dir: Path,
                           batch_filename: str = "batch_payment_data.xlsx") -> Tuple[Path, int, int]:
        """
        Generate batch Excel file and update historical data.
        
        Args:
            bills: List of bill dictionaries
            batch_output_dir: Directory for batch output
            batch_filename: Name of the batch Excel file
            
        Returns:
            Tuple of (batch_excel_path, new_records_count, duplicate_count)
        """
        try:
            logger.info(f"Generating Excel batch for {len(bills)} bills")
            
            # Reset current batch keys for new batch
            self.current_batch_keys.clear()
            
            # Create Excel rows
            excel_rows = []
            duplicate_count = 0
            
            for bill in bills:
                row = self.create_excel_row(bill)
                excel_rows.append(row)
                
                if row['Duplicate Check'] == 'Y':
                    duplicate_count += 1
            
            new_records_count = len(excel_rows) - duplicate_count
            
            # Create DataFrame
            batch_df = pd.DataFrame(excel_rows)
            
            # Save batch Excel file
            excel_dir = batch_output_dir / "excel"
            excel_dir.mkdir(parents=True, exist_ok=True)
            batch_excel_path = excel_dir / batch_filename
            
            batch_df.to_excel(batch_excel_path, index=False)
            logger.info(f"Saved batch Excel file: {batch_excel_path}")
            
            # Update historical data with new records only
            new_records = batch_df[batch_df['Duplicate Check'] == 'N'].copy()
            
            if not new_records.empty:
                # Append to historical DataFrame
                self.historical_df = pd.concat([self.historical_df, new_records], ignore_index=True)
                
                # Save updated historical file
                self.historical_excel_path.parent.mkdir(parents=True, exist_ok=True)
                self.historical_df.to_excel(self.historical_excel_path, index=False)
                logger.info(f"Updated historical file with {len(new_records)} new records")
            else:
                logger.info("No new records to add to historical file")
            
            logger.info(f"Excel generation complete: {len(excel_rows)} total, "
                       f"{new_records_count} new, {duplicate_count} duplicates")
            
            return batch_excel_path, new_records_count, duplicate_count
            
        except Exception as e:
            logger.error(f"Error generating batch Excel: {str(e)}")
            raise
    
    def get_batch_summary(self, batch_excel_path: Path) -> Dict[str, Any]:
        """
        Get summary information about a generated batch.
        
        Args:
            batch_excel_path: Path to the batch Excel file
            
        Returns:
            Dictionary with batch summary information
        """
        try:
            batch_df = pd.read_excel(batch_excel_path)
            
            summary = {
                'total_records': len(batch_df),
                'new_records': len(batch_df[batch_df['Duplicate Check'] == 'N']),
                'duplicate_records': len(batch_df[batch_df['Duplicate Check'] == 'Y']),
                'total_amount': batch_df['Amount'].sum(),
                'release_amount': batch_df[batch_df['Release Payment'] == 'Y']['Amount'].sum(),
                'batch_file': str(batch_excel_path),
                'generation_time': datetime.now().isoformat()
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting batch summary: {str(e)}")
            return {}

def generate_excel_batch(bills: List[Dict[str, Any]], 
                        batch_output_dir: Path,
                        historical_excel_path: Path = None) -> Tuple[Path, Dict[str, Any]]:
    """
    Convenience function to generate Excel batch and return summary.
    
    Args:
        bills: List of prepared bill dictionaries
        batch_output_dir: Directory for batch output
        historical_excel_path: Path to historical Excel file
        
    Returns:
        Tuple of (batch_excel_path, summary_dict)
    """
    # Set up logging for this run
    logging.basicConfig(level=logging.INFO)
    logger.setLevel(logging.INFO)
    
    generator = ExcelBatchGenerator(historical_excel_path)
    batch_excel_path, new_count, dup_count = generator.generate_batch_excel(bills, batch_output_dir)
    summary = generator.get_batch_summary(batch_excel_path)
    
    return batch_excel_path, summary

if __name__ == "__main__":
    # Test the Excel generator
    import logging
    
    logging.basicConfig(level=logging.INFO)
    
    # Sample bill data for testing
    test_bills = [
        {
            'id': 'BILL_001',
            'FileMaker_Record_Number': 'FM_12345',
            'PatientName': 'John Doe',
            'provider_billing_name': 'Test Medical Center',
            'provider_billing_address1': '123 Medical Drive',
            'provider_billing_city': 'Orlando',
            'provider_billing_state': 'FL',
            'provider_billing_postal_code': '32801',
            'line_items': [
                {
                    'cpt_code': '99213',
                    'allowed_amount': 120.00,
                    'date_of_service': '2024-01-15'
                },
                {
                    'cpt_code': '73610',
                    'allowed_amount': 180.00,
                    'date_of_service': '2024-01-15'
                }
            ]
        }
    ]
    
    # Test generation
    try:
        output_dir = Path("test_batch")
        batch_path, summary = generate_excel_batch(test_bills, output_dir)
        print(f"✅ Test Excel generated: {batch_path}")
        print(f"📊 Summary: {summary}")
    except Exception as e:
        print(f"❌ Error: {str(e)}")