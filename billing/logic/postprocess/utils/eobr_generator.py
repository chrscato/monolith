# billing/logic/postprocess/utils/eobr_generator.py

import logging
import os
import tempfile
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, date
from docx import Document
import re

logger = logging.getLogger(__name__)

# Get the project root directory
PROJECT_ROOT = Path(__file__).resolve().parents[4]
TEMPLATE_PATH = PROJECT_ROOT / "EOBR Template.docx"

class EOBRGenerator:
    """
    Generator for EOBR (Explanation of Bill Review) documents.
    """
    
    def __init__(self, template_path: Path = None):
        """
        Initialize the EOBR generator.
        
        Args:
            template_path: Path to the EOBR template file
        """
        self.template_path = template_path or TEMPLATE_PATH
        if not self.template_path.exists():
            raise FileNotFoundError(f"EOBR template not found at {self.template_path}")
        
        logger.info(f"EOBR Generator initialized with template: {self.template_path}")
    
    def format_currency(self, amount: float) -> str:
        """Format currency amount."""
        if amount is None:
            return "$0.00"
        return f"${amount:.2f}"
    
    def format_date(self, date_value: Any) -> str:
        """Format date value to MM/DD/YYYY."""
        if not date_value:
            return ""
        
        if isinstance(date_value, str):
            # Try to parse if it's a string
            try:
                if '-' in date_value:  # YYYY-MM-DD format
                    parsed_date = datetime.strptime(date_value, '%Y-%m-%d').date()
                else:
                    parsed_date = datetime.strptime(date_value, '%m/%d/%Y').date()
                return parsed_date.strftime('%m/%d/%Y')
            except ValueError:
                return date_value  # Return as-is if can't parse
        
        if isinstance(date_value, (date, datetime)):
            return date_value.strftime('%m/%d/%Y')
        
        return str(date_value)
    
    def prepare_bill_data(self, bill: Dict[str, Any]) -> Dict[str, str]:
        """
        Prepare bill data for EOBR template replacement.
        
        Args:
            bill: Bill dictionary with all required data
            
        Returns:
            Dictionary of placeholder mappings
        """
        line_items = bill.get('line_items', [])
        
        # Calculate total paid (sum of allowed amounts)
        total_paid = sum(
            float(item.get('allowed_amount', 0)) 
            for item in line_items 
            if item.get('allowed_amount') is not None
        )
        
        # Prepare header data
        data = {
            # Header section
            'PatientName': bill.get('PatientName', ''),
            'dob': self.format_date(bill.get('Patient_DOB')),
            'process_date': datetime.now().strftime('%m/%d/%Y'),
            'order_no': bill.get('Order_ID', ''),
            'provider_ref': bill.get('FileMaker_Record_Number', ''),
            'doi': self.format_date(bill.get('Patient_Injury_Date')),
            
            # Provider section
            'TIN': bill.get('provider_tin', ''),
            'NPI': bill.get('provider_npi', ''),
            'billing_name': bill.get('provider_billing_name', ''),
            'billing_address1': bill.get('provider_billing_address1', ''),
            'billing_address2': bill.get('provider_billing_address2', ''),
            'billing_city': bill.get('provider_billing_city', ''),
            'billing_state': bill.get('provider_billing_state', ''),
            'billing_zip': bill.get('provider_billing_postal_code', ''),
            
            # Footer
            'total_paid': self.format_currency(total_paid)
        }
        
        # Prepare line items (up to 6 slots)
        for i in range(1, 7):  # dos1 through dos6
            if i <= len(line_items):
                item = line_items[i-1]
                
                # Determine paid amount and reason code
                allowed_amount = float(item.get('allowed_amount', 0))
                charge_amount = float(item.get('charge_amount', 0))
                
                # Default reason codes based on amount comparison
                if allowed_amount == 0:
                    reason_code = "125"  # Denied
                    paid_amount = 0
                elif allowed_amount < charge_amount:
                    reason_code = "85"   # Reduced payment
                    paid_amount = allowed_amount
                else:
                    reason_code = "85"   # Full payment
                    paid_amount = allowed_amount
                
                data.update({
                    f'dos{i}': self.format_date(item.get('date_of_service')),
                    f'pos{i}': item.get('place_of_service', '11'),
                    f'cpt{i}': item.get('cpt_code', ''),
                    f'modifier{i}': item.get('modifier', ''),
                    f'units{i}': str(item.get('units', 1)),
                    f'charge{i}': self.format_currency(charge_amount),
                    f'alwd{i}': self.format_currency(allowed_amount),
                    f'paid{i}': self.format_currency(paid_amount),
                    f'code{i}': reason_code
                })
            else:
                # Empty slots
                data.update({
                    f'dos{i}': '',
                    f'pos{i}': '',
                    f'cpt{i}': '',
                    f'modifier{i}': '',
                    f'units{i}': '',
                    f'charge{i}': '',
                    f'alwd{i}': '',
                    f'paid{i}': '',
                    f'code{i}': ''
                })
        
        return data
    
    def replace_placeholders_in_text(self, text: str, data: Dict[str, str]) -> str:
        """
        Replace placeholders in text with actual data.
        
        Args:
            text: Text containing placeholders like <PatientName>
            data: Dictionary of placeholder mappings
            
        Returns:
            Text with placeholders replaced
        """
        if not text:
            return text
        
        # Replace placeholders using regex
        def replace_placeholder(match):
            placeholder = match.group(1)
            return data.get(placeholder, f"<{placeholder}>")  # Keep original if not found
        
        # Find all placeholders in format <placeholder>
        return re.sub(r'<([^>]+)>', replace_placeholder, text)
    
    def replace_placeholders_in_document(self, doc: Document, data: Dict[str, str]):
        """
        Replace placeholders throughout the entire document.
        
        Args:
            doc: python-docx Document object
            data: Dictionary of placeholder mappings
        """
        # Replace in paragraphs
        for paragraph in doc.paragraphs:
            if paragraph.text:
                new_text = self.replace_placeholders_in_text(paragraph.text, data)
                if new_text != paragraph.text:
                    # Clear existing runs and add new text
                    paragraph.clear()
                    paragraph.add_run(new_text)
        
        # Replace in tables
        for table in doc.tables:
            for row in table.rows:
                for cell in row.cells:
                    for paragraph in cell.paragraphs:
                        if paragraph.text:
                            new_text = self.replace_placeholders_in_text(paragraph.text, data)
                            if new_text != paragraph.text:
                                paragraph.clear()
                                paragraph.add_run(new_text)
        
        # Replace in headers and footers
        for section in doc.sections:
            # Header
            if section.header:
                for paragraph in section.header.paragraphs:
                    if paragraph.text:
                        new_text = self.replace_placeholders_in_text(paragraph.text, data)
                        if new_text != paragraph.text:
                            paragraph.clear()
                            paragraph.add_run(new_text)
            
            # Footer
            if section.footer:
                for paragraph in section.footer.paragraphs:
                    if paragraph.text:
                        new_text = self.replace_placeholders_in_text(paragraph.text, data)
                        if new_text != paragraph.text:
                            paragraph.clear()
                            paragraph.add_run(new_text)
    
    def generate_eobr(self, bill: Dict[str, Any], output_path: Path) -> bool:
        """
        Generate an EOBR document for a single bill.
        
        Args:
            bill: Bill dictionary with all required data including line_items
            output_path: Path where the generated EOBR should be saved
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Generating EOBR for bill {bill.get('bill_id')}")
            
            # Prepare data for replacement
            data = self.prepare_bill_data(bill)
            
            # Load template
            doc = Document(self.template_path)
            
            # Replace placeholders
            self.replace_placeholders_in_document(doc, data)
            
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save document
            doc.save(str(output_path))
            
            logger.info(f"EOBR generated successfully: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error generating EOBR for bill {bill.get('bill_id')}: {str(e)}")
            return False
    
    def generate_batch_eobrs(self, 
                           bills: List[Dict[str, Any]], 
                           output_dir: Path,
                           filename_pattern: str = "EOBR_{bill_id}_{patient_name}.docx") -> List[Path]:
        """
        Generate EOBR documents for multiple bills.
        
        Args:
            bills: List of bill dictionaries
            output_dir: Directory where EOBRs should be saved
            filename_pattern: Pattern for output filenames (can use {bill_id}, {patient_name}, etc.)
            
        Returns:
            List of paths to successfully generated EOBRs
        """
        generated_files = []
        
        logger.info(f"Generating EOBRs for {len(bills)} bills")
        
        for bill in bills:
            try:
                # Prepare filename
                bill_id = bill.get('bill_id', 'unknown')
                patient_name = bill.get('PatientName', 'unknown_patient')
                
                # Clean patient name for filename
                safe_patient_name = re.sub(r'[<>:"/\\|?*]', '_', patient_name)
                
                filename = filename_pattern.format(
                    bill_id=bill_id,
                    patient_name=safe_patient_name,
                    order_id=bill.get('Order_ID', ''),
                    date=datetime.now().strftime('%Y%m%d')
                )
                
                output_path = output_dir / filename
                
                # Generate EOBR
                if self.generate_eobr(bill, output_path):
                    generated_files.append(output_path)
                    
            except Exception as e:
                logger.error(f"Error processing bill {bill.get('bill_id')}: {str(e)}")
                continue
        
        logger.info(f"Successfully generated {len(generated_files)} EOBRs out of {len(bills)} bills")
        return generated_files

def generate_eobr_documents(bills: List[Dict[str, Any]], 
                          output_dir: Path = None,
                          template_path: Path = None) -> List[Path]:
    """
    Convenience function to generate EOBR documents.
    
    Args:
        bills: List of prepared bill dictionaries
        output_dir: Output directory (defaults to temp directory)
        template_path: Path to EOBR template
        
    Returns:
        List of paths to generated EOBR files
    """
    if output_dir is None:
        output_dir = Path(tempfile.mkdtemp()) / "eobrs"
    
    generator = EOBRGenerator(template_path)
    return generator.generate_batch_eobrs(bills, output_dir)

if __name__ == "__main__":
    # Test the EOBR generator
    logging.basicConfig(level=logging.INFO)
    
    # Sample bill data for testing
    test_bill = {
        'bill_id': 'TEST_001',
        'PatientName': 'John Doe',
        'Patient_DOB': '1980-01-15',
        'Order_ID': 'ORD_12345',
        'FileMaker_Record_Number': 'FM_001',
        'Patient_Injury_Date': '2024-01-01',
        'provider_tin': '12-3456789',
        'provider_npi': '1234567890',
        'provider_billing_name': 'Test Medical Center',
        'provider_billing_address1': '123 Medical Drive',
        'provider_billing_address2': 'Suite 100',
        'provider_billing_city': 'Orlando',
        'provider_billing_state': 'FL',
        'provider_billing_postal_code': '32801',
        'line_items': [
            {
                'date_of_service': '2024-01-15',
                'place_of_service': '11',
                'cpt_code': '99213',
                'modifier': 'LT',
                'units': 1,
                'charge_amount': 150.00,
                'allowed_amount': 120.00
            },
            {
                'date_of_service': '2024-01-15',
                'place_of_service': '11',
                'cpt_code': '73610',
                'modifier': '',
                'units': 1,
                'charge_amount': 200.00,
                'allowed_amount': 180.00
            }
        ]
    }
    
    # Test generation
    try:
        generator = EOBRGenerator()
        output_path = Path("test_eobr.docx")
        success = generator.generate_eobr(test_bill, output_path)
        if success:
            print(f"✅ Test EOBR generated: {output_path}")
        else:
            print("❌ Failed to generate test EOBR")
    except Exception as e:
        print(f"❌ Error: {str(e)}")