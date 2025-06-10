# billing/logic/postprocess/utils/patient_extraction_utils.py

import re
import pandas as pd
from datetime import datetime
from typing import Optional, Tuple
import logging

logger = logging.getLogger(__name__)

def extract_patient_name_from_description(description: str) -> Optional[str]:
    """
    Extract patient name from various description formats found in historical data.
    

    
    Args:
        description: The description field from historical data
        
    Returns:
        Extracted patient name or None if not found
    """
    if not description or pd.isna(description):
        return None
    
    description = str(description).strip()
    
    # Remove surrounding quotes if present (handles multiline entries)
    if description.startswith('"') and description.endswith('"'):
        description = description[1:-1]
    
    # Pattern 1: Date prefix format "YYYY-MM-DD CPT(s) Patient Name Record"
    # Example: "2024-11-05 72110 Bianell Martinez 20241021018-02"
    date_prefix_pattern = r'^\d{4}-\d{2}-\d{2}\s+(?:\d{5}(?:\s+\d{5})*)\s+(.+?)\s+\w+\d+(?:-\d+)?$'
    match = re.match(date_prefix_pattern, description)
    if match:
        patient_name = match.group(1).strip()
        logger.debug(f"Extracted patient (date prefix): {patient_name}")
        return patient_name
    
    # Pattern 2: Date range format "MM/DD/YY - MM/DD/YY CPT Patient Name Record"
    # Example: "12/26/24 - 12/26/24 73221 Janice Suarez Rivera 2024122221401"
    date_range_pattern = r'^\d{1,2}/\d{1,2}/\d{2}\s*-\s*\d{1,2}/\d{1,2}/\d{2}\s+\d{5}\s+(.+?)\s+\w+\d+(?:-\d+)?$'
    match = re.match(date_range_pattern, description)
    if match:
        patient_name = match.group(1).strip()
        logger.debug(f"Extracted patient (date range): {patient_name}")
        return patient_name
    
    # Pattern 3: Space-separated format "CPT(s) Patient Name Record"
    # Example: "72148 PATTERSON HENRY 11-160655"
    space_pattern = r'^(\d{5}(?:\s+\d{5})*)\s+(.+?)\s+([A-Z0-9\-/]+)$'
    match = re.match(space_pattern, description)
    if match:
        patient_name = match.group(2).strip()
        logger.debug(f"Extracted patient (space format): {patient_name}")
        return patient_name
    
    # Pattern 4: Comma-separated format "CPT(s), PATIENT, RECORD"
    # Example: "72148, PATTERSON, HENRY, 11-160655"
    if ',' in description:
        parts = [part.strip() for part in description.split(',')]
        
        if len(parts) >= 3:
            # First part should be CPT codes
            first_part = parts[0].strip()
            
            # Check if first part contains CPT-like codes
            cpt_pattern = r'^(?:\d{5}(?:\s+[A-Z]?\d{4,5})*|\d{5}(?:\s+\d{5})*)$'
            if re.match(cpt_pattern, first_part):
                # Handle provider names at the end (like "Global Neuro & Spine Institute")
                # Check if last part looks like a provider
                provider_indicators = ['LLC', 'INC', 'PC', 'CENTER', 'INSTITUTE', 'IMAGING', 'RADIOLOGY', 'HEALTH', 'MEDICAL']
                last_part = parts[-1].upper()
                
                if any(indicator in last_part for indicator in provider_indicators):
                    # Provider name at end, patient is second-to-last
                    if len(parts) >= 4:
                        patient_name = parts[-2].strip()
                        logger.debug(f"Extracted patient (with provider): {patient_name}")
                        return patient_name
                
                # Check if last part looks like a record number
                record_pattern = r'^(?:N/A|\w*\d+(?:-\d+)?(?:/\w+)?|\d+|[A-Z0-9\-/]+)$'
                if re.match(record_pattern, parts[-1]):
                    if len(parts) == 3:
                        # Simple case: CPT, PATIENT, RECORD
                        patient_name = parts[1].strip()
                        logger.debug(f"Extracted patient (3-part): {patient_name}")
                        return patient_name
                    elif len(parts) == 4:
                        # Could be: CPT, FIRST, LAST, RECORD
                        patient_name = f"{parts[1].strip()} {parts[2].strip()}"
                        logger.debug(f"Extracted patient (4-part): {patient_name}")
                        return patient_name
                    else:
                        # Multiple parts - join middle parts as patient name
                        patient_name = ' '.join(parts[1:-1]).strip()
                        logger.debug(f"Extracted patient (multi-part): {patient_name}")
                        return patient_name
    
    logger.debug(f"Could not extract patient name from: {description}")
    return None

def extract_date_from_description(description: str) -> Optional[str]:
    """
    Extract service date from description field.
    
    Args:
        description: The description field
        
    Returns:
        Date in YYYY-MM-DD format or None
    """
    if not description or pd.isna(description):
        return None
    
    description = str(description).strip()
    
    # Pattern 1: Date prefix "YYYY-MM-DD ..."
    date_prefix_pattern = r'^(\d{4}-\d{2}-\d{2})'
    match = re.match(date_prefix_pattern, description)
    if match:
        date_str = match.group(1)
        logger.debug(f"Extracted date (prefix): {date_str}")
        return date_str
    
    # Pattern 2: Date range "MM/DD/YY - MM/DD/YY ..." (take first date)
    date_range_pattern = r'^(\d{1,2})/(\d{1,2})/(\d{2})'
    match = re.match(date_range_pattern, description)
    if match:
        month, day, year = match.groups()
        # Convert 2-digit year to 4-digit (assuming 20XX for now)
        full_year = f"20{year}"
        try:
            # Validate and format the date
            date_obj = datetime.strptime(f"{full_year}-{month.zfill(2)}-{day.zfill(2)}", "%Y-%m-%d")
            date_str = date_obj.strftime("%Y-%m-%d")
            logger.debug(f"Extracted date (range): {date_str}")
            return date_str
        except ValueError:
            logger.warning(f"Invalid date extracted: {month}/{day}/{year}")
    
    return None

def normalize_patient_name(name: str) -> str:
    """
    Normalize patient name for comparison by removing special characters and standardizing format.
    
    Args:
        name: Patient name to normalize
        
    Returns:
        Normalized name
    """
    if not name:
        return ""
    
    # Remove special characters, keep only letters and spaces
    normalized = re.sub(r'[^a-zA-Z\s]', '', name.upper()).strip()
    
    # Collapse multiple spaces to single space
    normalized = re.sub(r'\s+', ' ', normalized)
    
    return normalized

def compare_patient_names(name1: str, name2: str) -> bool:
    """
    Compare two patient names with fuzzy matching to handle variations.
    
    Args:
        name1: First patient name
        name2: Second patient name
        
    Returns:
        True if names are considered a match
    """
    if not name1 or not name2:
        return False
    
    # Normalize both names
    norm1 = normalize_patient_name(name1)
    norm2 = normalize_patient_name(name2)
    
    if not norm1 or not norm2:
        return False
    
    # Exact match
    if norm1 == norm2:
        return True
    
    # Check if one name contains the other (handles cases like "JOHN DOE" vs "DOE, JOHN")
    if norm1 in norm2 or norm2 in norm1:
        return True
    
    # Split names and check if all parts of shorter name are in longer name
    parts1 = norm1.split()
    parts2 = norm2.split()
    
    if len(parts1) <= len(parts2):
        shorter_parts = parts1
        longer_parts = parts2
    else:
        shorter_parts = parts2
        longer_parts = parts1
    
    # Check if all parts of shorter name appear in longer name
    if all(part in longer_parts for part in shorter_parts):
        return True
    
    return False

def find_patient_date_duplicates(historical_df: pd.DataFrame, current_patient: str, current_date: str) -> list:
    """
    Find potential duplicates based on patient name and service date.
    
    Args:
        historical_df: DataFrame containing historical data
        current_patient: Current patient name
        current_date: Current service date
        
    Returns:
        List of matching row indices
    """
    matches = []
    
    if not current_patient or not current_date:
        return matches
    
    logger.debug(f"Searching for patient+date duplicates: {current_patient} on {current_date}")
    
    for idx, row in historical_df.iterrows():
        hist_description = row.get('Description', '')
        hist_bill_date = row.get('Bill Date', '')
        
        # Extract patient name from historical description
        hist_patient = extract_patient_name_from_description(hist_description)
        
        # Extract date from historical description (fallback to Bill Date)
        hist_service_date = extract_date_from_description(hist_description)
        if not hist_service_date:
            # Try to use Bill Date as fallback
            if not pd.isna(hist_bill_date):
                hist_service_date = str(hist_bill_date)
                # Try to parse if it's in a different format
                try:
                    if '/' in hist_service_date:
                        date_obj = datetime.strptime(hist_service_date, "%m/%d/%Y")
                        hist_service_date = date_obj.strftime("%Y-%m-%d")
                except ValueError:
                    continue
        
        # Compare names and dates
        if hist_patient and hist_service_date:
            name_match = compare_patient_names(current_patient, hist_patient)
            date_match = current_date == hist_service_date
            
            if name_match and date_match:
                logger.info(f"Found patient+date match at row {idx}:")
                logger.info(f"  Historical: {hist_patient} on {hist_service_date}")
                logger.info(f"  Current: {current_patient} on {current_date}")
                matches.append(idx)
    
    return matches

# Test function
def test_patient_extraction():
    """Test function to validate patient name extraction."""
    test_cases = [
    ]
    
    print("Testing patient name extraction:")
    print("=" * 80)
    
    for description, expected in test_cases:
        result = extract_patient_name_from_description(description)
        date_result = extract_date_from_description(description)
        
        print(f"Description: {description}")
        print(f"Expected patient: {expected}")
        print(f"Extracted patient: {result}")
        print(f"Extracted date: {date_result}")
        print(f"Match: {'✓' if result == expected else '✗'}")
        print("-" * 80)

if __name__ == "__main__":
    test_patient_extraction()