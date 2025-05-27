import re
from datetime import datetime
from difflib import SequenceMatcher
from typing import Optional

def clean_name(name: str) -> str:
    if not name:
        return ""
    name = name.lower().replace(",", "").strip()
    name = re.sub(r'[^a-z\s-]', '', name)
    suffixes = ['jr', 'sr', 'ii', 'iii', 'iv', 'v', 'phd', 'md', 'do']
    for sfx in suffixes:
        name = re.sub(rf'\b{sfx}\b', '', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip()

def normalize_date(date_str: str) -> Optional[datetime.date]:
    if not date_str:
        return None
    date_str = str(date_str).strip()
    if ' - ' in date_str:
        date_str = date_str.split(' - ')[0].strip()
    if ' ' in date_str:
        date_str = date_str.split(' ')[0]
    formats = [
        '%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y',
        '%Y/%m/%d', '%m/%d/%y', '%m-%d-%y',
        '%Y%m%d', '%m%d%Y', '%m%d%y'
    ]
    for fmt in formats:
        try:
            d = datetime.strptime(date_str, fmt).date()
            if 2020 <= d.year <= 2035:
                return d
        except ValueError:
            continue
    return None

def similar(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()

def extract_last_name(full_name: str) -> str:
    if not full_name:
        return ""
    
    # Handle "Last, First" format
    if ',' in full_name:
        last_name = full_name.split(',')[0].strip()
    else:
        # Handle "First Last" format
        parts = full_name.split()
        last_name = parts[-1] if parts else ""
    
    return clean_name(last_name) 