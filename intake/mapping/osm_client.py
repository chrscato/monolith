"""
OpenStreetMap/Nominatim client for geocoding.
"""
import requests
import time
from typing import Optional, Dict

NOMINATIM_URL = 'https://nominatim.openstreetmap.org/search'

def geocode_address(address: str) -> Optional[Dict[str, float]]:
    """
    Geocode an address using OpenStreetMap's Nominatim service.
    
    Args:
        address: The address to geocode
        
    Returns:
        Dict containing 'lat' and 'lon' if successful, None otherwise
    """
    params = {
        'q': address,
        'format': 'json',
        'limit': 1
    }
    
    headers = {
        'User-Agent': 'IntakePortal/1.0'  # Required by Nominatim's terms of use
    }
    
    try:
        # Respect Nominatim's usage policy
        time.sleep(1)  # Minimum 1 second between requests
        
        response = requests.get(NOMINATIM_URL, params=params, headers=headers)
        response.raise_for_status()
        
        results = response.json()
        if results:
            return {
                'lat': float(results[0]['lat']),
                'lon': float(results[0]['lon'])
            }
        return None
        
    except (requests.RequestException, (KeyError, ValueError, IndexError)):
        return None 