"""
Geospatial services for the intake portal.
"""
from typing import List, Dict, Any
from django.contrib.gis.geos import Point
from django.contrib.gis.db.models.functions import Distance
from providers.models import Provider

def find_providers_in_radius(
    point: Point,
    radius_km: float,
    limit: int = 10
) -> List[Dict[str, Any]]:
    """
    Find providers within a given radius of a point.
    
    Args:
        point: The center point to search from
        radius_km: The radius in kilometers
        limit: Maximum number of results to return
        
    Returns:
        List of provider dictionaries with distance information
    """
    providers = Provider.objects.filter(
        location__distance_lte=(point, radius_km * 1000)  # Convert km to meters
    ).annotate(
        distance=Distance('location', point)
    ).order_by('distance')[:limit]
    
    return [
        {
            'id': p.id,
            'name': p.name,
            'address': p.address,
            'phone': p.phone,
            'email': p.email,
            'specialties': p.specialties,
            'distance_km': float(p.distance.km)  # Convert to float for JSON serialization
        }
        for p in providers
    ] 