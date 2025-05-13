"""
Views for the providers app.
"""
from django.shortcuts import render, get_object_or_404
from django.contrib.gis.geos import Point
from django.contrib.gis.db.models.functions import Distance
from .models import Provider

def provider_list(request):
    """List all providers."""
    providers = Provider.objects.all()
    return render(request, 'providers/list.html', {'providers': providers})

def provider_detail(request, pk):
    """Show details for a specific provider."""
    provider = get_object_or_404(Provider, pk=pk)
    return render(request, 'providers/detail.html', {'provider': provider})

def find_nearest(request):
    """Find the nearest providers to a given location."""
    lat = request.GET.get('lat')
    lng = request.GET.get('lng')
    radius = request.GET.get('radius', 10)  # Default 10km radius
    
    if not (lat and lng):
        return render(request, 'providers/nearest.html', {'error': 'Missing coordinates'})
    
    point = Point(float(lng), float(lat), srid=4326)
    providers = Provider.objects.filter(
        location__distance_lte=(point, radius * 1000)  # Convert km to meters
    ).annotate(
        distance=Distance('location', point)
    ).order_by('distance')
    
    return render(request, 'providers/nearest.html', {'providers': providers}) 