"""
Management command to geocode provider addresses.
"""
from django.core.management.base import BaseCommand
from django.contrib.gis.geos import Point
from providers.models import Provider
from mapping.osm_client import geocode_address

class Command(BaseCommand):
    help = 'Geocode provider addresses using OSM/Nominatim'

    def add_arguments(self, parser):
        parser.add_argument(
            '--all',
            action='store_true',
            help='Geocode all providers without location',
        )
        parser.add_argument(
            '--provider-id',
            type=int,
            help='Geocode a specific provider by ID',
        )

    def handle(self, *args, **options):
        if options['provider_id']:
            providers = Provider.objects.filter(id=options['provider_id'])
        elif options['all']:
            providers = Provider.objects.filter(location__isnull=True)
        else:
            self.stdout.write(self.style.ERROR('Please specify --all or --provider-id'))
            return

        for provider in providers:
            try:
                location = geocode_address(provider.address)
                if location:
                    provider.location = Point(location['lon'], location['lat'], srid=4326)
                    provider.save()
                    self.stdout.write(
                        self.style.SUCCESS(f'Successfully geocoded provider {provider.name}')
                    )
                else:
                    self.stdout.write(
                        self.style.WARNING(f'Could not geocode provider {provider.name}')
                    )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'Error geocoding provider {provider.name}: {str(e)}')
                ) 