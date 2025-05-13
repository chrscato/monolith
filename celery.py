# cdx_crm/celery.py (put this next to settings.py)

import os
from celery import Celery

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'cdx_crm.settings')

app = Celery('cdx_crm')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()
