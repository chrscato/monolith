# Celery configuration for the cdx_ehr project.

import os
from celery import Celery

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'cdx_ehr.settings')

app = Celery('cdx_ehr')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()
