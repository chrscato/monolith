intake/
├── intake_portal/              # Django project
│   ├── settings.py
│   ├── urls.py
│   └── celery.py               # if you’re still using Celery
├── mail_ingest/                # your mail + AI apps
├── ai_extraction/
├── referrals/                  # patient & referral models + views
├── providers/                  # ← NEW: your in‑house provider directory
│   ├── migrations/
│   ├── management/             # for import/geocode commands
│   │   └── commands/
│   │       └── geocode_providers.py
│   ├── models.py
│   ├── admin.py
│   ├── views.py                # e.g. “find nearest” endpoint
│   ├── urls.py
│   └── serializers.py          # if you expose an API
├── mapping/                    # keeps pure geospatial logic
│   ├── services.py             # e.g. radius search
│   └── osm_client.py           # OSM/Nominatim wrapper
├── templates/
├── static/
└── manage.py
