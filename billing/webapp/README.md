# Bill Review Application

A Django application for reviewing and managing medical bills.

## Setup Instructions

1. Install required packages:
```bash
pip install django django-widget-tweaks
```

2. Navigate to the webapp directory:
```bash
cd billing/webapp
```

3. Run database migrations:
```bash
python manage.py migrate --run-syncdb
```

4. Start the development server:
```bash
python manage.py runserver
```

5. Access the application:
- Open your web browser and go to: http://localhost:8000
- The dashboard will show bills that need review, organized by status:
  - Flagged Bills
  - Error Bills
  - Arthrogram Bills

## Features

- View bills by status category
- Update bill status and details
- Manage line items
- Reset bills to MAPPED status for reprocessing
- Responsive interface with Bootstrap styling

## Database

The application uses an existing SQLite database (monolith.db) located in the parent directory. The database should contain the following tables:
- ProviderBill
- BillLineItem
- orders
- providers

## Development

- Templates are located in the `templates/bill_review/` directory
- Views and forms are in the `bill_review/` directory
- Static files (if any) should be placed in `static/` directory

## Error Handling

The application includes comprehensive error handling:
- Database errors are logged
- Empty states are handled gracefully
- User-friendly error messages are displayed
- Failed operations return to safe states 