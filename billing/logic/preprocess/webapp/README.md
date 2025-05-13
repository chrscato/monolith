# Billing Failure Review Web Application

A web application for reviewing and managing billing failures in the preprocessing workflow.

## Features

- View list of failed bills
- Review detailed information about each bill
- Retry processing of failed bills
- Override validation and approve bills
- Reject bills with reason
- Add/remove line items
- View raw data from the database

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run the application:
```bash
# Development environment (default)
python app.py

# Test environment with monolith data
set FLASK_ENV=testing  # On Windows
python populate_from_monolith.py  # Populate with real data from monolith
python app.py

# Production environment
set FLASK_ENV=production  # On Windows
python app.py
```

The application will be available at http://localhost:5000

## Environments

The application supports three environments:

1. **Development** (default)
   - Uses `billing_failures_dev.db`
   - Debug mode enabled
   - Suitable for local development

2. **Testing**
   - Uses `billing_failures_test.db`
   - Can be populated with:
     - Sample test data (`test_data.py`)
     - Real data from monolith (`populate_from_monolith.py`)
   - Perfect for testing and demonstration

3. **Production**
   - Uses `billing_failures.db` or `DATABASE_URL` environment variable
   - Debug mode disabled
   - Configure with environment variables:
     - `DATABASE_URL`: Database connection string
     - `SECRET_KEY`: Application secret key

## Data Sources

The application can be populated with data from two sources:

1. **Test Data** (`test_data.py`)
   - Generates synthetic data
   - 10 failed bills with random failure reasons
   - 2-5 line items per bill
   - Realistic provider and patient information

2. **Monolith Data** (`populate_from_monolith.py`)
   - Copies real data from the monolith database
   - Transforms pending bills into failed bills
   - Creates realistic failure scenarios:
     - Missing provider information
     - Amount mismatches
     - Invalid CPT codes
     - Date mismatches
     - Duplicate bills
   - Preserves original line items and amounts

To use monolith data:
```bash
set FLASK_ENV=testing  # On Windows
python populate_from_monolith.py
```

## Usage

1. The main page shows a list of all failed bills
2. Click "Review" on any bill to see its details
3. On the bill detail page, you can:
   - Retry processing the bill
   - Override validation and approve the bill
   - Reject the bill with a reason
   - Add or remove line items
   - View the raw data from the database

## Development

To modify the application:

1. The main application logic is in `app.py`
2. Templates are in the `templates` directory
3. Database models are defined in `app.py`
4. Static files (CSS, JS) can be added to a `static` directory
5. Test data generation is in `test_data.py`
6. Monolith data population is in `populate_from_monolith.py`

## Database

The application uses SQLite as its database. The database file (`billing_failures.db`) will be created automatically when you first run the application. 