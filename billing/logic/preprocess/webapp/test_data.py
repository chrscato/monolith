from app import app, db, ProviderBill, LineItem
from datetime import datetime, timedelta
import json
import random

def generate_test_data():
    """Generate test data for the billing failure review system."""
    with app.app_context():
        # Clear existing data
        db.drop_all()
        db.create_all()

        # Sample failure reasons
        failure_reasons = [
            "Invalid provider information",
            "Missing required fields",
            "Amount mismatch with line items",
            "Duplicate bill number",
            "Invalid date format",
            "Missing authorization",
            "Invalid CPT code",
            "Missing diagnosis code"
        ]

        # Sample line item descriptions
        line_item_descriptions = [
            "Office Visit - New Patient",
            "Office Visit - Established Patient",
            "Physical Therapy - Initial Evaluation",
            "Physical Therapy - Follow-up",
            "X-Ray - Chest",
            "MRI - Knee",
            "Blood Work - Complete Panel",
            "Vaccination - Flu Shot"
        ]

        # Create 10 test bills
        for i in range(1, 11):
            # Generate random dates within the last 30 days
            created_at = datetime.utcnow() - timedelta(days=random.randint(0, 30))
            updated_at = created_at + timedelta(hours=random.randint(1, 24))

            # Create the bill
            bill = ProviderBill(
                bill_number=f"BILL-{i:04d}",
                status='failed',
                created_at=created_at,
                updated_at=updated_at,
                failure_reason=random.choice(failure_reasons),
                raw_data=json.dumps({
                    "provider": {
                        "name": f"Dr. John Smith {i}",
                        "npi": f"123456789{i}",
                        "tax_id": f"12-345678{i}"
                    },
                    "patient": {
                        "name": f"Jane Doe {i}",
                        "dob": "1980-01-01",
                        "insurance": "Blue Cross Blue Shield"
                    },
                    "service_date": created_at.strftime("%Y-%m-%d"),
                    "total_amount": random.randint(100, 1000)
                })
            )
            db.session.add(bill)
            db.session.flush()  # Flush to get the bill ID

            # Add 2-5 line items to each bill
            num_line_items = random.randint(2, 5)
            for j in range(num_line_items):
                line_item = LineItem(
                    provider_bill_id=bill.id,
                    description=random.choice(line_item_descriptions),
                    amount=random.randint(50, 500),
                    status='pending',
                    created_at=created_at
                )
                db.session.add(line_item)

        # Commit all changes
        db.session.commit()

if __name__ == '__main__':
    generate_test_data()
    print("Test data generated successfully!") 