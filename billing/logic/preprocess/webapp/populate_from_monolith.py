import os
import sqlite3
from pathlib import Path
from datetime import datetime
import random
from app import app
from database import db, FailedBill, LineItem

# Set up paths
MONOLITH_DB = Path(r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\monolith\cdx_ehr\monolith.db")

def get_monolith_bills():
    """Get bills from monolith database that need review."""
    conn = sqlite3.connect(str(MONOLITH_DB))
    cursor = conn.cursor()
    
    query = """
    SELECT 
        pb.id as bill_id,
        pb.claim_id,
        pb.patient_name,
        pb.patient_dob,
        pb.billing_provider_name,
        pb.billing_provider_npi,
        pb.total_charge,
        pb.status,
        o.Order_ID,
        o.Referring_Physician,
        o.Referring_Physician_NPI,
        GROUP_CONCAT(bli.id) as line_item_ids,
        GROUP_CONCAT(bli.cpt_code) as cpt_codes,
        GROUP_CONCAT(bli.charge_amount) as charge_amounts,
        GROUP_CONCAT(bli.date_of_service) as service_dates
    FROM ProviderBill pb
    LEFT JOIN orders o ON pb.claim_id = o.Claim_Number
    LEFT JOIN BillLineItem bli ON pb.id = bli.provider_bill_id
    WHERE pb.status = 'MAPPED'
    GROUP BY pb.id
    LIMIT 50
    """
    
    cursor.execute(query)
    bills = cursor.fetchall()
    conn.close()
    return bills

def create_failure_scenarios():
    """Create realistic failure scenarios based on monolith data."""
    bills = get_monolith_bills()
    failure_types = [
        "missing_provider_info",
        "amount_mismatch",
        "invalid_cpt",
        "date_mismatch",
        "duplicate_bill"
    ]
    
    with app.app_context():
        for bill in bills:
            failure_type = random.choice(failure_types)
            
            # Create failed bill
            failed_bill = FailedBill(
                bill_id=bill[0],
                claim_id=bill[1],
                patient_name=bill[2],
                patient_dob=bill[3],
                provider_name=bill[4],
                provider_npi=bill[5],
                total_charge=bill[6],
                failure_type=failure_type,
                failure_details=generate_failure_details(failure_type, bill),
                status="pending",
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            db.session.add(failed_bill)
            
            # Create line items
            line_item_ids = bill[11].split(',') if bill[11] else []
            cpt_codes = bill[12].split(',') if bill[12] else []
            charge_amounts = bill[13].split(',') if bill[13] else []
            service_dates = bill[14].split(',') if bill[14] else []
            
            for i in range(len(line_item_ids)):
                line_item = LineItem(
                    bill_id=bill[0],
                    line_item_id=line_item_ids[i],
                    cpt_code=cpt_codes[i],
                    charge_amount=float(charge_amounts[i]),
                    date_of_service=service_dates[i],
                    status="pending"
                )
                db.session.add(line_item)
        
        db.session.commit()

def generate_failure_details(failure_type, bill):
    """Generate realistic failure details based on the type."""
    details = {
        "missing_provider_info": f"Provider NPI {bill[5]} not found in our records",
        "amount_mismatch": f"Total charge {bill[6]} exceeds expected amount by 20%",
        "invalid_cpt": "One or more CPT codes are invalid or expired",
        "date_mismatch": "Service dates do not match order dates",
        "duplicate_bill": f"Duplicate bill found for claim {bill[1]}"
    }
    return details.get(failure_type, "Unknown failure type")

def main():
    """Main function to populate the database."""
    print("Starting database population...")
    create_failure_scenarios()
    print("Database population complete!")

if __name__ == "__main__":
    main() 