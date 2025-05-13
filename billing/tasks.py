# billing/tasks.py

from celery import shared_task
from billing.jobs.run_pipeline import process_bill

@shared_task
def run_bill_pipeline(bill_id):
    result = process_bill(bill_id)
    print(f"Processed bill {bill_id}: {result['status']}")
    return result
