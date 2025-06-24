#!/usr/bin/env python3
"""
llm_hcfa.py

Processes OCR text from ProviderBills through LLM to extract structured data.
"""
import os
import sys
import logging
import tempfile
import json
import sqlite3
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI
from datetime import datetime

# Get the project root directory (2 levels up from this file) for imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

# Get the absolute path to the monolith root directory
DB_ROOT = Path(r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\monolith")

# Load environment variables from the root .env file
load_dotenv(DB_ROOT / '.env')

# Import S3 helper functions
from config.s3_utils import list_objects, download, upload, move

# S3 prefixes
INPUT_PREFIX = 'data/ProviderBills/txt/'
OUTPUT_PREFIX = 'data/ProviderBills/json/'
ARCHIVE_PREFIX = 'data/ProviderBills/txt/archive/'
LOG_PREFIX = 'logs/extract_errors.log'
S3_BUCKET = os.getenv('S3_BUCKET', 'bill-review-prod')

# Load prompt from utils directory
PROMPT_PATH = Path(__file__).resolve().parent / 'gpt41_prompt.txt'

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

# Helper to clean charge strings
def clean_charge(charge: str) -> str:
    try:
        if not charge.startswith("$"):
            return charge
        amount = float(charge.replace("$", "").replace(",", ""))
        if amount >= 10000:
            for divisor in (100, 10):
                candidate = amount / divisor
                if 10 <= candidate <= 5000:
                    return f"${candidate:.2f}"
        return f"${amount:.2f}"
    except:
        return charge


def fix_all_charges(data: dict) -> dict:
    if 'service_lines' in data:
        for line in data['service_lines']:
            if 'charge_amount' in line:
                line['charge_amount'] = clean_charge(line['charge_amount'])
    if 'billing_info' in data and 'total_charge' in data['billing_info']:
        data['billing_info']['total_charge'] = clean_charge(data['billing_info']['total_charge'])
    return data


def extract_data_via_llm(prompt_text: str, ocr_text: str) -> str:
    messages = [
        {"role": "system", "content": "You are an AI assistant that extracts structured data from CMS-1500 medical claim forms."},
        {"role": "user", "content": prompt_text + "\n---\n" + ocr_text}
    ]
    response = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=messages,
        temperature=0.0,
        max_tokens=2000
    )
    return response.choices[0].message.content


def clean_gpt_output(raw: str) -> str:
    txt = raw.strip()
    if txt.startswith("```json") and txt.endswith("```"):
        txt = txt[len("```json"): -len("```")]
    return txt.strip()


def update_provider_bill_record(provider_bill_id: str, extracted_data: dict) -> bool:
    """Update ProviderBill record and create BillLineItem entries in the database."""
    # Use the absolute path to monolith.db
    db_path = DB_ROOT / 'monolith.db'
    print(f"Connecting to database at: {db_path}")
    
    # Add retry logic for database lock issues
    max_retries = 3
    retry_delay = 1  # seconds
    
    for attempt in range(max_retries):
        try:
            # Add timeout to handle database locks
            conn = sqlite3.connect(db_path, timeout=30.0)
            cursor = conn.cursor()
            
            # Extract patient and billing info
            patient_info = extracted_data.get('patient_info', {})
            billing_info = extracted_data.get('billing_info', {})
            
            # Convert total charge from string to float if present
            total_charge = None
            if 'total_charge' in billing_info:
                total_charge = float(billing_info['total_charge'].replace('$', '').replace(',', ''))
            
            # First check if the record exists
            cursor.execute("SELECT id FROM ProviderBill WHERE id = ?", (provider_bill_id,))
            if not cursor.fetchone():
                print(f"Record {provider_bill_id} not found in ProviderBill table")
                return False
            
            # Update ProviderBill record with all fields
            cursor.execute(
                """
                UPDATE "ProviderBill" 
                SET status = ?,
                    last_error = NULL,
                    patient_name = ?,
                    patient_dob = ?,
                    patient_zip = ?,
                    billing_provider_name = ?,
                    billing_provider_address = ?,
                    billing_provider_tin = ?,
                    billing_provider_npi = ?,
                    total_charge = ?,
                    patient_account_no = ?,
                    action = NULL,
                    bill_paid = 'N'
                WHERE id = ?
                """,
                (
                    'RECEIVED',
                    patient_info.get('patient_name'),
                    patient_info.get('patient_dob'),
                    patient_info.get('patient_zip'),
                    billing_info.get('billing_provider_name'),
                    billing_info.get('billing_provider_address'),
                    billing_info.get('billing_provider_tin'),
                    billing_info.get('billing_provider_npi'),
                    total_charge,
                    billing_info.get('patient_account_no'),
                    provider_bill_id
                )
            )
            
            # Create BillLineItem entries for each service line
            for line in extracted_data.get('service_lines', []):
                # Convert charge amount from string to float
                charge_amount = float(line['charge_amount'].replace('$', '').replace(',', ''))
                
                # Join modifiers with comma if multiple
                modifiers = ','.join(line.get('modifiers', [])) if line.get('modifiers') else ''
                
                cursor.execute(
                    """
                    INSERT INTO "BillLineItem" (
                        provider_bill_id, cpt_code, modifier, units,
                        charge_amount, allowed_amount, decision,
                        reason_code, date_of_service, place_of_service,
                        diagnosis_pointer
                    ) VALUES (?, ?, ?, ?, ?, NULL, 'pending', '', ?, ?, ?)
                    """,
                    (
                        provider_bill_id,
                        line['cpt_code'],
                        modifiers,
                        line['units'],
                        charge_amount,
                        line['date_of_service'],
                        line.get('place_of_service'),
                        line.get('diagnosis_pointer')
                    )
                )
            
            conn.commit()
            return True
            
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries - 1:
                print(f"Database locked for {provider_bill_id}, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})")
                import time
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                continue
            else:
                conn.rollback()
                print(f"Database error updating ProviderBill {provider_bill_id}: {str(e)}")
                return False
        except sqlite3.Error as e:
            conn.rollback()
            print(f"Database error updating ProviderBill {provider_bill_id}: {str(e)}")
            return False
        finally:
            conn.close()
    
    return False


def process_llm_s3(limit=None):
    print(f"Starting LLM extraction run against bucket: {S3_BUCKET} (prefix: {INPUT_PREFIX})")
    # Load prompt template
    with open(PROMPT_PATH, 'r', encoding='utf-8') as pf:
        prompt = pf.read()

    # Get all JSON files from the input directory
    json_keys = [k for k in list_objects(INPUT_PREFIX) if k.lower().endswith('.json')]
    if limit:
        json_keys = json_keys[:int(limit)]

    for key in json_keys:
        print(f"→ Processing s3://{S3_BUCKET}/{key}")
        local_json = None
        temp_output = None
        try:
            # Download the file to a temporary location
            temp_dir = tempfile.gettempdir()
            local_json = os.path.join(temp_dir, os.path.basename(key))
            download(key, local_json)
            
            # Read the OCR JSON file
            with open(local_json, 'r', encoding='utf-8') as f:
                ocr_data = json.load(f)
            
            # Extract the OCR text
            ocr_text = ocr_data.get('ocr_text', '')
            provider_bill_id = ocr_data.get('provider_bill_id', '')
            
            # Process with LLM
            extracted_json = extract_data_via_llm(prompt, ocr_text)
            extracted_data = json.loads(clean_gpt_output(extracted_json))
            
            # Clean up charge amounts
            extracted_data = fix_all_charges(extracted_data)
            
            # Update database
            if update_provider_bill_record(provider_bill_id, extracted_data):
                # Save to S3
                output_key = f"{OUTPUT_PREFIX}{provider_bill_id}.json"
                temp_output = tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False)
                json.dump(extracted_data, temp_output, indent=2)
                temp_output.close()  # Close the file before uploading
                
                upload(temp_output.name, output_key)
                os.unlink(temp_output.name)  # Clean up temp file
                
                # Create local backup before archiving (if we have the original file)
                if local_json and os.path.exists(local_json):
                    backup_dir = Path("backup_ocr_files")
                    backup_dir.mkdir(exist_ok=True)
                    backup_path = backup_dir / f"{provider_bill_id}.json"
                    
                    try:
                        # Copy the local OCR JSON to backup location
                        import shutil
                        shutil.copy2(local_json, backup_path)
                        print(f" 📁 Local backup: {backup_path}")
                    except Exception as backup_exc:
                        print(f" ⚠ Local backup failed: {backup_exc}")
                
                # Archive the input file
                try:
                    archive_key = f"{ARCHIVE_PREFIX}{os.path.basename(key)}"
                    move(key, archive_key)
                    print(f"✓ Processed and archived: {key}")
                    
                    # If S3 archive succeeded, we can optionally remove local backup
                    # Uncomment the next line if you want to auto-cleanup successful backups
                    # backup_path.unlink(missing_ok=True)
                    
                except Exception as archive_exc:
                    print(f" ⚠ Archive failed: {archive_exc}")
                    if 'backup_path' in locals():
                        print(f" 💾 Local backup preserved at: {backup_path}")
                    # Log the archiving failure
                    err = tempfile.mktemp(suffix=".log")
                    with open(err, "w") as f:
                        f.write(f"{datetime.now()}: {key} – Archive failed: {archive_exc}\n")
                    upload(err, LOG_PREFIX)
            else:
                print(f"❌ Database update failed for: {key}")
                
        except Exception as e:
            print(f"❌ Extraction error {key}: {str(e)}")
            # Write error to log file
            temp_log = tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False)
            temp_log.write(f"Error processing {key}: {str(e)}\n")
            temp_log.close()  # Close the file before uploading
            
            upload(temp_log.name, LOG_PREFIX)
            os.unlink(temp_log.name)  # Clean up temp file
        finally:
            # Clean up the downloaded file
            if local_json and os.path.exists(local_json):
                try:
                    os.unlink(local_json)
                except Exception as e:
                    print(f"Warning: Could not delete temporary file {local_json}: {str(e)}")

    print("LLM processing complete")


if __name__ == '__main__':
    process_llm_s3()
