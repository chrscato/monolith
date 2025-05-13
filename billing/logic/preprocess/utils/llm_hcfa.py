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

# Get the project root directory (2 levels up from this file)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

# Load environment variables from the root .env file
load_dotenv(PROJECT_ROOT / '.env')

# Import S3 helper functions
from config.s3_utils import list_objects, download, upload, move

# S3 prefixes
INPUT_PREFIX = 'data/ProviderBills/txt/'
OUTPUT_PREFIX = 'data/ProviderBills/json/'
ARCHIVE_PREFIX = 'data/ProviderBills/txt/archive/'
LOG_PREFIX = 'logs/extract_errors.log'
S3_BUCKET = os.getenv('S3_BUCKET', 'bill-review-prod')

# Load prompt from project root
PROMPT_PATH = PROJECT_ROOT / 'preprocess' / 'utils' / 'gpt41_prompt.txt'

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
    db_path = PROJECT_ROOT / 'monolith.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Extract patient and billing info
        patient_info = extracted_data.get('patient_info', {})
        billing_info = extracted_data.get('billing_info', {})
        
        # Convert total charge from string to float if present
        total_charge = None
        if 'total_charge' in billing_info:
            total_charge = float(billing_info['total_charge'].replace('$', '').replace(',', ''))
        
        # Update ProviderBill record with all fields
        cursor.execute(
            """
            UPDATE ProviderBill 
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
                patient_account_no = ?
            WHERE id = ?
            """,
            (
                'REVIEWED',
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
                INSERT INTO BillLineItem (
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
        
    except sqlite3.Error as e:
        conn.rollback()
        print(f"Database error updating ProviderBill {provider_bill_id}: {str(e)}")
        return False
    finally:
        conn.close()


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
        local_json = download(key, os.path.join(tempfile.gettempdir(), os.path.basename(key)))
        output_json = None
        try:
            # Read the OCR JSON file
            with open(local_json, 'r', encoding='utf-8') as f:
                ocr_data = json.load(f)
            
            # Extract the OCR text
            ocr_text = ocr_data.get('ocr_text', '')
            provider_bill_id = ocr_data.get('provider_bill_id', '')
            
            # Process with LLM
            raw_output = extract_data_via_llm(prompt, ocr_text)
            cleaned = clean_gpt_output(raw_output)

            if not cleaned.startswith('{'):
                raise ValueError('LLM output not JSON')

            # Parse and clean the extracted data
            parsed = json.loads(cleaned)
            parsed = fix_all_charges(parsed)
            
            # Add metadata
            parsed['provider_bill_id'] = provider_bill_id
            parsed['processed_at'] = datetime.now().isoformat()

            # Update database record
            if not update_provider_bill_record(provider_bill_id, parsed):
                raise Exception("Failed to update database record")

            # Print JSON structure for debugging
            print("\nJSON Structure:")
            print(json.dumps(parsed, indent=2))
            print("\nChecking required fields:")
            print(f"patient_info.patient_name: {parsed.get('patient_info', {}).get('patient_name', 'MISSING')}")
            service_lines = parsed.get('service_lines', [])
            print(f"service_lines[0].date_of_service: {service_lines[0].get('date_of_service', 'MISSING') if service_lines else 'NO SERVICE LINES'}")

            # Write JSON locally
            output_json = tempfile.mktemp(suffix='.json')
            with open(output_json, 'w', encoding='utf-8') as jf:
                json.dump(parsed, jf, indent=4)

            # Upload JSON to S3
            base = os.path.splitext(os.path.basename(key))[0]
            s3_json_key = f"{OUTPUT_PREFIX}{base}.json"
            upload(output_json, s3_json_key)
            print(f"✔ Uploaded JSON to s3://{S3_BUCKET}/{s3_json_key}")

            # Archive original text
            archived_key = key.replace(INPUT_PREFIX, ARCHIVE_PREFIX)
            move(key, archived_key)
            print(f"✔ Archived text to s3://{S3_BUCKET}/{archived_key}\n")

        except Exception as e:
            err = f"❌ Extraction error {key}: {e}"
            print(err)
            log_local = tempfile.mktemp(suffix='.log')
            with open(log_local, 'a', encoding='utf-8') as logf:
                logf.write(err + '\n')
            upload(log_local, LOG_PREFIX)
            os.remove(log_local)

        finally:
            if os.path.exists(local_json):
                os.remove(local_json)
            if output_json and os.path.exists(output_json):
                os.remove(output_json)

    print("LLM extraction complete.")


if __name__ == '__main__':
    process_llm_s3()
