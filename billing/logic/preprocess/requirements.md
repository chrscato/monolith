# Provider Bill Preprocessing Requirements

## Overview
The preprocessing pipeline processes medical provider bills (CMS-1500 forms) through several stages:
1. OCR - Extract text from PDFs
2. LLM - Structure the extracted text
3. Validate - Validate the structured data
4. Map - Map to orders

## Input Requirements

### Initial Input (PDF)
- Format: PDF files containing CMS-1500 medical claim forms
- Location: S3 bucket under `data/ProviderBills/pdf/`
- Naming: Files should be named with a unique ProviderBill ID (e.g., `{provider_bill_id}.pdf`)

### OCR Output (JSON)
```json
{
    "provider_bill_id": "string",
    "ocr_text": "string",
    "processed_at": "ISO8601 datetime"
}
```

### LLM Output (JSON)
```json
{
    "provider_bill_id": "string",
    "processed_at": "ISO8601 datetime",
    "patient_info": {
        "patient_name": "string",
        "patient_dob": "string",
        "patient_zip": "string"
    },
    "billing_info": {
        "billing_provider_name": "string",
        "billing_provider_address": "string",
        "billing_provider_tin": "string",
        "billing_provider_npi": "string",
        "total_charge": "string (format: $XX.XX)",
        "patient_account_no": "string"
    },
    "service_lines": [
        {
            "cpt_code": "string (5 digits)",
            "modifiers": ["string"],
            "units": "number",
            "charge_amount": "string (format: $XX.XX)",
            "date_of_service": "string (format: MM/DD/YY)",
            "place_of_service": "string",
            "diagnosis_pointer": "string"
        }
    ]
}
```

## Validation Requirements

### Required Fields
1. Patient Information:
   - `patient_name` (required)
   - `patient_dob` (optional)
   - `patient_zip` (optional)

2. Billing Information:
   - `total_charge` (required)
   - `billing_provider_name` (optional)
   - `billing_provider_address` (optional)
   - `billing_provider_tin` (optional)
   - `billing_provider_npi` (optional)
   - `patient_account_no` (optional)

3. Service Lines:
   - At least one service line is required
   - Each service line must have:
     - `cpt_code` (5 digits)
     - `charge_amount` (positive number)
     - `date_of_service` (valid date in MM/DD/YY format, not in future)
     - `units` (number)
     - `modifiers` (optional array)
     - `place_of_service` (optional)
     - `diagnosis_pointer` (optional)

### Validation Rules
1. Total charge must match sum of line item charges (within $10.00 tolerance)
2. No future dates of service allowed
3. CPT codes must be 5 digits
4. Charge amounts must be positive numbers
5. Date of service must be in MM/DD/YY format

## Data Flow

1. **OCR Stage**
   - Input: PDF files in S3 `data/ProviderBills/pdf/`
   - Process: Google Vision OCR
   - Output: JSON files in S3 `data/ProviderBills/txt/`
   - Archive: Processed PDFs moved to `data/ProviderBills/pdf/archive/`

2. **LLM Stage**
   - Input: OCR JSON files from `data/ProviderBills/txt/`
   - Process: GPT-4.1-mini extraction
   - Output: Structured JSON in S3 `data/ProviderBills/json/`
   - Archive: Processed OCR files moved to `data/ProviderBills/txt/archive/`

3. **Validation Stage**
   - Input: Database records from LLM stage
   - Process: Validation checks
   - Output: Updated database records with status and action fields
   - Status values: 'VALID', 'INVALID'
   - Action values: 'to_map', 'to_validate'

4. **Mapping Stage**
   - Input: Validated database records
   - Process: Map to orders
   - Output: Updated database records with mapped order information

## Error Handling

1. **OCR Errors**
   - Logged to `logs/ocr_errors.log`
   - Failed PDFs remain in input directory

2. **LLM Errors**
   - Logged to `logs/extract_errors.log`
   - Failed OCR files remain in input directory

3. **Validation Errors**
   - Stored in database `last_error` field
   - Status set to 'INVALID'
   - Action set to 'to_validate'

## Database Schema

### ProviderBill Table
```sql
CREATE TABLE ProviderBill (
    id TEXT PRIMARY KEY,
    status TEXT,
    action TEXT,
    last_error TEXT,
    patient_name TEXT,
    patient_dob TEXT,
    patient_zip TEXT,
    billing_provider_name TEXT,
    billing_provider_address TEXT,
    billing_provider_tin TEXT,
    billing_provider_npi TEXT,
    total_charge REAL,
    patient_account_no TEXT
);
```

### BillLineItem Table
```sql
CREATE TABLE BillLineItem (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_bill_id TEXT,
    cpt_code TEXT,
    modifier TEXT,
    units INTEGER,
    charge_amount REAL,
    allowed_amount REAL,
    decision TEXT,
    reason_code TEXT,
    date_of_service TEXT,
    place_of_service TEXT,
    diagnosis_pointer TEXT,
    FOREIGN KEY (provider_bill_id) REFERENCES ProviderBill(id)
);
```

## Environment Requirements

1. Required Environment Variables:
   - `S3_BUCKET`: S3 bucket name
   - `OPENAI_API_KEY`: OpenAI API key
   - Google Cloud credentials file (`googlecloud.json`)

2. Required Python Packages:
   - boto3
   - google-cloud-vision
   - openai
   - python-dotenv
   - sqlite3

## Notes for Data Preparation

1. PDF files should be:
   - Clear, readable scans of CMS-1500 forms
   - Named with unique ProviderBill IDs
   - Uploaded to the correct S3 prefix

2. When preparing data for preprocessing:
   - Ensure all required fields are present and legible
   - Verify date formats are correct
   - Check that charge amounts are reasonable
   - Validate CPT codes are 5 digits
   - Ensure total charges match line item sums 