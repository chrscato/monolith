#!/usr/bin/env python3
"""
ocr_hcfa.py

Fetches ProviderBill PDFs from S3, runs OCR via Google Vision,
writes extracted text as JSON back to S3, and archives processed PDFs.
"""
import os
import sys
import logging
import tempfile
import json
from pathlib import Path
from dotenv import load_dotenv
import boto3
from google.cloud import vision
from google.cloud.vision_v1 import types
from datetime import datetime

# Get the project root directory (3 levels up from this file)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

# Load environment variables from the root .env file
load_dotenv(PROJECT_ROOT / '.env')

# Set credentials path relative to project root
credentials_path = PROJECT_ROOT / 'config' / 'googlecloud.json'
print(f"Looking for credentials at: {credentials_path}")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(credentials_path)

# Import S3 helper functions
from config.s3_utils import list_objects, download, upload, move

# Initialize Vision API client
vision_client = vision.ImageAnnotatorClient()

# S3 prefixes
INPUT_PREFIX = 'data/ProviderBills/pdf/'
ARCHIVE_PREFIX = 'data/ProviderBills/pdf/archive/'
OUTPUT_PREFIX = 'data/ProviderBills/txt/'
LOG_PREFIX = 'logs/ocr_errors.log'
S3_BUCKET = os.getenv('S3_BUCKET', 'bill-review-prod')


def ocr_pdf_with_vision(local_pdf_path: str) -> str:
    """Run Google Vision Document Text Detection on the PDF file."""
    with open(local_pdf_path, 'rb') as f:
        content = f.read()

    input_config = types.InputConfig(
        content=content,
        mime_type='application/pdf'
    )
    feature = types.Feature(
        type_=types.Feature.Type.DOCUMENT_TEXT_DETECTION
    )
    request = types.AnnotateFileRequest(
        input_config=input_config,
        features=[feature]
    )

    response = vision_client.batch_annotate_files(requests=[request])
    texts = []
    for file_resp in response.responses:
        for page_resp in file_resp.responses:
            if page_resp.full_text_annotation:
                texts.append(page_resp.full_text_annotation.text)
    return "\n".join(texts)


def process_ocr_s3():
    """Process PDFs with OCR, save JSON output, and archive processed PDFs."""
    logger = logging.getLogger("OCR Processing")
    
    # List all PDFs in source folder (excluding archived)
    pdf_keys = [key for key in list_objects(INPUT_PREFIX) 
                if key.lower().endswith('.pdf') 
                and not key.startswith(ARCHIVE_PREFIX)]
    
    if not pdf_keys:
        logger.info("No PDFs found to process")
        return

    logger.info(f"Found {len(pdf_keys)} PDFs to process")
    
    for key in pdf_keys:
        pdf_name = Path(key).name
        logger.info(f"Processing {pdf_name}")
        
        try:
            # Create temp directory for processing
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # Download PDF
                local_pdf = temp_path / pdf_name
                download(key, str(local_pdf))
                
                # Perform OCR
                extracted = ocr_pdf_with_vision(str(local_pdf))
                
                # Create JSON structure
                base_name = local_pdf.stem  # This will be the ProviderBill ID
                json_data = {
                    "provider_bill_id": base_name,
                    "ocr_text": extracted,
                    "processed_at": datetime.now().isoformat()
                }
                
                # Write JSON locally
                local_json = temp_path / f"{base_name}.json"
                with open(local_json, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, indent=2)

                # Upload JSON to S3
                s3_json_key = f"{OUTPUT_PREFIX}{base_name}.json"
                upload(str(local_json), s3_json_key)
                logger.info(f"Saved OCR JSON: {s3_json_key}")

                # Move processed PDF to archived folder
                archive_key = f"{ARCHIVE_PREFIX}{pdf_name}"
                move(key, archive_key)
                logger.info(f"Archived PDF to: {archive_key}")

        except Exception as e:
            logger.error(f"Error processing {pdf_name}: {str(e)}", exc_info=True)
            # Write error to log file
            log_local = temp_path / "error.log"
            with open(log_local, 'w', encoding='utf-8') as logf:
                logf.write(f"Error OCR {key}: {str(e)}\n")
            upload(str(log_local), LOG_PREFIX)

    logger.info("OCR processing complete")


if __name__ == '__main__':
    # Setup basic logging when run directly
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    process_ocr_s3()
