#!/usr/bin/env python3
"""
split_hcfa_batch.py

Splits multi-page HCFA batch PDFs from billing/data/billbatch into single-page PDFs,
creates ProviderBill entries in the database,
and uploads them to S3 with ProviderBill ID naming format.
"""
import os
import sys
import logging
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from PyPDF2 import PdfReader, PdfWriter
from dotenv import load_dotenv
import sqlite3

# Get the project root directory
project_root = Path(__file__).resolve().parents[2]
sys.path.append(str(project_root))

# Load environment variables from .env
load_dotenv(project_root / '.env')

# Import S3 helper functions
from config.s3_utils import upload

# Constants
INPUT_DIR = project_root / 'billing' / 'data' / 'billbatch'
OUTPUT_PREFIX = 'data/ProviderBills/pdf/'
S3_BUCKET = os.getenv('S3_BUCKET', 'bill-review-prod')

def create_provider_bill_entry(source_file: str) -> str:
    """Create a new entry in the ProviderBill table and return its ID."""
    db_path = project_root / 'monolith.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Generate a UUID for the ID
        bill_id = str(uuid.uuid4())
        now = datetime.now().isoformat()
        
        cursor.execute(
            """
            INSERT INTO ProviderBill (
                id, claim_id, source_file, status, created_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (bill_id, None, source_file, 'RECEIVED', now)
        )
        conn.commit()
        return bill_id
    except sqlite3.Error as e:
        logger.error(f"Database error creating ProviderBill entry: {str(e)}")
        raise
    finally:
        conn.close()

def split_and_upload(pdf_path: Path):
    """Split a batch PDF into pages, create ProviderBill entries, and upload to S3."""
    logger = logging.getLogger("Split HCFA")
    logger.info(f"Processing {pdf_path}")

    try:
        # Read and split PDF pages
        reader = PdfReader(str(pdf_path))
        for page_idx, page in enumerate(reader.pages, start=1):
            # Create a new ProviderBill entry for each page
            source_file = f"{pdf_path.name}_page_{page_idx}"
            bill_id = create_provider_bill_entry(source_file)
            
            writer = PdfWriter()
            writer.add_page(page)

            # Write page to a temporary file
            local_out = Path(tempfile.mktemp(suffix=".pdf"))
            with open(local_out, "wb") as f:
                writer.write(f)

            # Create filename with ProviderBill ID
            output_filename = f"{bill_id}.pdf"
            s3_key = f"{OUTPUT_PREFIX}{output_filename}"
            
            # Upload to S3
            if not upload(str(local_out), s3_key, bucket=S3_BUCKET):
                raise Exception(f"Failed to upload {s3_key} to S3")
            logger.info(f"Uploaded {s3_key}")

            # Clean up local page file
            local_out.unlink()

    except Exception as e:
        logger.error(f"Error processing {pdf_path}: {str(e)}", exc_info=True)
        raise

def process_batch_files():
    """Process all batch PDFs in the input directory."""
    logger = logging.getLogger("Split HCFA")
    try:
        # Ensure input directory exists
        if not INPUT_DIR.exists():
            logger.error(f"Input directory {INPUT_DIR} does not exist")
            return

        # Get all PDF files in the input directory
        pdf_files = list(INPUT_DIR.glob('*.pdf'))
        
        if not pdf_files:
            logger.warning(f"No PDF batches found to process in {INPUT_DIR}")
            return
            
        logger.info(f"Found {len(pdf_files)} PDF batches to process:")
        for pdf_file in pdf_files:
            logger.info(f"  - {pdf_file}")
            
        for pdf_file in pdf_files:
            split_and_upload(pdf_file)
        logger.info("All batches processed.")
    except Exception as e:
        logger.error(f"Error in batch processing: {str(e)}", exc_info=True)
        raise

def main():
    # Setup basic logging when run directly
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    process_batch_files()

if __name__ == '__main__':
    main()
