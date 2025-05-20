#!/usr/bin/env python3
"""
main.py

Orchestrates the entire preprocessing workflow for ProviderBills:
1. OCR - Extract text from PDFs
2. LLM - Structure the extracted text
3. Validate - Validate the structured data
4. Map - Map to orders
"""
import os
import sys
import logging
from pathlib import Path
from datetime import datetime

# Get the project root directory (3 levels up from this file)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

# Import preprocessing modules
from .utils.ocr_hcfa import process_ocr_s3 as process_ocr
from .utils.llm_hcfa import process_llm_s3 as process_llm
from .utils.validate_intake import process_validation
from .utils.map_bill import process_mapping

# Create logs directory if it doesn't exist
log_dir = PROJECT_ROOT / 'logs'
log_dir.mkdir(exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / f'preprocess_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_preprocessing():
    """
    Run the complete preprocessing workflow.
    Each step processes all available records in its input state.
    """
    try:
        # Step 1: OCR - Extract text from PDFs
        logger.info("Starting OCR processing...")
        process_ocr()
        logger.info("OCR processing complete")

        # Step 2: LLM - Structure the extracted text
        logger.info("Starting LLM processing...")
        process_llm()
        logger.info("LLM processing complete")

        # Step 3: Validate - Validate the structured data
        logger.info("Starting validation...")
        process_validation()
        logger.info("Validation complete")

        # Step 4: Map - Map to orders
        logger.info("Starting bill mapping...")
        process_mapping()
        logger.info("Bill mapping complete")

        logger.info("Preprocessing workflow complete")

    except Exception as e:
        logger.error(f"Error in preprocessing workflow: {str(e)}")
        raise

def main():
    """Main entry point for the preprocessing workflow."""
    try:
        logger.info("Starting preprocessing workflow")
        run_preprocessing()
        logger.info("Preprocessing workflow completed successfully")

    except Exception as e:
        logger.error(f"Fatal error in preprocessing workflow: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main() 