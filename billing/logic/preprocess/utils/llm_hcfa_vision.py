#!/usr/bin/env python3
"""
llm_hcfa.py  –  Vision-enabled HCFA-1500 extractor (prompt loaded from JSON)

• Renders each PDF page to JPEG (≈200 DPI, max 2200 px).
• Sends images + prompt to GPT-4o-mini via function-calling.
• Normalises charges, writes structured JSON to S3, updates SQLite.

Required:
    pip install pymupdf pillow openai python-dotenv
"""

from __future__ import annotations
import os, json, base64, tempfile, sqlite3, sys
from pathlib import Path
from datetime import datetime

import fitz                                 # PyMuPDF
from dotenv import load_dotenv
from openai import OpenAI


#   …/monolith/billing/logic/preprocess/utils/llm_hcfa_vision.py
# parents[0] = utils, [1] = preprocess, [2] = logic,
# [3] = billing, [4] = monolith   ← repo root that holds "config"
PROJECT_ROOT = Path(__file__).resolve().parents[4]
sys.path.append(str(PROJECT_ROOT))  

DB_ROOT   = PROJECT_ROOT / "billing"
PROMPT_FN = PROJECT_ROOT / "billing" / "prompts" / "gpt4o_prompt.json"

load_dotenv(PROJECT_ROOT / ".env")   # OPENAI_API_KEY, S3_BUCKET, etc.

# ─────────────────────────────────────────────────────────────────────────────
#  S3 helpers  –  keep your existing utils
# ─────────────────────────────────────────────────────────────────────────────
sys.path.insert(0, str(PROJECT_ROOT))
from config.s3_utils import list_objects, download, upload, move

INPUT_PREFIX   = "data/ProviderBills/pdf/"
OUTPUT_PREFIX  = "data/ProviderBills/json/"
ARCHIVE_PREFIX = "data/ProviderBills/pdf/archive/"
LOG_PREFIX     = "logs/extract_errors.log"
S3_BUCKET      = os.getenv("S3_BUCKET", "bill-review-prod")

# ─────────────────────────────────────────────────────────────────────────────
#  Load prompt JSON  (system, user_hint, functions)
# ─────────────────────────────────────────────────────────────────────────────
with open(PROMPT_FN, "r", encoding="utf-8") as f:
    _PROMPT_JSON = json.load(f)

SYSTEM_PROMPT = _PROMPT_JSON["system"]
USER_HINT     = _PROMPT_JSON["user_hint"]
FUNCTIONS     = _PROMPT_JSON["functions"]

# ─────────────────────────────────────────────────────────────────────────────
#  OpenAI client
# ─────────────────────────────────────────────────────────────────────────────
client       = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")    # override via ENV

# ─────────────────────────────────────────────────────────────────────────────
#  PDF → base64-JPEG helper
# ─────────────────────────────────────────────────────────────────────────────
def pdf_to_b64_images(pdf_path: str,
                      max_dim_px: int = 2200,
                      jpeg_q: int = 80) -> list[str]:
    """Render each PDF page to base64-encoded JPEG."""
    doc  = fitz.open(pdf_path)
    pages = []
    for pg in doc:
        pix = pg.get_pixmap(dpi=200)
        if pix.width > max_dim_px:               # downscale huge scans
            scale = max_dim_px / pix.width
            pix   = pg.get_pixmap(matrix=fitz.Matrix(scale, scale))
        pages.append(base64.b64encode(
            pix.tobytes("jpg", jpg_quality=jpeg_q)).decode())
    doc.close()
    return pages

# ─────────────────────────────────────────────────────────────────────────────
#  Charge clean-up helpers
# ─────────────────────────────────────────────────────────────────────────────
def _fix_charge(raw: str) -> str:
    if not raw:
        return raw
    raw = raw.replace(":", ".").replace(" ", "")
    try:
        val = float(raw.strip("$").replace(",", ""))
    except ValueError:
        return raw
    if val > 10_000:                             # inflation fix 195000 → 1950.00
        for div in (10, 100):
            cand = val / div
            if 10 <= cand <= 5_000:
                val = cand
                break
    return f"${val:.2f}"

def validate_cpt_code(cpt: str) -> str:
    """
    Validate and clean CPT codes to ensure they match the correct format.
    
    Args:
        cpt: Raw CPT code string
        
    Returns:
        Validated CPT code or 'unknown' if invalid
    """
    if not cpt or cpt == "unknown":
        return "unknown"
    
    # Remove whitespace and convert to uppercase
    cleaned = str(cpt).strip().upper()
    
    # Check for exactly 5 characters
    if len(cleaned) != 5:
        print(f"   ⚠ Invalid CPT length '{cpt}' (length {len(cleaned)})")
        return "unknown"
    
    # Check format: either all digits OR 1 letter + 4 digits
    if cleaned.isdigit():
        # All digits: 00000-99999
        return cleaned
    elif cleaned[0].isalpha() and cleaned[1:].isdigit():
        # 1 letter + 4 digits: A0000-Z9999
        return cleaned
    else:
        # Invalid format
        print(f"   ⚠ Invalid CPT format '{cpt}' (should be 5 digits OR 1 letter + 4 digits)")
        return "unknown"

def normalise_charges(data: dict) -> dict:
    for line in data.get("service_lines", []):
        if line.get("charge_amount"):
            line["charge_amount"] = _fix_charge(line["charge_amount"])
        # Validate CPT codes
        if line.get("cpt_code"):
            original_cpt = line["cpt_code"]
            validated_cpt = validate_cpt_code(original_cpt)
            if validated_cpt != original_cpt:
                print(f"   ⚠ CPT validation: '{original_cpt}' → '{validated_cpt}'")
            line["cpt_code"] = validated_cpt
    binfo = data.get("billing_info", {})
    if binfo.get("total_charge"):
        binfo["total_charge"] = _fix_charge(binfo["total_charge"])
    return data

# ─────────────────────────────────────────────────────────────────────────────
#  Vision LLM call
# ─────────────────────────────────────────────────────────────────────────────
def extract_via_llm(pdf_path: str) -> dict:
    images_b64 = pdf_to_b64_images(pdf_path)

    user_parts = [
        {"type": "image_url",
         "image_url": {"url": f"data:image/jpeg;base64,{b64}"}}
        for b64 in images_b64
    ] + [{"type": "text", "text": USER_HINT}]

    messages = [
        {"role": "system",    "content": SYSTEM_PROMPT},
        {"role": "assistant", "content": None,
         "function_call": {"name": "extract_hcfa1500", "arguments": "{}"}},
        {"role": "user",      "content": user_parts}
    ]

    resp = client.chat.completions.create(
        model         = OPENAI_MODEL,
        messages      = messages,
        functions     = FUNCTIONS,
        function_call = {"name": "extract_hcfa1500"},
        temperature   = 0.0,
        max_tokens    = 2048
    )

    extracted_data = json.loads(resp.choices[0].message.function_call.arguments)
    
    # Validate service lines were extracted
    service_lines = extracted_data.get("service_lines", [])
    if not service_lines:
        print(f" ⚠ WARNING: No service lines extracted from {Path(pdf_path).name}")
        print(f"   Extracted data keys: {list(extracted_data.keys())}")
        if "billing_info" in extracted_data:
            total_charge = extracted_data["billing_info"].get("total_charge")
            if total_charge:
                print(f"   Total charge found: {total_charge} - creating fallback service line")
                # Create a fallback service line with the total charge
                extracted_data["service_lines"] = [{
                    "date_of_service": None,
                    "place_of_service": None,
                    "cpt_code": "unknown",
                    "modifiers": [],
                    "diagnosis_pointer": None,
                    "charge_amount": total_charge,
                    "units": 1
                }]
    else:
        print(f"   ✓ Extracted {len(service_lines)} service line(s)")
        for i, line in enumerate(service_lines):
            cpt = line.get("cpt_code", "unknown")
            charge = line.get("charge_amount", "unknown")
            print(f"     Line {i+1}: CPT={cpt}, Charge={charge}")
    
    return extracted_data

# ─────────────────────────────────────────────────────────────────────────────
#  SQLite update  (same logic as legacy script)
# ─────────────────────────────────────────────────────────────────────────────
def update_provider_bill(bill_id: str, extracted: dict) -> bool:
    db_path = r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\monolith\monolith.db"
    
    # Add retry logic for database lock issues
    max_retries = 3
    retry_delay = 1  # seconds
    
    for attempt in range(max_retries):
        try:
            # Add timeout to handle database locks
            conn = sqlite3.connect(db_path, timeout=30.0)
            cur = conn.cursor()
            
            pinfo, binfo = extracted["patient_info"], extracted["billing_info"]
            tot = binfo.get("total_charge")
            tot_f = float(tot.strip("$").replace(",", "")) if tot else None

            cur.execute("SELECT id FROM ProviderBill WHERE id=?", (bill_id,))
            if not cur.fetchone():
                print(f"ProviderBill {bill_id} not found.")
                return False

            cur.execute("""
                UPDATE ProviderBill
                SET status='RECEIVED', last_error=NULL, action=NULL, bill_paid='N',
                    patient_name=?, patient_dob=?, patient_zip=?,
                    billing_provider_name=?, billing_provider_address=?,
                    billing_provider_tin=?, billing_provider_npi=?,
                    total_charge=?, patient_account_no=?
                WHERE id=?
            """, (
                pinfo.get("patient_name"), pinfo.get("patient_dob"),
                pinfo.get("patient_zip"),
                binfo.get("billing_provider_name"), binfo.get("billing_provider_address"),
                binfo.get("billing_provider_tin"),  binfo.get("billing_provider_npi"),
                tot_f, binfo.get("patient_account_no"), bill_id
            ))

            for ln in extracted["service_lines"]:
                charge_f = float(ln["charge_amount"].strip("$").replace(",", ""))
                mods = ",".join(ln.get("modifiers", [])) if ln.get("modifiers") else ""
                cur.execute("""
                    INSERT INTO BillLineItem (
                        provider_bill_id, cpt_code, modifier, units,
                        charge_amount, allowed_amount, decision, reason_code,
                        date_of_service, place_of_service, diagnosis_pointer
                    ) VALUES (?, ?, ?, ?, ?, NULL, 'pending', '',
                              ?, ?, ?)
                """, (
                    bill_id, ln["cpt_code"], mods, ln["units"],
                    charge_f, ln.get("date_of_service"),
                    ln.get("place_of_service"), ln.get("diagnosis_pointer")
                ))

            conn.commit()
            return True
            
        except sqlite3.OperationalError as exc:
            if "database is locked" in str(exc) and attempt < max_retries - 1:
                print(f"Database locked for {bill_id}, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})")
                import time
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
                continue
            else:
                conn.rollback()
                print(f"SQLite error {bill_id}: {exc}")
                return False
        except Exception as exc:
            conn.rollback()
            print(f"SQLite error {bill_id}: {exc}")
            return False
        finally:
            conn.close()
    
    return False

# ─────────────────────────────────────────────────────────────────────────────
#  Main S3 loop
# ─────────────────────────────────────────────────────────────────────────────
def process_s3(limit: int | None = None):
    print(f"Vision extraction for bills with status 'RECEIVED'")
    
    # Query database for bills that need processing
    db_path = r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\monolith\monolith.db"
    conn = sqlite3.connect(db_path, timeout=30.0)
    cur = conn.cursor()
    
    try:
        # Get all bills with status 'RECEIVED'
        cur.execute("SELECT id FROM ProviderBill WHERE status = 'RECEIVED' ORDER BY created_at ASC")
        bill_ids = [row[0] for row in cur.fetchall()]
        
        if limit:
            bill_ids = bill_ids[:limit]
        
        print(f"Found {len(bill_ids)} bills with status 'RECEIVED' to process")
        
        if not bill_ids:
            print("No bills with status 'RECEIVED' found. Nothing to process.")
            return
        
        for bill_id in bill_ids:
            # Check if PDF exists in S3 input folder
            pdf_key = f"{INPUT_PREFIX}{bill_id}.pdf"
            
            # Check if PDF exists in S3
            try:
                s3_objects = list_objects(INPUT_PREFIX)
                if pdf_key not in s3_objects:
                    print(f" ❌ PDF not found in S3 for bill {bill_id}: {pdf_key}")
                    # Log this as an error
                    err = tempfile.mktemp(suffix=".log")
                    with open(err, "w") as f:
                        f.write(f"{datetime.now()}: {bill_id} – PDF not found in S3: {pdf_key}\n")
                    upload(err, LOG_PREFIX)
                    continue
            except Exception as s3_exc:
                print(f" ❌ Error checking S3 for bill {bill_id}: {s3_exc}")
                continue
            
            tmp_pdf = tempfile.mktemp(suffix=".pdf")
            try:
                download(pdf_key, tmp_pdf)
                print(f"→ {bill_id}")

                data = normalise_charges(extract_via_llm(tmp_pdf))

                # Additional validation before database update
                service_lines = data.get("service_lines", [])
                if not service_lines:
                    print(f" ❌ ERROR: No service lines found after extraction for {bill_id}")
                    # Log this as an error
                    err = tempfile.mktemp(suffix=".log")
                    with open(err, "w") as f:
                        f.write(f"{datetime.now()}: {bill_id} – No service lines extracted\n")
                    upload(err, LOG_PREFIX)
                    continue
                
                # Validate that service lines have required fields
                valid_service_lines = []
                for i, line in enumerate(service_lines):
                    if not line.get("cpt_code") or not line.get("charge_amount"):
                        print(f" ⚠ WARNING: Service line {i+1} missing required fields (CPT: {line.get('cpt_code')}, Charge: {line.get('charge_amount')})")
                        continue
                    valid_service_lines.append(line)
                
                if not valid_service_lines:
                    print(f" ❌ ERROR: No valid service lines found for {bill_id}")
                    err = tempfile.mktemp(suffix=".log")
                    with open(err, "w") as f:
                        f.write(f"{datetime.now()}: {bill_id} – No valid service lines (missing CPT or charge)\n")
                    upload(err, LOG_PREFIX)
                    continue
                
                # Update data with validated service lines
                data["service_lines"] = valid_service_lines
                print(f"   ✓ Validated {len(valid_service_lines)} service line(s)")

                if update_provider_bill(bill_id, data):
                    # push JSON
                    tmp_json = tempfile.mktemp(suffix=".json")
                    with open(tmp_json, "w", encoding="utf-8") as f:
                        json.dump(data, f, indent=2)
                    upload(tmp_json, f"{OUTPUT_PREFIX}{bill_id}.json")
                    os.unlink(tmp_json)

                    # Create local backup before archiving
                    backup_dir = Path("backup_pdfs")
                    backup_dir.mkdir(exist_ok=True)
                    backup_path = backup_dir / f"{bill_id}.pdf"
                    
                    try:
                        # Copy the local PDF to backup location
                        import shutil
                        shutil.copy2(tmp_pdf, backup_path)
                        print(f" 📁 Local backup: {backup_path}")
                    except Exception as backup_exc:
                        print(f" ⚠ Local backup failed: {backup_exc}")

                    # archive original PDF
                    try:
                        move(pdf_key, f"{ARCHIVE_PREFIX}{bill_id}.pdf")
                        print(" ✓ success")
                        
                        # If S3 archive succeeded, we can optionally remove local backup
                        # Uncomment the next line if you want to auto-cleanup successful backups
                        # backup_path.unlink(missing_ok=True)
                        
                    except Exception as archive_exc:
                        print(f" ⚠ Archive failed: {archive_exc}")
                        print(f" 💾 Local backup preserved at: {backup_path}")
                        # Log the archiving failure
                        err = tempfile.mktemp(suffix=".log")
                        with open(err, "w") as f:
                            f.write(f"{datetime.now()}: {bill_id} – Archive failed: {archive_exc}\n")
                        upload(err, LOG_PREFIX)
                        # Note: Data is still processed and in database, but PDF remains in input
                else:
                    print(" ⚠ DB update failed")
            except Exception as exc:
                print(f" ❌ {exc}")
                err = tempfile.mktemp(suffix=".log")
                with open(err, "w") as f:
                    f.write(f"{datetime.now()}: {bill_id} – {exc}\n")
                upload(err, LOG_PREFIX)
            finally:
                if os.path.exists(tmp_pdf):
                    os.unlink(tmp_pdf)
    
    except Exception as db_exc:
        print(f" ❌ Database error: {db_exc}")
    finally:
        conn.close()

    print("Done – vision extraction complete.")

# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Extract HCFA-1500 PDFs with GPT-4o-vision")
    ap.add_argument("--limit", type=int, help="Process only N PDFs for testing")
    args = ap.parse_args()
    process_s3(limit=args.limit)
