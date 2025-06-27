#!/usr/bin/env python3
"""
map_bill.py

Maps validated ProviderBill records to claims.
Supports both operational and diagnostic CLI mode.
"""
import os
import sys
import logging
import sqlite3
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import re
from difflib import SequenceMatcher
from typing import Optional

# ─── Config ──────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / '.env')

DB_ROOT = Path(__file__).resolve().parents[4]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── Utilities ───────────────────────────────────────────────────

def clean_name(name: str) -> str:
    if not name:
        return ""
    name = name.lower().replace(",", "").strip()
    name = re.sub(r'[^a-z\s-]', '', name)
    suffixes = ['jr', 'sr', 'ii', 'iii', 'iv', 'v', 'phd', 'md', 'do']
    for sfx in suffixes:
        name = re.sub(rf'\b{sfx}\b', '', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip()

def normalize_date(date_str: str) -> Optional[datetime.date]:
    if not date_str:
        return None
    date_str = str(date_str).strip()
    if ' - ' in date_str:
        date_str = date_str.split(' - ')[0].strip()
    if ' ' in date_str:
        date_str = date_str.split(' ')[0]
    formats = [
        '%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y',
        '%Y/%m/%d', '%m/%d/%y', '%m-%d-%y',
        '%Y%m%d', '%m%d%Y', '%m%d%y'
    ]
    for fmt in formats:
        try:
            d = datetime.strptime(date_str, fmt).date()
            if 2020 <= d.year <= 2035:
                return d
        except ValueError:
            continue
    return None

def similar(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()

# ─── Matching Logic ──────────────────────────────────────────────

def find_matching_claim(bill: dict, cursor: sqlite3.Cursor, is_diagnostic=False) -> str | None:
    bill_patient_name = clean_name(bill['patient_name'])
    logger.info(f"📌 Cleaned bill name: {bill_patient_name}")

    cursor.execute("""
        SELECT date_of_service FROM BillLineItem 
        WHERE provider_bill_id = ?
        ORDER BY date_of_service
    """, (bill['id'],))
    bill_dates = [normalize_date(row['date_of_service']) for row in cursor.fetchall()]
    bill_dates = [d for d in bill_dates if d]

    if not bill_dates:
        logger.warning("❌ No valid DOS found in BillLineItem")
        return None

    cursor.execute("""
        SELECT DISTINCT o.Order_ID, o.Patient_Last_Name, o.Patient_First_Name, oli.DOS 
        FROM Orders o
        JOIN order_line_items oli ON o.Order_ID = oli.order_id
        WHERE oli.DOS >= '2024-01-01' AND oli.DOS <= '2025-12-31'
    """)
    orders = cursor.fetchall()

    best_match = None
    best_score = 0.0
    top_matches = []

    for order in orders:
        # Try "first last" format first
        order_name = f"{clean_name(order['Patient_First_Name'])} {clean_name(order['Patient_Last_Name'])}"
        sim = similar(bill_patient_name, order_name)
        order_date = normalize_date(order['DOS'])
        date_close = any(abs((order_date - bd).days) <= 21 for bd in bill_dates if order_date)

        # If no match with "first last", try "last first" format
        if sim < 0.80 and date_close:
            order_name_flipped = f"{clean_name(order['Patient_Last_Name'])} {clean_name(order['Patient_First_Name'])}"
            sim = similar(bill_patient_name, order_name_flipped)

        if sim >= 0.80 and date_close:
            top_matches.append((order['Order_ID'], sim, order_date))
            if sim > best_score:
                best_score = sim
                best_match = order['Order_ID']

    if is_diagnostic:
        print(f"\n🔍 Top Matching Orders:")
        for match in sorted(top_matches, key=lambda x: x[1], reverse=True)[:10]:
            print(f"  → Order: {match[0]} | Similarity: {match[1]:.2f} | DOS: {match[2]}")
        print(f"\n🎯 Best Match: {best_match} (Score: {best_score:.2f})")
        if not best_match:
            print("⚠️ No claim found.\n")
        return None  # Don't map in diagnostic mode

    if not best_match:
        logger.warning("⚠️ No matching claim found")
        return None

    # CPT check
    cursor.execute("SELECT cpt_code FROM BillLineItem WHERE provider_bill_id = ?", (bill['id'],))
    bill_cpts = set(code['cpt_code'].strip() for code in cursor.fetchall() if code['cpt_code'])

    cursor.execute("SELECT CPT FROM order_line_items WHERE order_id = ?", (best_match,))
    order_cpts = set(code['CPT'].strip() for code in cursor.fetchall() if code['CPT'])

    overlap = bill_cpts & order_cpts
    logger.info(f"✅ CPT match overlap: {overlap} (count: {len(overlap)})")

    return best_match

# ─── Mapping Flow ────────────────────────────────────────────────

def map_provider_bill(bill_id: str, cursor: sqlite3.Cursor) -> tuple[str, str, str]:
    cursor.execute("SELECT * FROM ProviderBill WHERE id = ?", (bill_id,))
    bill = cursor.fetchone()
    if not bill:
        return 'INVALID', 'to_validate', f"ProviderBill {bill_id} not found"
    if bill['status'] != 'VALID' or bill['action'] != 'to_map':
        return bill['status'], bill['action'], "Bill not ready for mapping"

    claim_id = find_matching_claim(bill, cursor)
    if claim_id:
        cursor.execute("SELECT FULLY_PAID, BILLS_REC FROM Orders WHERE Order_ID = ?", (claim_id,))
        order_status = cursor.fetchone()

        if order_status and order_status[0] == 'Y':
            cursor.execute("UPDATE Orders SET BILLS_REC = BILLS_REC + 1 WHERE Order_ID = ?", (claim_id,))
            cursor.execute("""
                UPDATE ProviderBill 
                SET claim_id = ?, status = ?, action = ?, last_error = ?
                WHERE id = ?
            """, (claim_id, 'DUPLICATE', 'to_review', 'Order already fully paid', bill_id))
            return 'DUPLICATE', 'to_review', 'Order already fully paid'

        cursor.execute("""
            UPDATE ProviderBill 
            SET claim_id = ?, status = ?, action = ?, last_error = NULL
            WHERE id = ?
        """, (claim_id, 'MAPPED', 'to_review', bill_id))
        return 'MAPPED', 'to_review', None
    else:
        error = "No matching claim found for patient and dates"
        cursor.execute("""
            UPDATE ProviderBill 
            SET status = ?, action = ?, last_error = ?
            WHERE id = ?
        """, ('UNMAPPED', 'to_map', error, bill_id))
        return 'UNMAPPED', 'to_map', error

def process_mapping():
    db_path = os.getenv("MONOLITH_DB_PATH", str(DB_ROOT / 'monolith.db'))
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    mapped = 0
    duplicate = 0
    unmapped = 0

    try:
        cursor.execute("""
            SELECT id FROM ProviderBill 
            WHERE status = 'VALID' AND action = 'to_map'
        """)
        bills = cursor.fetchall()
        total = len(bills)
        logger.info(f"🔁 Mapping {total} bills...")

        for (bill_id,) in bills:
            status, action, error = map_provider_bill(bill_id, cursor)
            if status == "MAPPED":
                mapped += 1
            elif status == "DUPLICATE":
                duplicate += 1
            elif status == "UNMAPPED":
                unmapped += 1

        conn.commit()

        print(f"\n✅ Summary:")
        print(f"- MAPPED: {mapped}")
        print(f"- DUPLICATE: {duplicate}")
        print(f"- UNMAPPED: {unmapped}")
        print(f"- TOTAL: {total}\n")
        logger.info("✅ Mapping complete.")

    except sqlite3.Error as e:
        conn.rollback()
        logger.error(f"❌ DB Error: {str(e)}")
    finally:
        conn.close()


def run_diagnostic(bill_id: str):
    db_path = os.getenv("MONOLITH_DB_PATH", str(DB_ROOT / 'monolith.db'))
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM ProviderBill WHERE id = ?", (bill_id,))
        bill = cursor.fetchone()
        if not bill:
            print(f"❌ Bill {bill_id} not found.")
            return

        print(f"\n🩺 Running diagnostic for bill: {bill_id}")
        print(f"Original Name: {bill['patient_name']}")
        print(f"Normalized: {clean_name(bill['patient_name'])}\n")

        find_matching_claim(bill, cursor, is_diagnostic=True)
    finally:
        conn.close()

# ─── Entry Point ─────────────────────────────────────────────────

if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == "--diagnostic":
        run_diagnostic(sys.argv[2])
    else:
        process_mapping()
