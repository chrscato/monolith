
import sqlite3
import os

def create_monolith_db(db_path=None):
    if db_path is None:
        db_path = os.getenv('MONOLITH_DB_PATH', 'monolith.db')
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # ProviderBill table
    c.execute('''
    CREATE TABLE IF NOT EXISTS ProviderBill (
        id TEXT PRIMARY KEY,
        claim_id TEXT,
        uploaded_by TEXT,
        source_file TEXT,
        status TEXT,
        last_error TEXT,
        created_at TEXT
    )
    ''')

    # BillLineItem table
    c.execute('''
    CREATE TABLE IF NOT EXISTS BillLineItem (
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
        FOREIGN KEY (provider_bill_id) REFERENCES ProviderBill (id)
    )
    ''')

    # FeeSchedule table
    c.execute('''
    CREATE TABLE IF NOT EXISTS FeeSchedule (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        state TEXT,
        cpt_code TEXT,
        modifier TEXT,
        rate REAL,
        effective_date TEXT
    )
    ''')

    # AdjustmentReason table
    c.execute('''
    CREATE TABLE IF NOT EXISTS AdjustmentReason (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE,
        label TEXT,
        description TEXT
    )
    ''')

    # EOBR table
    c.execute('''
    CREATE TABLE IF NOT EXISTS EOBR (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        provider_bill_id TEXT,
        summary_notes TEXT,
        file_path TEXT,
        generated_by TEXT,
        created_at TEXT,
        FOREIGN KEY (provider_bill_id) REFERENCES ProviderBill (id)
    )
    ''')

    # ReimbursementLog table
    c.execute('''
    CREATE TABLE IF NOT EXISTS ReimbursementLog (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bill_line_id INTEGER,
        paid_amount REAL,
        paid_date TEXT,
        method TEXT,
        FOREIGN KEY (bill_line_id) REFERENCES BillLineItem (id)
    )
    ''')

    conn.commit()
    conn.close()
    print(f"Monolith DB created at: {db_path}")

if __name__ == "__main__":
    create_monolith_db()
