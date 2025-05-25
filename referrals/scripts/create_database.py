import sqlite3
from datetime import datetime

def create_referrals_database(db_path='referrals_wc.db'):
    """
    Create a streamlined workers' compensation referrals database
    with essential fields for practical use.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Enable foreign key constraints
    cursor.execute("PRAGMA foreign_keys = ON")

    # 1. Outlook Graph API Metadata Table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS outlook_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id TEXT UNIQUE NOT NULL,
        subject TEXT,
        sender_email TEXT,
        sender_name TEXT,
        received_datetime TEXT,
        body_preview TEXT,
        body_content TEXT,
        has_attachments BOOLEAN DEFAULT 0,
        processing_status TEXT DEFAULT 'pending', -- pending, processed, error
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # 2. Attachments Table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS attachments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id INTEGER,
        outlook_attachment_id TEXT,
        filename TEXT NOT NULL,
        content_type TEXT,
        size INTEGER,
        s3_key TEXT,
        upload_status TEXT DEFAULT 'pending', -- pending, uploaded, failed
        uploaded_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (message_id) REFERENCES outlook_messages (id) ON DELETE CASCADE
    )
    ''')

    # 3. Main Referrals Table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS referrals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_id INTEGER,
        referral_number TEXT,
        
        -- Injured Worker (IW) Information
        iw_first_name TEXT,
        iw_last_name TEXT,
        iw_date_of_birth DATE,
        iw_phone TEXT,
        iw_email TEXT,
        iw_address TEXT,
        iw_city TEXT,
        iw_state TEXT,
        iw_zip_code TEXT,
        iw_employee_id TEXT,
        iw_job_title TEXT,
        
        -- Employer Information
        employer_name TEXT,
        employer_phone TEXT,
        employer_email TEXT,
        employer_address TEXT,
        employer_city TEXT,
        employer_state TEXT,
        employer_zip_code TEXT,
        employer_contact_name TEXT,
        employer_contact_phone TEXT,
        
        -- Claim Information
        claim_number TEXT,
        adjuster_name TEXT,
        adjuster_phone TEXT,
        adjuster_email TEXT,
        insurance_carrier TEXT,
        date_of_injury DATE,
        injury_description TEXT,
        body_parts_affected TEXT,
        
        -- Service Order Information
        service_type TEXT, -- PT, OT, Psychology, IME, FCE, etc.
        cpt_codes TEXT, -- comma-separated
        icd10_codes TEXT, -- comma-separated
        service_frequency TEXT, -- 2x/week, etc.
        authorized_visits INTEGER,
        priority_level TEXT DEFAULT 'routine', -- routine, urgent
        authorization_number TEXT,
        
        -- Referring Provider Information
        referring_provider_name TEXT,
        referring_provider_npi TEXT,
        referring_provider_phone TEXT,
        referring_provider_address TEXT,
        
        -- Service Provider Information
        service_provider_name TEXT,
        service_provider_npi TEXT,
        service_provider_phone TEXT,
        service_provider_address TEXT,
        service_provider_network_status TEXT, -- in-network, out-of-network
        
        -- Clinical Information
        diagnosis_primary TEXT,
        clinical_notes TEXT,
        treatment_goals TEXT,
        work_restrictions TEXT,
        
        -- Status and Workflow
        referral_status TEXT DEFAULT 'received', -- received, reviewed, scheduled, completed
        assigned_coordinator TEXT,
        notes TEXT,
        
        -- Timestamps
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        due_date DATE,
        
        -- Data Quality
        manual_review_required BOOLEAN DEFAULT 0,
        
        FOREIGN KEY (message_id) REFERENCES outlook_messages (id) ON DELETE SET NULL
    )
    ''')

    # 4. Simple Activity Log
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS referral_activities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        referral_id INTEGER,
        activity_type TEXT, -- note, status_change, contact
        activity_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        user_name TEXT,
        description TEXT,
        FOREIGN KEY (referral_id) REFERENCES referrals (id) ON DELETE CASCADE
    )
    ''')

    # Create essential indexes
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_outlook_messages_id ON outlook_messages(message_id)",
        "CREATE INDEX IF NOT EXISTS idx_referrals_status ON referrals(referral_status)",
        "CREATE INDEX IF NOT EXISTS idx_referrals_claim ON referrals(claim_number)",
        "CREATE INDEX IF NOT EXISTS idx_referrals_iw_name ON referrals(iw_last_name, iw_first_name)",
        "CREATE INDEX IF NOT EXISTS idx_referrals_date ON referrals(date_of_injury)",
    ]
    
    for index in indexes:
        cursor.execute(index)

    conn.commit()
    conn.close()
    
    print(f"Streamlined referrals database created at: {db_path}")
    print("\nTables created:")
    print("- outlook_messages: Email metadata")
    print("- attachments: File storage")
    print("- referrals: Core referral data")
    print("- referral_activities: Activity tracking")
    
    return db_path

if __name__ == "__main__":
    create_referrals_database()