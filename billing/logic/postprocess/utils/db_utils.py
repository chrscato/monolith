from typing import List, Dict, Any
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

def get_db_connection():
    """Get database connection."""
    
    # Method 1: Try to use existing Django connection (if available)
    try:
        # If you're using Django
        from django.db import connection
        return connection
    except ImportError:
        pass
    
    # Method 2: Try SQLAlchemy connection (if available)
    try:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        
        # You'll need to set your database URL
        # Examples:
        # DATABASE_URL = "postgresql://user:password@localhost/dbname"
        # DATABASE_URL = "mysql://user:password@localhost/dbname" 
        # DATABASE_URL = "sqlite:///path/to/database.db"
        
        DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///billing_system.db')
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        return Session()
    except ImportError:
        pass
    
    # Method 3: Direct database connection based on your database type
    try:
        # Option A: PostgreSQL
        if os.getenv('DB_TYPE', '').lower() == 'postgresql':
            import psycopg2
            import psycopg2.extras
            
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                database=os.getenv('DB_NAME', 'billing'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD', ''),
                port=os.getenv('DB_PORT', '5432')
            )
            conn.cursor_factory = psycopg2.extras.RealDictCursor
            return conn
            
        # Option B: MySQL
        elif os.getenv('DB_TYPE', '').lower() == 'mysql':
            import mysql.connector
            
            conn = mysql.connector.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                database=os.getenv('DB_NAME', 'billing'),
                user=os.getenv('DB_USER', 'root'),
                password=os.getenv('DB_PASSWORD', ''),
                port=os.getenv('DB_PORT', '3306')
            )
            return conn
            
        # Option C: SQL Server
        elif os.getenv('DB_TYPE', '').lower() == 'sqlserver':
            import pyodbc
            
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={os.getenv('DB_HOST', 'localhost')};"
                f"DATABASE={os.getenv('DB_NAME', 'billing')};"
                f"UID={os.getenv('DB_USER', 'sa')};"
                f"PWD={os.getenv('DB_PASSWORD', '')}"
            )
            conn = pyodbc.connect(conn_str)
            return conn
            
        # Option D: SQLite (fallback for development)
        else:
            import sqlite3
            
            db_path = os.getenv('DATABASE_PATH', 'billing_system.db')
            
            # Create database file if it doesn't exist
            if not os.path.exists(db_path):
                logger.warning(f"Database file {db_path} does not exist. Creating new database.")
                # You might want to run schema creation here
            
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row  # Enable column access by name
            return conn
            
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise ConnectionError(f"Could not connect to database: {str(e)}")

def get_bill_data(bill_ids: List[int]) -> List[Dict[str, Any]]:
    """
    Fetch all relevant data for the given bill IDs by joining necessary tables.
    
    Tables to join:
    - ProviderBills
    - Orders
    - OrdersLineItems
    - Providers
    - BillLineItem
    - PPO
    - FeeSchedule
    - OTA
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Build the complex join query
        placeholders = ','.join(['?' for _ in bill_ids])
        
        query = f"""
        SELECT 
            pb.id as bill_id,
            pb.bill_paid,
            pb.bill_status,
            pb.total_charge,
            pb.created_at,
            pb.updated_at,
            
            o.id as order_id,
            o.FileMaker_Record_Number,
            o.PatientName,
            o.Patient_DOB,
            o.Patient_First_Name,
            o.Patient_Last_Name,
            o.Patient_Address,
            o.Patient_City,
            o.Patient_State,
            o.Patient_Zip,
            o.PatientPhone,
            o.Patient_Injury_Date,
            o.Order_Type,
            o.Assigning_Company,
            o.Claim_Number,
            o.Referring_Physician,
            o.Jurisdiction_State,
            
            p.id as provider_id,
            p.provider_name,
            p.billing_name as provider_billing_name,
            p.tin as provider_tin,
            p.npi as provider_npi,
            p.network as provider_network,
            p.billing_address1 as provider_billing_address1,
            p.billing_address2 as provider_billing_address2,
            p.billing_city as provider_billing_city,
            p.billing_state as provider_billing_state,
            p.billing_postal_code as provider_billing_postal_code,
            p.phone as provider_phone,
            p.fax as provider_fax,
            
            bli.id as line_item_id,
            bli.cpt_code,
            bli.modifier,
            bli.units,
            bli.allowed_amount,
            bli.charge_amount,
            bli.date_of_service,
            bli.place_of_service,
            bli.decision,
            bli.reason_code,
            bli.diagnosis_pointer,
            bli.category,
            bli.subcategory,
            bli.proc_desc
            
        FROM ProviderBills pb
        LEFT JOIN Orders o ON pb.order_id = o.id
        LEFT JOIN Providers p ON pb.provider_id = p.id
        LEFT JOIN BillLineItem bli ON pb.id = bli.provider_bill_id
        WHERE pb.id IN ({placeholders})
        ORDER BY pb.id, bli.id
        """
        
        cursor.execute(query, bill_ids)
        results = cursor.fetchall()
        
        # Convert to list of dictionaries
        bill_data = []
        for row in results:
            if hasattr(row, 'keys'):
                # SQLite Row object or similar
                bill_dict = dict(row)
            else:
                # Regular tuple, need to map to column names
                columns = [desc[0] for desc in cursor.description]
                bill_dict = dict(zip(columns, row))
            bill_data.append(bill_dict)
        
        conn.close()
        logger.info(f"Fetched {len(bill_data)} records for {len(bill_ids)} bills")
        return bill_data
        
    except Exception as e:
        logger.error(f"Error fetching bill data: {str(e)}")
        return []

def update_bill_status(bill_ids: List[int], status: str):
    """Update the status of bills in the database."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        placeholders = ','.join(['?' for _ in bill_ids])
        
        # Update query - adjust table name if needed
        query = f"""
        UPDATE ProviderBills 
        SET bill_status = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id IN ({placeholders})
        """
        
        cursor.execute(query, [status] + bill_ids)
        conn.commit()
        
        updated_count = cursor.rowcount
        conn.close()
        
        logger.info(f"Updated {updated_count} bills to status: {status}")
        return updated_count
        
    except Exception as e:
        logger.error(f"Error updating bill status: {str(e)}")
        raise

# Configuration helper function
def setup_database_config():
    """
    Helper function to set up database configuration.
    Call this to configure your database connection.
    """
    print("🔧 DATABASE CONFIGURATION SETUP")
    print("=" * 50)
    print("Set these environment variables for your database:")
    print()
    print("For PostgreSQL:")
    print("  export DB_TYPE=postgresql")
    print("  export DB_HOST=your_host")
    print("  export DB_NAME=your_database")
    print("  export DB_USER=your_username")
    print("  export DB_PASSWORD=your_password")
    print("  export DB_PORT=5432")
    print()
    print("For MySQL:")
    print("  export DB_TYPE=mysql")
    print("  export DB_HOST=your_host")
    print("  export DB_NAME=your_database")
    print("  export DB_USER=your_username")
    print("  export DB_PASSWORD=your_password")
    print("  export DB_PORT=3306")
    print()
    print("For SQL Server:")
    print("  export DB_TYPE=sqlserver")
    print("  export DB_HOST=your_host")
    print("  export DB_NAME=your_database")
    print("  export DB_USER=your_username")
    print("  export DB_PASSWORD=your_password")
    print()
    print("For SQLite (default):")
    print("  export DATABASE_PATH=/path/to/your/database.db")
    print()
    print("Or set DATABASE_URL directly:")
    print("  export DATABASE_URL=postgresql://user:pass@host:port/db")
    print("  export DATABASE_URL=mysql://user:pass@host:port/db")
    print("  export DATABASE_URL=sqlite:///path/to/db.sqlite")

if __name__ == "__main__":
    # Test the database connection
    setup_database_config()
    
    try:
        conn = get_db_connection()
        print("✅ Database connection successful!")
        print(f"Connection type: {type(conn)}")
        conn.close()
    except Exception as e:
        print(f"❌ Database connection failed: {str(e)}")
        print("💡 Please check your database configuration")