import sqlite3
from pathlib import Path
import os

# Get the actual project root (one level up from billing)
project_root = Path(__file__).resolve().parent
db_path = project_root / 'monolith.db'

print(f"Current working directory: {os.getcwd()}")
print(f"Checking database at: {db_path}")
print(f"Database exists: {db_path.exists()}")
print(f"Database absolute path: {db_path.absolute()}")

if db_path.exists():
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get list of tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        print("\nTables in database:")
        for table in tables:
            print(f"- {table[0]}")
            
            # Get table schema
            cursor.execute(f"PRAGMA table_info({table[0]})")
            columns = cursor.fetchall()
            for col in columns:
                print(f"  {col[1]} ({col[2]})")
        
        # Check if ProviderBill table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ProviderBill';")
        provider_bill = cursor.fetchone()
        print(f"\nProviderBill table exists: {provider_bill is not None}")
        
        conn.close()
    except sqlite3.Error as e:
        print(f"Error connecting to database: {str(e)}") 