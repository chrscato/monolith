import sqlite3
from pathlib import Path
import sys

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

def check_monolith_db():
    """Check the monolith database connection and structure."""
    monolith_db_path = Path(r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\monolith\cdx_ehr\monolith.db")
    print(f"Looking for database at: {monolith_db_path}")
    
    if not monolith_db_path.exists():
        print("Error: monolith.db not found!")
        return
    
    try:
        conn = sqlite3.connect(str(monolith_db_path))
        cursor = conn.cursor()
        
        # Get list of tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        print("\nFound tables:")
        for table in tables:
            print(f"\n{table[0]}:")
            # Get table structure
            cursor.execute(f"PRAGMA table_info({table[0]});")
            columns = cursor.fetchall()
            for col in columns:
                print(f"  {col[1]} ({col[2]})")
        
        conn.close()
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")

if __name__ == '__main__':
    check_monolith_db() 