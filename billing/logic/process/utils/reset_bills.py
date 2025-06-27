import sqlite3
import logging
import os
from typing import List, Optional
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_connection(db_path: str = None) -> sqlite3.Connection:
    """Get a connection to the SQLite database."""
    if db_path is None:
        db_path = os.getenv("MONOLITH_DB_PATH", "monolith.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def reset_bills(
    status: Optional[str] = None,
    action: Optional[str] = None,
    error_message: Optional[str] = None,
    limit: Optional[int] = None
) -> int:
    """
    Reset bills based on specified criteria.
    
    Args:
        status: Reset bills with this status (e.g., 'FLAGGED', 'REVIEW_FLAG')
        action: Reset bills with this action (e.g., 'update_prov_info', 'to_review')
        error_message: Reset bills with this error message
        limit: Optional maximum number of bills to reset
        
    Returns:
        Number of bills reset
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Build the query
    query = "UPDATE ProviderBill SET status = 'MAPPED', action = NULL, last_error = NULL WHERE 1=1"
    params = []
    
    if status:
        query += " AND status = ?"
        params.append(status)
    if action:
        query += " AND action = ?"
        params.append(action)
    if error_message:
        query += " AND last_error LIKE ?"
        params.append(f"%{error_message}%")
    
    if limit:
        query += f" LIMIT {limit}"
    
    # Execute the update
    cursor.execute(query, params)
    rows_affected = cursor.rowcount
    
    # Log the changes
    logger.info(f"Reset {rows_affected} bills back to MAPPED status")
    
    conn.commit()
    conn.close()
    return rows_affected

def main():
    """Main function to run the reset script."""
    # Example: Reset all bills that were flagged due to missing provider info
    reset_bills(
        status='FLAGGED',
        action='update_prov_info',
        error_message='Provider information not found'
    )

if __name__ == "__main__":
    main() 