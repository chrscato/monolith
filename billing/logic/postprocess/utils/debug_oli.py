# Debug script to troubleshoot order line items retrieval

import sqlite3
from pathlib import Path

DB_ROOT = Path(r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\monolith")

def debug_order_line_items():
    """Debug the order line items table and relationships."""
    
    db_path = str(DB_ROOT / 'monolith.db')
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    try:
        print("=== DEBUGGING ORDER LINE ITEMS ===\n")
        
        # 1. Check order_line_items table structure
        print("1. order_line_items table structure:")
        cursor.execute("PRAGMA table_info(order_line_items)")
        columns = cursor.fetchall()
        for col in columns:
            print(f"   {col[1]} ({col[2]})")
        
        # 2. Get total count
        cursor.execute("SELECT COUNT(*) as count FROM order_line_items")
        total_count = cursor.fetchone()[0]
        print(f"\n2. Total order_line_items records: {total_count}")
        
        # 3. Check sample data
        print("\n3. Sample order_line_items data (first 5 rows):")
        cursor.execute("SELECT * FROM order_line_items LIMIT 5")
        samples = cursor.fetchall()
        for i, row in enumerate(samples):
            row_dict = dict(row)
            print(f"   Row {i+1}:")
            print(f"      id: {row_dict.get('id')}")
            print(f"      Order_ID: '{row_dict.get('Order_ID')}'")
            print(f"      DOS: '{row_dict.get('DOS')}'")
            print(f"      CPT: '{row_dict.get('CPT')}'")
            print(f"      is_active: '{row_dict.get('is_active')}'")
            print()
        
        # 4. Check unique Order_IDs in order_line_items
        print("4. Sample Order_IDs in order_line_items:")
        cursor.execute("SELECT DISTINCT Order_ID FROM order_line_items LIMIT 10")
        order_ids = cursor.fetchall()
        for row in order_ids:
            order_id = row[0]
            print(f"   '{order_id}' (type: {type(order_id)})")
        
        # 5. Get some Order_IDs from our bills to compare
        print("\n5. Order_IDs from our bills:")
        cursor.execute("""
            SELECT DISTINCT o.Order_ID
            FROM ProviderBill pb
            INNER JOIN orders o ON pb.claim_id = o.Order_ID
            WHERE pb.status = 'REVIEWED'
            AND pb.action = 'apply_rate'
            AND (pb.bill_paid IS NULL OR pb.bill_paid = 'N')
            LIMIT 10
        """)
        bill_order_ids = cursor.fetchall()
        for row in bill_order_ids:
            order_id = row[0]
            print(f"   '{order_id}' (type: {type(order_id)})")
        
        # 6. Check for any matches between the two
        print("\n6. Testing direct matches:")
        if bill_order_ids and order_ids:
            test_order_id = bill_order_ids[0][0]
            print(f"   Testing with Order_ID: '{test_order_id}'")
            
            # Try exact match
            cursor.execute("SELECT COUNT(*) FROM order_line_items WHERE Order_ID = ?", (test_order_id,))
            exact_count = cursor.fetchone()[0]
            print(f"   Exact match count: {exact_count}")
            
            # Try case-insensitive match
            cursor.execute("SELECT COUNT(*) FROM order_line_items WHERE UPPER(Order_ID) = UPPER(?)", (test_order_id,))
            case_insensitive_count = cursor.fetchone()[0]
            print(f"   Case-insensitive match count: {case_insensitive_count}")
            
            # Try with LIKE
            cursor.execute("SELECT COUNT(*) FROM order_line_items WHERE Order_ID LIKE ?", (f"%{test_order_id}%",))
            like_count = cursor.fetchone()[0]
            print(f"   LIKE match count: {like_count}")
            
            # Show any order_line_items that contain part of our Order_ID
            cursor.execute("SELECT Order_ID, CPT FROM order_line_items WHERE Order_ID LIKE ? LIMIT 5", (f"%{test_order_id[-5:]}%",))
            partial_matches = cursor.fetchall()
            if partial_matches:
                print(f"   Partial matches found:")
                for match in partial_matches:
                    print(f"      Order_ID: '{match[0]}', CPT: '{match[1]}'")
        
        # 7. Check if there are any active order line items
        print("\n7. Active order line items check:")
        cursor.execute("SELECT COUNT(*) FROM order_line_items WHERE is_active IS NULL OR is_active != 'N'")
        active_count = cursor.fetchone()[0]
        print(f"   Active order line items: {active_count}")
        
        cursor.execute("SELECT COUNT(*) FROM order_line_items WHERE is_active = 'N'")
        inactive_count = cursor.fetchone()[0]
        print(f"   Inactive order line items: {inactive_count}")
        
        # 8. Check for any order line items from recent dates
        print("\n8. Recent order line items (by creation date):")
        cursor.execute("""
            SELECT Order_ID, CPT, DOS, created_at 
            FROM order_line_items 
            WHERE created_at IS NOT NULL 
            ORDER BY created_at DESC 
            LIMIT 5
        """)
        recent_items = cursor.fetchall()
        for item in recent_items:
            print(f"   Order_ID: '{item[0]}', CPT: '{item[1]}', DOS: '{item[2]}', Created: {item[3]}")
        
    except Exception as e:
        print(f"Error during debugging: {str(e)}")
    finally:
        conn.close()

# Run the debug
debug_order_line_items()