#!/usr/bin/env python3
"""
Test script to verify expected record counts before merge
Fetches live VM database directly for comparison
"""

import sqlite3
import subprocess
import tempfile
import shutil
from pathlib import Path
from datetime import datetime

def fetch_live_vm_db():
    """Fetch the live VM database to a temporary location"""
    print("🔄 Fetching live VM database...")
    
    # Create a temporary file for the VM database
    temp_vm_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    temp_vm_db.close()
    temp_vm_path = temp_vm_db.name
    
    try:
        # Use scp to fetch the live VM database
        result = subprocess.run([
            'scp', 
            'root@159.223.104.254:/srv/monolith/monolith.db', 
            temp_vm_path
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ Successfully fetched live VM database to: {temp_vm_path}")
            return temp_vm_path
        else:
            print(f"❌ Failed to fetch VM database: {result.stderr}")
            return None
            
    except Exception as e:
        print(f"❌ Error fetching VM database: {e}")
        return None

def test_merge_counts():
    """Test the expected record counts for ProviderBill and BillLineItem tables"""
    
    local_db = "monolith.db"
    
    print("Testing Merge Record Counts (Live VM)")
    print("=" * 50)
    
    # Check if local database exists
    if not Path(local_db).exists():
        print(f"❌ Local database not found: {local_db}")
        return
    
    print(f"✅ Local database found: {Path(local_db).absolute()}")
    
    # Fetch live VM database
    vm_db_path = fetch_live_vm_db()
    if not vm_db_path:
        print("❌ Could not fetch live VM database")
        return
    
    try:
        # Connect to both databases
        conn_local = sqlite3.connect(local_db)
        conn_vm = sqlite3.connect(vm_db_path)
        
        cursor_local = conn_local.cursor()
        cursor_vm = conn_vm.cursor()
        
        # Test ProviderBill counts
        print("\n📊 ProviderBill Table Analysis:")
        print("-" * 30)
        
        cursor_local.execute("SELECT COUNT(*) FROM ProviderBill")
        local_pb_count = cursor_local.fetchone()[0]
        
        cursor_vm.execute("SELECT COUNT(*) FROM ProviderBill")
        vm_pb_count = cursor_vm.fetchone()[0]
        
        print(f"Local ProviderBill records: {local_pb_count}")
        print(f"Live VM ProviderBill records: {vm_pb_count}")
        print(f"Difference: {local_pb_count - vm_pb_count}")
        
        # Test BillLineItem counts
        print("\n📊 BillLineItem Table Analysis:")
        print("-" * 30)
        
        cursor_local.execute("SELECT COUNT(*) FROM BillLineItem")
        local_bli_count = cursor_local.fetchone()[0]
        
        cursor_vm.execute("SELECT COUNT(*) FROM BillLineItem")
        vm_bli_count = cursor_vm.fetchone()[0]
        
        print(f"Local BillLineItem records: {local_bli_count}")
        print(f"Live VM BillLineItem records: {vm_bli_count}")
        print(f"Difference: {local_bli_count - vm_bli_count}")
        
        # Check for different status bills in local
        print("\n🔍 Checking for bill statuses:")
        print("-" * 30)
        
        cursor_local.execute("SELECT status, COUNT(*) FROM ProviderBill GROUP BY status ORDER BY COUNT(*) DESC")
        status_counts = cursor_local.fetchall()
        
        print(f"Bill statuses in local database:")
        for status, count in status_counts:
            print(f"  - {status}: {count} bills")
        
        # Check for recent bills (last 24 hours)
        cursor_local.execute("""
            SELECT COUNT(*) FROM ProviderBill 
            WHERE created_at >= datetime('now', '-1 day')
        """)
        recent_count = cursor_local.fetchone()[0]
        print(f"Bills created in last 24 hours: {recent_count}")
        
        # Get sample of recent bills
        cursor_local.execute("""
            SELECT id, status, created_at FROM ProviderBill 
            WHERE created_at >= datetime('now', '-1 day')
            ORDER BY created_at DESC LIMIT 5
        """)
        sample_bills = cursor_local.fetchall()
        
        print(f"Sample recent bills:")
        for bill_id, status, created_at in sample_bills:
            print(f"  - {bill_id[:8]}... (status: {status}, created: {created_at})")
        
        # Check BillLineItems for these bills
        if sample_bills:
            bill_ids = [bill[0] for bill in sample_bills]
            placeholders = ','.join(['?' for _ in bill_ids])
            
            cursor_local.execute(f"SELECT COUNT(*) FROM BillLineItem WHERE provider_bill_id IN ({placeholders})", bill_ids)
            sample_bli_count = cursor_local.fetchone()[0]
            print(f"BillLineItems for sample bills: {sample_bli_count}")
        
        # Test the merge query logic
        print("\n🧪 Testing Merge Query Logic:")
        print("-" * 30)
        
        # Attach VM database to local connection
        conn_local.execute(f"ATTACH DATABASE '{vm_db_path}' AS vm_db")
        
        # Test ProviderBill merge query
        cursor_local.execute("""
            SELECT COUNT(*) FROM ProviderBill l
            WHERE NOT EXISTS (
                SELECT 1 FROM vm_db.ProviderBill v
                WHERE l.id = v.id
            )
        """)
        new_pb_count = cursor_local.fetchone()[0]
        print(f"New ProviderBill records to add: {new_pb_count}")
        
        # Test BillLineItem merge query
        cursor_local.execute("""
            SELECT COUNT(*) FROM BillLineItem l
            WHERE NOT EXISTS (
                SELECT 1 FROM vm_db.BillLineItem v
                WHERE l.id = v.id
            )
        """)
        new_bli_count = cursor_local.fetchone()[0]
        print(f"New BillLineItem records to add: {new_bli_count}")
        
        # Show some sample new records
        if new_pb_count > 0:
            print(f"\n📋 Sample new ProviderBill records:")
            cursor_local.execute("""
                SELECT id, status, created_at FROM ProviderBill l
                WHERE NOT EXISTS (
                    SELECT 1 FROM vm_db.ProviderBill v
                    WHERE l.id = v.id
                )
                LIMIT 3
            """)
            sample_new_pb = cursor_local.fetchall()
            for pb_id, status, created_at in sample_new_pb:
                print(f"  - {pb_id[:8]}... (status: {status}, created: {created_at})")
        
        if new_bli_count > 0:
            print(f"\n📋 Sample new BillLineItem records:")
            cursor_local.execute("""
                SELECT id, provider_bill_id, cpt_code, charge_amount FROM BillLineItem l
                WHERE NOT EXISTS (
                    SELECT 1 FROM vm_db.BillLineItem v
                    WHERE l.id = v.id
                )
                LIMIT 3
            """)
            sample_new_bli = cursor_local.fetchall()
            for bli_id, pb_id, cpt, charge in sample_new_bli:
                # Handle case where id might be an integer
                bli_id_str = str(bli_id)[:8] if isinstance(bli_id, str) else str(bli_id)
                pb_id_str = str(pb_id)[:8] if isinstance(pb_id, str) else str(pb_id)
                print(f"  - {bli_id_str}... (bill: {pb_id_str}..., CPT: {cpt}, Charge: {charge})")
        
        # Summary
        print("\n" + "=" * 50)
        print("📈 SUMMARY:")
        print(f"Expected ProviderBill additions: ~31")
        print(f"Actual ProviderBill additions: {new_pb_count}")
        print(f"Expected BillLineItem additions: ~35-40")
        print(f"Actual BillLineItem additions: {new_bli_count}")
        
        print(f"\n💡 ANALYSIS:")
        print(f"- Comparing against LIVE VM database (not backup)")
        print(f"- You have {new_pb_count} more ProviderBill records locally than on VM")
        print(f"- You have {new_bli_count} more BillLineItem records locally than on VM")
        
        if new_pb_count > 0 and new_bli_count > 0:
            print("✅ Ready to proceed with merge - will add your local records to VM")
        else:
            print("⚠️  No new records to merge")
        
        conn_local.close()
        conn_vm.close()
        
    finally:
        # Clean up temporary file
        try:
            Path(vm_db_path).unlink()
            print(f"\n🧹 Cleaned up temporary VM database file")
        except:
            pass

if __name__ == "__main__":
    test_merge_counts() 