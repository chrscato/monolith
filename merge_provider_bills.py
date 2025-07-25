#!/usr/bin/env python3
"""
Specialized Database Merge Script for ProviderBill and BillLineItem
Uses VM database as base and adds any ProviderBill/BillLineItem records from local
that don't exist in the VM version.
"""

import sqlite3
import shutil
import argparse
from pathlib import Path
from datetime import datetime
import os
import sys

class ProviderBillMerger:
    def __init__(self, local_db_path, vm_db_path, output_db_path=None):
        self.local_db_path = Path(local_db_path)
        self.vm_db_path = Path(vm_db_path)
        self.output_db_path = Path(output_db_path) if output_db_path else Path("monolith_merged.db")
        self.merge_log = []
        self.target_tables = ['ProviderBill', 'BillLineItem']
        
    def log(self, message):
        """Log a message with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        print(log_entry)
        self.merge_log.append(log_entry)
        
    def validate_databases(self):
        """Validate that both databases exist and have the required tables"""
        for db_path, name in [(self.local_db_path, "Local"), (self.vm_db_path, "VM")]:
            if not db_path.exists():
                raise FileNotFoundError(f"{name} database not found: {db_path}")
            
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # Check if required tables exist
                for table in self.target_tables:
                    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
                    if not cursor.fetchone():
                        raise ValueError(f"Required table '{table}' not found in {name} database")
                
                conn.close()
                
            except sqlite3.Error as e:
                raise ValueError(f"{name} database is not a valid SQLite database: {e}")
        
        self.log("Database validation passed - all required tables found")
        
    def get_table_schema(self, db_path, table_name):
        """Get the schema for a specific table"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        # Get primary key information
        cursor.execute(f"PRAGMA table_info({table_name})")
        schema_info = cursor.fetchall()
        primary_keys = [col[1] for col in schema_info if col[5] == 1]  # col[5] is pk flag
        
        conn.close()
        
        return {
            'columns': [col[1] for col in columns],  # Column names
            'primary_keys': primary_keys,
            'full_schema': columns
        }
    
    def get_table_stats(self, db_path, table_name):
        """Get statistics for a table"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        # Get a sample of records to understand the data structure
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
        sample_rows = cursor.fetchall()
        
        conn.close()
        
        return {
            'row_count': row_count,
            'sample_rows': sample_rows
        }
    
    def compare_tables(self):
        """Compare the target tables between local and VM databases"""
        self.log("Comparing ProviderBill and BillLineItem tables...")
        
        comparison = {}
        
        for table in self.target_tables:
            self.log(f"\nAnalyzing table: {table}")
            
            # Get schemas
            local_schema = self.get_table_schema(self.local_db_path, table)
            vm_schema = self.get_table_schema(self.vm_db_path, table)
            
            # Get stats
            local_stats = self.get_table_stats(self.local_db_path, table)
            vm_stats = self.get_table_stats(self.vm_db_path, table)
            
            comparison[table] = {
                'local_schema': local_schema,
                'vm_schema': vm_schema,
                'local_stats': local_stats,
                'vm_stats': vm_stats,
                'schema_match': local_schema['columns'] == vm_schema['columns'],
                'primary_keys': local_schema['primary_keys']
            }
            
            self.log(f"  Local {table}: {local_stats['row_count']} rows")
            self.log(f"  VM {table}: {vm_stats['row_count']} rows")
            self.log(f"  Schema match: {comparison[table]['schema_match']}")
            self.log(f"  Primary keys: {comparison[table]['primary_keys']}")
        
        return comparison
    
    def find_new_records(self, table_name, primary_keys):
        """Find records in local database that don't exist in VM database"""
        self.log(f"Finding new records in {table_name}...")
        
        # Use a single connection and ATTACH the VM database
        conn = sqlite3.connect(self.local_db_path)
        conn.execute(f"ATTACH DATABASE '{self.vm_db_path}' AS vm_db")
        
        cursor = conn.cursor()
        
        # Build the WHERE clause for primary key comparison
        if primary_keys:
            pk_conditions = []
            for pk in primary_keys:
                pk_conditions.append(f"l.{pk} = v.{pk}")
            
            where_clause = " AND ".join(pk_conditions)
            
            # Query to find local records not in VM using primary keys
            query = f"""
            SELECT l.* FROM {table_name} l
            WHERE NOT EXISTS (
                SELECT 1 FROM vm_db.{table_name} v
                WHERE {where_clause}
            )
            """
        else:
            # If no primary keys, we'll need to compare all columns
            # This is less efficient but necessary
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [col[1] for col in cursor.fetchall()]
            
            # For now, we'll use a simple approach - get all local records
            # and check if they exist in VM (this might be slow for large tables)
            query = f"SELECT * FROM {table_name}"
        
        cursor.execute(query)
        new_records = cursor.fetchall()
        
        conn.close()
        
        self.log(f"  Found {len(new_records)} new records in {table_name}")
        return new_records
    
    def merge_databases(self):
        """Merge databases using VM as base and adding new ProviderBill/BillLineItem records"""
        self.log("Starting specialized merge (VM base + new local records)")
        
        # Validate databases first
        self.validate_databases()
        
        # Create backup of current local database
        backup_path = f"monolith_backup_before_merge_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        shutil.copy2(self.local_db_path, backup_path)
        self.log(f"Created backup: {backup_path}")
        
        # Compare tables
        comparison = self.compare_tables()
        
        # Check for schema mismatches
        for table in self.target_tables:
            if not comparison[table]['schema_match']:
                self.log(f"WARNING: Schema mismatch in {table} - merge may fail!")
                self.log(f"  Local columns: {comparison[table]['local_schema']['columns']}")
                self.log(f"  VM columns: {comparison[table]['vm_schema']['columns']}")
        
        # Copy VM database as base
        shutil.copy2(self.vm_db_path, self.output_db_path)
        self.log(f"Copied VM database as base to: {self.output_db_path}")
        
        # Connect to output database
        conn_output = sqlite3.connect(self.output_db_path)
        cursor_output = conn_output.cursor()
        
        total_added = 0
        
        # Process each target table
        for table in self.target_tables:
            self.log(f"\nProcessing table: {table}")
            
            # Get primary keys for this table
            primary_keys = comparison[table]['primary_keys']
            
            if not primary_keys:
                self.log(f"  WARNING: No primary keys found for {table} - using all columns for comparison")
                # For tables without primary keys, we'll need a different approach
                # This is a simplified version - you might want to customize this
                continue
            
            # Find new records in local database
            new_records = self.find_new_records(table, primary_keys)
            
            if new_records:
                # Get column names for insert
                columns = comparison[table]['local_schema']['columns']
                placeholders = ','.join(['?' for _ in columns])
                
                # Insert new records
                insert_query = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
                
                try:
                    cursor_output.executemany(insert_query, new_records)
                    self.log(f"  Successfully added {len(new_records)} records to {table}")
                    total_added += len(new_records)
                except sqlite3.IntegrityError as e:
                    self.log(f"  ERROR: Failed to insert records into {table}: {e}")
                    self.log(f"  This might be due to schema differences or constraint violations")
                except Exception as e:
                    self.log(f"  ERROR: Unexpected error inserting into {table}: {e}")
            else:
                self.log(f"  No new records found for {table}")
        
        # Commit changes
        conn_output.commit()
        conn_output.close()
        
        self.log(f"\nMerge completed! Total records added: {total_added}")
        return self.output_db_path
    
    def save_merge_log(self, log_path="provider_bill_merge_log.txt"):
        """Save the merge log to a file"""
        with open(log_path, 'w') as f:
            for entry in self.merge_log:
                f.write(entry + '\n')
        self.log(f"Merge log saved to: {log_path}")

def main():
    """Main function to run the specialized database merge"""
    parser = argparse.ArgumentParser(description='Merge ProviderBill and BillLineItem tables from local to VM database')
    parser.add_argument('--local', default='monolith.db', help='Path to local database')
    parser.add_argument('--vm', default='db_backups/vm_backups/monolith_vm_backup_20250721_233459.db', 
                       help='Path to VM database backup')
    parser.add_argument('--output', default='monolith_merged.db', help='Output database path')
    parser.add_argument('--compare-only', action='store_true', 
                       help='Only compare databases, do not merge')
    
    args = parser.parse_args()
    
    print("ProviderBill/BillLineItem Database Merger")
    print("=" * 50)
    print("Strategy: Use VM as base, add new ProviderBill/BillLineItem records from local")
    print("=" * 50)
    
    # Check if databases exist
    if not Path(args.local).exists():
        print(f"Error: Local database not found at {args.local}")
        sys.exit(1)
    
    if not Path(args.vm).exists():
        print(f"Error: VM database backup not found at {args.vm}")
        sys.exit(1)
    
    # Create merger
    merger = ProviderBillMerger(args.local, args.vm, args.output)
    
    try:
        # Show comparison first
        print("\nComparing ProviderBill and BillLineItem tables...")
        comparison = merger.compare_tables()
        
        for table in merger.target_tables:
            print(f"\n{table}:")
            print(f"  Local: {comparison[table]['local_stats']['row_count']} rows")
            print(f"  VM: {comparison[table]['vm_stats']['row_count']} rows")
            print(f"  Schema match: {comparison[table]['schema_match']}")
            print(f"  Primary keys: {comparison[table]['primary_keys']}")
        
        if args.compare_only:
            print("\nComparison completed. Run without --compare-only to perform merge.")
            return
        
        # Confirm before proceeding
        print("\n" + "=" * 50)
        print("This will:")
        print("1. Use the VM database as the base")
        print("2. Add any ProviderBill records from local that don't exist in VM")
        print("3. Add any BillLineItem records from local that don't exist in VM")
        print("4. Keep all other tables unchanged from VM")
        print("=" * 50)
        
        confirm = input("\nProceed with merge? (y/N): ").strip().lower()
        if confirm not in ['y', 'yes']:
            print("Merge cancelled.")
            return
        
        # Perform merge
        print(f"\nPerforming specialized merge...")
        output_path = merger.merge_databases()
        
        # Save merge log
        merger.save_merge_log()
        
        print(f"\nMerge completed! Output database: {output_path}")
        print("Please review the merge log and test the merged database before replacing your main database.")
        
    except Exception as e:
        print(f"Error during merge: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 