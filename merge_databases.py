#!/usr/bin/env python3
"""
Database Merge Script for Monolith
Merges local and VM databases to resolve conflicts when local updates were made
without pulling latest VM changes first.
"""

import sqlite3
import shutil
from pathlib import Path
from datetime import datetime
import os

class DatabaseMerger:
    def __init__(self, local_db_path, vm_db_path, output_db_path=None):
        self.local_db_path = Path(local_db_path)
        self.vm_db_path = Path(vm_db_path)
        self.output_db_path = Path(output_db_path) if output_db_path else Path("monolith_merged.db")
        self.merge_log = []
        
    def log(self, message):
        """Log a message with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        print(log_entry)
        self.merge_log.append(log_entry)
        
    def get_table_info(self, db_path):
        """Get table information from a database"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        table_info = {}
        for table in tables:
            table_name = table[0]
            
            # Get table schema
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            table_info[table_name] = {
                'columns': columns,
                'row_count': row_count
            }
            
        conn.close()
        return table_info
    
    def compare_databases(self):
        """Compare the structure and content of both databases"""
        self.log("Comparing database structures...")
        
        local_info = self.get_table_info(self.local_db_path)
        vm_info = self.get_table_info(self.vm_db_path)
        
        all_tables = set(local_info.keys()) | set(vm_info.keys())
        
        comparison = {
            'local_only': [],
            'vm_only': [],
            'common': [],
            'different_schema': [],
            'different_row_count': []
        }
        
        for table in all_tables:
            if table in local_info and table not in vm_info:
                comparison['local_only'].append(table)
            elif table in vm_info and table not in local_info:
                comparison['vm_only'].append(table)
            elif table in local_info and table in vm_info:
                comparison['common'].append(table)
                
                # Check for schema differences
                local_cols = local_info[table]['columns']
                vm_cols = vm_info[table]['columns']
                
                if local_cols != vm_cols:
                    comparison['different_schema'].append(table)
                
                # Check for row count differences
                local_count = local_info[table]['row_count']
                vm_count = vm_info[table]['row_count']
                
                if local_count != vm_count:
                    comparison['different_row_count'].append({
                        'table': table,
                        'local_count': local_count,
                        'vm_count': vm_count
                    })
        
        return comparison, local_info, vm_info
    
    def merge_databases(self, strategy='smart'):
        """
        Merge the databases using the specified strategy
        
        Strategies:
        - 'local_wins': Use local database as base, add VM-only tables
        - 'vm_wins': Use VM database as base, add local-only tables  
        - 'smart': Intelligent merge based on table types and timestamps
        """
        self.log(f"Starting database merge with strategy: {strategy}")
        
        # Create backup of current local database
        backup_path = f"monolith_backup_before_merge_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        shutil.copy2(self.local_db_path, backup_path)
        self.log(f"Created backup: {backup_path}")
        
        # Compare databases
        comparison, local_info, vm_info = self.compare_databases()
        
        # Print comparison results
        self.log("Database comparison results:")
        self.log(f"  Tables only in local: {comparison['local_only']}")
        self.log(f"  Tables only in VM: {comparison['vm_only']}")
        self.log(f"  Common tables: {len(comparison['common'])}")
        self.log(f"  Tables with different schema: {comparison['different_schema']}")
        
        for diff in comparison['different_row_count']:
            self.log(f"  {diff['table']}: local={diff['local_count']}, vm={diff['vm_count']}")
        
        # Choose base database based on strategy
        if strategy == 'local_wins':
            base_db = self.local_db_path
            other_db = self.vm_db_path
            self.log("Using local database as base")
        elif strategy == 'vm_wins':
            base_db = self.vm_db_path
            other_db = self.local_db_path
            self.log("Using VM database as base")
        else:  # smart strategy
            # For smart strategy, use the database with more recent changes
            # This is a simple heuristic - you might want to customize this
            base_db = self.local_db_path
            other_db = self.vm_db_path
            self.log("Using smart strategy - local database as base")
        
        # Copy base database to output
        shutil.copy2(base_db, self.output_db_path)
        self.log(f"Copied base database to: {self.output_db_path}")
        
        # Merge tables from other database
        self.merge_tables_from_other_db(other_db, comparison)
        
        self.log("Database merge completed!")
        return self.output_db_path
    
    def merge_tables_from_other_db(self, other_db_path, comparison):
        """Merge tables from the other database into the output database"""
        conn_output = sqlite3.connect(self.output_db_path)
        conn_other = sqlite3.connect(other_db_path)
        
        # Add tables that only exist in the other database
        for table in comparison['vm_only'] if other_db_path == self.vm_db_path else comparison['local_only']:
            self.log(f"Adding table from other database: {table}")
            
            # Get table schema
            cursor_other = conn_other.cursor()
            cursor_other.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'")
            create_sql = cursor_other.fetchone()[0]
            
            # Create table in output database
            cursor_output = conn_output.cursor()
            cursor_output.execute(create_sql)
            
            # Copy data
            cursor_other.execute(f"SELECT * FROM {table}")
            rows = cursor_other.fetchall()
            
            if rows:
                # Get column names
                cursor_other.execute(f"PRAGMA table_info({table})")
                columns = [col[1] for col in cursor_other.fetchall()]
                placeholders = ','.join(['?' for _ in columns])
                
                # Insert data
                cursor_output.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)
                self.log(f"  Copied {len(rows)} rows to {table}")
        
        # For common tables with different row counts, we need more sophisticated merging
        # This is a simplified approach - you might need to customize based on your data
        for diff in comparison['different_row_count']:
            table = diff['table']
            self.log(f"Table {table} has different row counts - manual review may be needed")
            
            # For now, we'll keep the base database's data
            # You might want to implement more sophisticated merging logic here
            self.log(f"  Keeping base database data for {table}")
        
        conn_output.commit()
        conn_output.close()
        conn_other.close()
    
    def save_merge_log(self, log_path="merge_log.txt"):
        """Save the merge log to a file"""
        with open(log_path, 'w') as f:
            for entry in self.merge_log:
                f.write(entry + '\n')
        self.log(f"Merge log saved to: {log_path}")

def main():
    """Main function to run the database merge"""
    print("Monolith Database Merger")
    print("=" * 50)
    
    # Define database paths
    local_db = "monolith.db"
    vm_db = "db_backups/vm_backups/monolith_vm_backup_20250721_233459.db"
    
    # Check if databases exist
    if not Path(local_db).exists():
        print(f"Error: Local database not found at {local_db}")
        return
    
    if not Path(vm_db).exists():
        print(f"Error: VM database backup not found at {vm_db}")
        return
    
    # Create merger
    merger = DatabaseMerger(local_db, vm_db)
    
    # Show comparison first
    print("\nComparing databases...")
    comparison, _, _ = merger.compare_databases()
    
    print(f"\nLocal-only tables: {comparison['local_only']}")
    print(f"VM-only tables: {comparison['vm_only']}")
    print(f"Common tables: {len(comparison['common'])}")
    print(f"Tables with different schema: {comparison['different_schema']}")
    
    for diff in comparison['different_row_count']:
        print(f"{diff['table']}: local={diff['local_count']}, vm={diff['vm_count']}")
    
    # Ask user for merge strategy
    print("\nMerge strategies:")
    print("1. local_wins - Use local database as base, add VM-only tables")
    print("2. vm_wins - Use VM database as base, add local-only tables")
    print("3. smart - Intelligent merge (currently same as local_wins)")
    
    strategy = input("\nChoose merge strategy (1/2/3) [default: 1]: ").strip()
    
    if strategy == "2":
        strategy = "vm_wins"
    elif strategy == "3":
        strategy = "smart"
    else:
        strategy = "local_wins"
    
    # Perform merge
    print(f"\nPerforming merge with strategy: {strategy}")
    output_path = merger.merge_databases(strategy)
    
    # Save merge log
    merger.save_merge_log()
    
    print(f"\nMerge completed! Output database: {output_path}")
    print("Please review the merge log and test the merged database before replacing your main database.")

if __name__ == "__main__":
    main() 