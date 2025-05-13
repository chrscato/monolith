import sqlite3

def copy_all_tables(source_path=r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\bill_review2\filemaker.db", target_path=r'C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\monolith\cdx_ehr\monolith.db'):
    src_conn = sqlite3.connect(source_path)
    tgt_conn = sqlite3.connect(target_path)
    src_cursor = src_conn.cursor()
    tgt_cursor = tgt_conn.cursor()

    # Get all table names from source
    src_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in src_cursor.fetchall()]

    for table in tables:
        # Skip sqlite_sequence as it's a system table that SQLite manages automatically
        if table == 'sqlite_sequence':
            print(f"Skipping system table: {table}")
            continue

        print(f"Copying table: {table}")

        # Drop existing table if it exists
        tgt_cursor.execute(f"DROP TABLE IF EXISTS {table}")

        # Copy create statement
        src_cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'")
        create_sql = src_cursor.fetchone()[0]
        tgt_cursor.execute(create_sql)

        # Copy data
        src_cursor.execute(f"SELECT * FROM {table}")
        rows = src_cursor.fetchall()
        if rows:
            placeholders = ",".join("?" * len(rows[0]))
            tgt_cursor.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)

    tgt_conn.commit()
    src_conn.close()
    tgt_conn.close()
    print(f"✅ All tables copied from {source_path} to {target_path}")

if __name__ == "__main__":
    copy_all_tables()
