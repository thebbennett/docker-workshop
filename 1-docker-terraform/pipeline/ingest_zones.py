#!/usr/bin/env python
"""Ingest taxi zone lookup CSV with proper PostgreSQL dtypes."""
import os
import io
import pandas as pd
import requests
import psycopg

URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

def main():
    pg_user = os.getenv("PG_USER", "root")
    pg_pass = os.getenv("PG_PASS", "root")
    pg_host = os.getenv("PG_HOST", "pgdatabase")
    pg_port = int(os.getenv("PG_PORT", "5432"))
    pg_db = os.getenv("PG_DB", "ny_taxi")
    table = os.getenv("TARGET_TABLE", "taxi_zones")
    
    conninfo = f"host={pg_host} port={pg_port} dbname={pg_db} user={pg_user} password={pg_pass}"
    
    print(f"📊 Loading Taxi Zone Data...")
    print(f"   Downloading from {URL}")
    
    # Download CSV
    r = requests.get(URL, timeout=120)
    r.raise_for_status()
    print(f"   Downloaded {len(r.content):,} bytes")
    
    # Read CSV
    df = pd.read_csv(io.StringIO(r.text))
    print(f"   DataFrame shape: {df.shape}")
    
    if df.shape[0] == 0:
        print(f"   ⚠️  Empty dataframe, skipping")
        return
    
    # Light normalization
    df.columns = df.columns.str.strip().str.lower()
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()
    
    # Connect
    conn = psycopg.connect(conninfo, autocommit=False)
    
    try:
        cur = conn.cursor()
        
        # Drop table
        print(f"   Dropping table {table} if exists...")
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')
        
        # Create table with proper types
        columns = []
        for col in df.columns:
            if df[col].dtype == 'int64':
                pg_type = 'INTEGER'
            elif df[col].dtype == 'float64':
                pg_type = 'NUMERIC(12,2)'
            else:
                pg_type = 'TEXT'
            columns.append(f'"{col}" {pg_type}')
        
        create_sql = f'CREATE TABLE "{table}" ({", ".join(columns)})'
        print(f"   Creating table with typed columns...")
        cur.execute(create_sql)
        
        # Insert data using COPY
        print(f"   Copying {len(df):,} rows...")
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False, na_rep='\\N')
        buf.seek(0)
        
        column_list = ', '.join([f'"{col}"' for col in df.columns])
        copy_sql = f'COPY "{table}" ({column_list}) FROM STDIN WITH (FORMAT CSV, NULL \'\\N\')'
        
        with cur.copy(copy_sql) as copy:
            while True:
                chunk = buf.read(8192)
                if not chunk:
                    break
                copy.write(chunk)
        
        # Verify before commit
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        count_before = cur.fetchone()[0]
        print(f"   Count before commit: {count_before:,}")
        
        # COMMIT
        print(f"   Committing...")
        conn.commit()
        
        # Verify after commit
        cur.execute(f'SELECT COUNT(*) FROM "{table}"')
        count_after = cur.fetchone()[0]
        print(f"   ✓ Count after commit: {count_after:,}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        conn.rollback()
        conn.close()
        raise

if __name__ == "__main__":
    main()
