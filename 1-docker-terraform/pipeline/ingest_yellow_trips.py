#!/usr/bin/env python
"""Ingest yellow taxi parquet data with proper PostgreSQL dtypes."""
import os
import io
import pandas as pd
import requests
import psycopg

URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"

def map_pandas_to_postgres(dtype):
    """Map pandas dtype to PostgreSQL type."""
    dtype_str = str(dtype)
    
    if 'datetime' in dtype_str or 'datetime64' in dtype_str:
        return 'TIMESTAMP'
    elif 'int64' in dtype_str:
        return 'BIGINT'
    elif 'int32' in dtype_str or 'int' in dtype_str:
        return 'INTEGER'
    elif 'float' in dtype_str:
        return 'NUMERIC(12,2)'
    elif 'bool' in dtype_str:
        return 'BOOLEAN'
    else:
        return 'TEXT'


def main():
    pg_user = os.getenv("PG_USER", "root")
    pg_pass = os.getenv("PG_PASS", "root")
    pg_host = os.getenv("PG_HOST", "pgdatabase")
    pg_port = int(os.getenv("PG_PORT", "5432"))
    pg_db = os.getenv("PG_DB", "ny_taxi")
    table = os.getenv("TARGET_TABLE", "yellow_taxi_trips")
    
    conninfo = f"host={pg_host} port={pg_port} dbname={pg_db} user={pg_user} password={pg_pass}"
    
    print(f"📊 Loading Yellow Taxi Data...")
    print(f"   Downloading from {URL}")
    
    # Download parquet
    r = requests.get(URL, timeout=120)
    r.raise_for_status()
    print(f"   Downloaded {len(r.content):,} bytes")
    
    # Read parquet
    df = pd.read_parquet(io.BytesIO(r.content))
    print(f"   DataFrame shape: {df.shape}")
    
    if df.shape[0] == 0:
        print(f"   ⚠️  Empty dataframe, skipping")
        return
    
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
            pg_type = map_pandas_to_postgres(df[col].dtype)
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