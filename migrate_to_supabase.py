import pandas as pd
import sqlite3
from sqlalchemy import create_engine

# ── 1. read from your local sqlite ─────────────────────────────────────────
sqlite_conn = sqlite3.connect("myntra_analysis.db")
df = pd.read_sql("SELECT * FROM products", sqlite_conn)
sqlite_conn.close()

print(f"Read {len(df)} rows from SQLite")

# ── 2. connect to supabase ──────────────────────────────────────────────────
# paste your connection string here — replace [YOUR-PASSWORD] with actual password
SUPABASE_URL = "postgresql://postgres:81H6DBXC8JTp&&@db.posyurevmljdzuufzrka.supabase.co:5432/postgres"
engine = create_engine(SUPABASE_URL)

# ── 3. push data to supabase ────────────────────────────────────────────────
df.to_sql(
    name="products",
    con=engine,
    if_exists="replace",    # creates table if not exists, replaces if it does
    index=False,
    chunksize=500,           # smaller chunks — supabase free tier is slower
    method="multi"
)

print(f"Successfully migrated {len(df)} rows to Supabase")

# ── 4. quick verify ─────────────────────────────────────────────────────────
result = pd.read_sql("""
    SELECT 
        source_sort,
        COUNT(*) as row_count,
        ROUND(AVG(discount_pct)::numeric, 1) as avg_discount_pct
    FROM products
    GROUP BY source_sort
""", engine)

print("\nVerification:")
print(result.to_string(index=False))