import sqlite3
import pandas as pd
import os
from glob import glob
from pathlib import Path

# ── 1. Connect (creates the file if it doesn't exist) ──────────────────────
conn = sqlite3.connect("myntra_analysis.db")
cursor = conn.cursor()

# ── 2. Create the table ────────────────────────────────────────────────────
cursor.executescript("""
    CREATE TABLE IF NOT EXISTS products (
        -- identifiers
        product_id              INTEGER,
        brand                   TEXT,
        gender                  TEXT,
        category                TEXT,
        master_category         TEXT,
        sub_category            TEXT,
        article_type            TEXT,

        -- pricing
        mrp                     INTEGER,
        price                   INTEGER,
        discount                INTEGER,
        discount_type           REAL,
        discount_display_label  TEXT,
        coupon_discount         REAL,
        best_price              REAL,

        -- engagement
        rating                  REAL,
        rating_count            INTEGER,
        list_views              INTEGER,
        inventory               INTEGER,
        available               INTEGER,   -- stored as 0/1 (SQLite has no bool)

        -- product details
        sizes                   TEXT,
        has_multiple_sizes      INTEGER,
        season                  TEXT,
        is_fast_fashion         INTEGER,
        promotion_tags          TEXT,
        has_promotion           INTEGER,
        year                    INTEGER,
        preferred_delivery_tag  TEXT,
        delivery_promise        TEXT,

        -- tracking columns (most important for your analysis)
        snapshot_date           TEXT,      -- format: YYYY-MM-DD
        source_sort             TEXT,      -- 'popularity', 'discount', 'recommended'

        -- composite primary key: same product can appear on multiple days/sorts
        PRIMARY KEY (product_id, snapshot_date, source_sort)
    )
""")
conn.commit()
print("Table created successfully.")

# ── 3. Load all your CSV files into the database ──────────────────────────

# Option A: if you have ONE combined CSV (recommended — use this)
df = pd.concat((pd.read_csv(f) for f in glob("../ecommerce_product_scraping/data/raw/*")), ignore_index=True)

# Option B: if you want to load all 20 daily files at once
# folder = Path("your_daily_csvs_folder")
# df = pd.concat([pd.read_csv(f) for f in folder.glob("*.csv")], ignore_index=True)

# clean up booleans — SQLite stores them as 0/1
bool_cols = ["available", "has_multiple_sizes", "is_fast_fashion", "has_promotion"]
for col in bool_cols:
    df[col] = df[col].astype(int)

df = df.drop_duplicates(subset=["product_id", "snapshot_date", "source_sort"], keep="first")

df["discount_pct"] = ((df["mrp"] - df["price"]) / df["mrp"] * 100).round(1)

# make sure snapshot_date is a clean string like '2024-01-15'
df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.strftime("%Y-%m-%d")

# load into SQLite — 'replace' on conflict so re-running is safe
df.to_sql("products", conn, if_exists="replace", index=False)

# re-add the indexes since 'replace' wiped them
cursor.executescript("""
    CREATE INDEX IF NOT EXISTS idx_date   ON products (snapshot_date);
    CREATE INDEX IF NOT EXISTS idx_brand  ON products (brand);
    CREATE INDEX IF NOT EXISTS idx_sort   ON products (source_sort);
    CREATE INDEX IF NOT EXISTS idx_rating ON products (rating);
""")
conn.commit()
print(f"Loaded {len(df)} rows into the database.")

# ── 4. Quick sanity check ─────────────────────────────────────────────────

result = pd.read_sql("""
    SELECT 
        source_sort,
        COUNT(*) as row_count,
        COUNT(DISTINCT product_id) as unique_products,
        COUNT(DISTINCT snapshot_date) as days_covered,
        ROUND(AVG(discount_pct), 1) as avg_discount_pct
    FROM products
    GROUP BY source_sort
""", conn)

print("\nData summary by sort mode:")
print(result.to_string(index=False))

conn.close()