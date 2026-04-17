import sqlite3
import pandas as pd
import numpy as np

conn = sqlite3.connect("myntra_analysis.db")

# ── Query 1: Discount distribution overview ────────────────────────────────
# Recreates your finding that 53% of products are discounted more than 70%
discount_dist = pd.read_sql("""
    SELECT 
        CASE 
            WHEN discount_pct < 40              THEN 'Under 40%'
            WHEN discount_pct BETWEEN 40 AND 60 THEN '40-60%'
            WHEN discount_pct BETWEEN 60 AND 70 THEN '60-70%'
            WHEN discount_pct BETWEEN 70 AND 80 THEN '70-80%'
            WHEN discount_pct > 80              THEN 'Above 80%'
        END as discount_bucket,
        COUNT(*) as product_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
    FROM products
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM products)
    GROUP BY discount_bucket
    ORDER BY MIN(discount_pct)
""", conn)
print("=== Discount Distribution (latest day) ===")
print(discount_dist.to_string(index=False))


# ── Query 2: Sort mode comparison ─────────────────────────────────────────
# Recreates your finding about sort mode being a genuine business signal
sort_comparison = pd.read_sql("""
    SELECT
        source_sort,
        COUNT(DISTINCT product_id)      as unique_products,
        ROUND(AVG(discount_pct), 1)     as avg_discount_pct,
        ROUND(AVG(rating), 2)           as avg_rating,
        ROUND(AVG(rating_count), 0)     as avg_rating_count,
        ROUND(AVG(mrp), 0)              as avg_mrp,
        ROUND(AVG(price), 0)            as avg_price
    FROM products
    GROUP BY source_sort
""", conn)
print("\n=== Sort Mode Comparison ===")
print(sort_comparison.to_string(index=False))


# ── Query 3: Brands that appear ONLY in discount sort ─────────────────────
# Recreates your finding about 70 brands with no organic placement
discount_only_brands = pd.read_sql("""
    SELECT 
        brand,
        COUNT(DISTINCT product_id)  as products,
        ROUND(AVG(discount_pct), 1) as avg_discount,
        ROUND(AVG(rating), 2)       as avg_rating
    FROM products
    WHERE brand NOT IN (
        SELECT DISTINCT brand FROM products WHERE source_sort != 'discount'
    )
    GROUP BY brand
    ORDER BY avg_discount DESC
    LIMIT 20
""", conn)
print("\n=== Brands appearing ONLY in discount sort (top 20) ===")
print(discount_only_brands.to_string(index=False))


# ── Query 4: Sell-through by discount bucket ──────────────────────────────
# Recreates your finding that extreme discounts don't clear inventory
df = pd.read_sql("""
    SELECT product_id, snapshot_date, inventory, discount_pct, rating
    FROM products
""", conn)

df = df.sort_values(["product_id", "snapshot_date"])

product_df = df.groupby("product_id").agg(
    first_inventory = ("inventory", "first"),
    last_inventory  = ("inventory", "last"),
    max_inventory   = ("inventory", "max"),
    avg_discount    = ("discount_pct", "mean"),
    avg_rating      = ("rating", "mean"),
    days_present    = ("snapshot_date", "nunique")   # replaces days_present column
).reset_index()

product_df["depletion"]      = (product_df["first_inventory"] - product_df["last_inventory"]).clip(lower=0)
product_df["depletion_rate"] = (product_df["depletion"] / product_df["first_inventory"]).replace([np.inf, -np.inf], np.nan)
product_df["restock_flag"]   = (product_df["max_inventory"] > product_df["first_inventory"])
product_df["sold_anything"]  = (product_df["depletion"] > 0).astype(int)

# ── recreate your discount buckets ────────────────────────────────────────
bins   = [0, 40, 60, 70, 80, 100]
labels = ["Under 40%", "40-60%", "60-70%", "70-80%", "Above 80%"]
product_df["discount_bucket"] = pd.cut(product_df["avg_discount"], bins=bins, labels=labels)

# ── recreate your exact groupby ───────────────────────────────────────────
sell_through = (
    product_df.groupby("discount_bucket", observed=True)
    .agg(
        products             = ("product_id",      "count"),
        sell_through_rate    = ("sold_anything",   "mean"),
        avg_depletion_rate   = ("depletion_rate",  "mean"),
        median_first_inv     = ("first_inventory", "median"),
        restock_pct          = ("restock_flag",    "mean")
    )
    .round(3)
)
sell_through["sell_through_rate_%"] = (sell_through["sell_through_rate"] * 100).round(1)
sell_through["restock_%"]           = (sell_through["restock_pct"] * 100).round(1)

print("=== Sell-through by Discount Bucket ===")
print(sell_through[["products", "sell_through_rate_%", "avg_depletion_rate", "median_first_inv", "restock_%"]].to_string())



# ── Query 5: Brand stability (how many days does a product stay listed) ───
# Recreates your finding that stable products rate higher
stability = pd.read_sql("""
    SELECT
        product_id,
        brand,
        COUNT(DISTINCT snapshot_date)   as days_listed,
        ROUND(AVG(discount_pct), 1)     as avg_discount,
        ROUND(AVG(rating), 2)           as avg_rating,
        MAX(price) - MIN(price)         as price_range,
        MAX(discount_pct) - MIN(discount_pct) as discount_range
    FROM products
    WHERE source_sort = 'popularity'
    GROUP BY product_id, brand
    ORDER BY days_listed DESC
    LIMIT 20
""", conn)
print("\n=== Most stable products (popularity sort) ===")
print(stability.to_string(index=False))


# ── Query 6: Fake-MRP brand detection ────────────────────────────────────
# Recreates your clustering finding — brands with CV=0 never reprice
brand_pricing = pd.read_sql("""
    SELECT
        brand,
        COUNT(DISTINCT product_id)          as products,
        ROUND(AVG(discount_pct), 1)         as avg_discount,
        ROUND(AVG(rating), 2)               as avg_rating,
        MAX(discount_pct) - MIN(discount_pct) as discount_range,
        COUNT(DISTINCT price)               as unique_prices
    FROM products
    GROUP BY brand
    HAVING COUNT(DISTINCT product_id) >= 5
    ORDER BY avg_discount DESC
    LIMIT 30
""", conn)
print("\n=== Brand pricing behaviour (brands with 5+ products) ===")
print(brand_pricing.to_string(index=False))

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
print(result.to_string(index=False))
conn.close()