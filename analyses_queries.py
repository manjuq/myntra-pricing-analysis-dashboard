import sqlite3
import pandas as pd
import numpy as np

conn = sqlite3.connect("myntra_analysis.db")

# ── Query 1: Discount distribution overview ────────────────────────────────
# Finding: 53% of products discounted more than 70% — this is the default pricing model
discount_dist = pd.read_sql("""
    SELECT 
        CASE 
            WHEN discount_pct < 40              THEN '<40%'
            WHEN discount_pct BETWEEN 40 AND 60 THEN '40-60%'
            WHEN discount_pct BETWEEN 60 AND 75 THEN '60-75%'
            WHEN discount_pct > 75              THEN '75%+'
        END AS discount_bucket,
        COUNT(DISTINCT product_id)                              AS unique_products,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)    AS pct_of_catalog
    FROM products
    GROUP BY discount_bucket
    ORDER BY MIN(discount_pct)
""", conn)
print("=== Discount Distribution ===")
print(discount_dist.to_string(index=False))


# ── Query 2: Sort mode comparison ─────────────────────────────────────────
# Finding: Discount sort has 88.8% avg discount and avg rating count of only 107
# vs 983 for Popularity — nearly 10x fewer reviews
sort_comparison = pd.read_sql("""
    SELECT
        source_sort,
        COUNT(DISTINCT product_id)          AS unique_products,
        ROUND(AVG(discount_pct), 1)         AS avg_discount_pct,
        ROUND(MEDIAN(discount_pct), 1)      AS median_discount_pct,
        ROUND(AVG(CASE WHEN rating > 0 THEN rating END), 2) AS avg_rating,
        ROUND(AVG(rating_count), 0)         AS avg_rating_count,
        ROUND(AVG(mrp), 0)                  AS avg_mrp,
        ROUND(AVG(price), 0)                AS avg_price
    FROM products
    GROUP BY source_sort
    ORDER BY avg_discount_pct DESC
""", conn)
print("\n=== Sort Mode Comparison ===")
print(sort_comparison.to_string(index=False))


# ── Query 3: Brands exclusive to discount sort ─────────────────────────────
# Finding: 70 brands never appear in Popularity or Recommended
# — no organic placement, exist only as MRP-inflation vehicles
discount_only_brands = pd.read_sql("""
    SELECT 
        brand,
        COUNT(DISTINCT product_id)          AS unique_products,
        ROUND(AVG(discount_pct), 1)         AS avg_discount,
        ROUND(AVG(CASE WHEN rating > 0 THEN rating END), 2) AS avg_rating,
        ROUND(AVG(rating_count), 0)         AS avg_rating_count
    FROM products
    WHERE brand NOT IN (
        SELECT DISTINCT brand 
        FROM products 
        WHERE source_sort IN ('popularity', 'recommended')
    )
    GROUP BY brand
    HAVING COUNT(DISTINCT product_id) >= 3
    ORDER BY avg_discount DESC
""", conn)
print("\n=== Brands appearing ONLY in Discount sort ===")
print(discount_only_brands.to_string(index=False))


# ── Query 4: Sell-through by discount bucket ──────────────────────────────
# Finding: 75%+ discount products have LOWEST sell-through (36.6%)
# Buckets match the notebook exactly: <40, 40-60, 60-75, 75+
# Computed in pandas because sell-through requires first/last inventory per product
df = pd.read_sql("""
    SELECT product_id, snapshot_date, inventory, discount_pct
    FROM products
    ORDER BY product_id, snapshot_date
""", conn)

product_df = (
    df.groupby("product_id")
    .agg(
        first_inventory=("inventory", "first"),
        last_inventory=("inventory", "last"),
        max_inventory=("inventory", "max"),
        avg_discount=("discount_pct", "mean"),
        days_present=("snapshot_date", "nunique")
    )
    .reset_index()
)

product_df["depletion"]      = (product_df["first_inventory"] - product_df["last_inventory"]).clip(lower=0)
product_df["depletion_rate"] = (product_df["depletion"] / product_df["first_inventory"]).replace([np.inf, -np.inf], np.nan)
product_df["restock_flag"]   = (product_df["max_inventory"] > product_df["first_inventory"])
product_df["sold_anything"]  = (product_df["depletion"] > 0).astype(int)

# buckets aligned with notebook analysis
product_df["discount_bucket"] = pd.cut(
    product_df["avg_discount"],
    bins=[0, 40, 60, 75, 100],
    labels=["<40%", "40-60%", "60-75%", "75%+"]
)

sell_through = (
    product_df.groupby("discount_bucket", observed=True)
    .agg(
        products=("product_id", "count"),
        sell_through_rate=("sold_anything", "mean"),
        avg_depletion_rate=("depletion_rate", "mean"),
        median_first_inventory=("first_inventory", "median"),
        restock_pct=("restock_flag", "mean")
    )
    .round(3)
)
sell_through["sell_through_%"] = (sell_through["sell_through_rate"] * 100).round(1)
sell_through["restock_%"]      = (sell_through["restock_pct"] * 100).round(1)

print("\n=== Sell-Through by Discount Bucket ===")
print(sell_through[["products", "sell_through_%", "avg_depletion_rate",
                     "median_first_inventory", "restock_%"]].to_string())


# ── Query 5: Product continuity cohorts ───────────────────────────────────
# Finding: stable products (15-20 days) carry higher avg discount (72.3%)
# than transient (65.9%) — deep discounts are not being pulled after failing to sell
continuity = pd.read_sql("""
    SELECT
        CASE
            WHEN days_present BETWEEN 1  AND 4  THEN 'Transient (1-4d)'
            WHEN days_present BETWEEN 5  AND 9  THEN 'Short (5-9d)'
            WHEN days_present BETWEEN 10 AND 14 THEN 'Medium (10-14d)'
            WHEN days_present BETWEEN 15 AND 20 THEN 'Stable (15-20d)'
        END AS continuity_cohort,
        COUNT(DISTINCT product_id)          AS unique_products,
        ROUND(AVG(discount_pct), 1)         AS avg_discount_pct,
        ROUND(AVG(CASE WHEN rating > 0 THEN rating END), 3) AS avg_rating,
        ROUND(AVG(rating_count), 0)         AS avg_rating_count
    FROM (
        SELECT
            product_id,
            discount_pct,
            rating,
            rating_count,
            COUNT(DISTINCT snapshot_date) OVER (PARTITION BY product_id) AS days_present
        FROM products
    ) sub
    GROUP BY continuity_cohort
    ORDER BY MIN(days_present)
""", conn)
print("\n=== Product Continuity Cohorts ===")
print(continuity.to_string(index=False))


# ── Query 6: Price volatility — fake-MRP brand detection ──────────────────
# Finding: brands with highest discounts are least likely to reprice (r = -0.44)
# unique_prices = 1 means price never changed across all days scraped
# discount_range close to 0 = permanent label, not a managed discount
brand_volatility = pd.read_sql("""
    SELECT
        brand,
        COUNT(DISTINCT product_id)                          AS products_tracked,
        ROUND(AVG(discount_pct), 1)                         AS avg_discount,
        ROUND(AVG(CASE WHEN rating > 0 THEN rating END), 2) AS avg_rating,
        COUNT(DISTINCT price)                               AS unique_prices_seen,
        ROUND(MAX(discount_pct) - MIN(discount_pct), 1)    AS discount_range,
        ROUND(MAX(price) - MIN(price), 0)                  AS price_range_abs
    FROM products
    GROUP BY brand
    HAVING COUNT(DISTINCT product_id) >= 5
    ORDER BY avg_discount DESC
""", conn)
print("\n=== Brand Pricing Behaviour (5+ products) ===")
print(brand_volatility.to_string(index=False))


# ── Query 7: Stable vs transient — rating comparison by discount bucket ───
# Finding: stable > transient most visible in 40-75% range
# At 75%+, both cohorts drop equally — extreme discounts erode trust regardless
stable_vs_transient = pd.read_sql("""
    SELECT
        disc_bucket,
        cohort,
        COUNT(DISTINCT product_id)          AS unique_products,
        ROUND(AVG(rating), 3)               AS avg_rating,
        ROUND(MEDIAN(rating), 3)            AS median_rating
    FROM (
        SELECT
            product_id,
            rating,
            CASE
                WHEN days_present BETWEEN 1 AND 4   THEN 'Transient (1-4d)'
                WHEN days_present BETWEEN 15 AND 20 THEN 'Stable (15-20d)'
            END AS cohort,
            CASE
                WHEN avg_discount < 40  THEN '<40%'
                WHEN avg_discount < 60  THEN '40-60%'
                WHEN avg_discount < 75  THEN '60-75%'
                ELSE '75%+'
            END AS disc_bucket
        FROM (
            SELECT
                product_id,
                rating,
                COUNT(DISTINCT snapshot_date) OVER (PARTITION BY product_id) AS days_present,
                AVG(discount_pct)             OVER (PARTITION BY product_id) AS avg_discount
            FROM products
            WHERE rating > 0
        ) sub
    ) cohort_data
    WHERE cohort IS NOT NULL
    GROUP BY disc_bucket, cohort
    ORDER BY disc_bucket, cohort
""", conn)
print("\n=== Stable vs Transient Rating by Discount Bucket ===")
print(stable_vs_transient.to_string(index=False))


# ── Query 8: Top brands by sort mode ──────────────────────────────────────
# Useful for dashboard: which brands dominate each sort mode?
brands_by_sort = pd.read_sql("""
    SELECT
        source_sort,
        brand,
        COUNT(DISTINCT product_id)          AS unique_products,
        ROUND(AVG(discount_pct), 1)         AS avg_discount,
        ROUND(AVG(CASE WHEN rating > 0 THEN rating END), 2) AS avg_rating,
        ROUND(AVG(rating_count), 0)         AS avg_rating_count
    FROM products
    GROUP BY source_sort, brand
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY source_sort 
        ORDER BY COUNT(DISTINCT product_id) DESC
    ) <= 10
    ORDER BY source_sort, unique_products DESC
""", conn)
print("\n=== Top 10 Brands per Sort Mode ===")
print(brands_by_sort.to_string(index=False))


# ── Query 9: Daily discount trend by sort mode ────────────────────────────
# Shows whether discounts deepened over the 20-day window
daily_trend = pd.read_sql("""
    SELECT
        snapshot_date,
        source_sort,
        COUNT(DISTINCT product_id)          AS products_listed,
        ROUND(AVG(discount_pct), 1)         AS avg_discount,
        ROUND(AVG(CASE WHEN rating > 0 THEN rating END), 3) AS avg_rating,
        ROUND(AVG(inventory), 0)            AS avg_inventory
    FROM products
    GROUP BY snapshot_date, source_sort
    ORDER BY snapshot_date, source_sort
""", conn)
print("\n=== Daily Trend by Sort Mode ===")
print(daily_trend.to_string(index=False))


conn.close()
