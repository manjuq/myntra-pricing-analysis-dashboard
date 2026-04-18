import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text

# ── connection ───────────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    url = st.secrets["SUPABASE_URL"]
    return create_engine(url)

@st.cache_data
def run_query(query, params=None):
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn, params=params)

# ── page config ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="Myntra Women's Dresses Analysis", layout="wide")
st.title("Myntra Women's Dresses — Pricing & Discount Analysis")
st.caption("54,000 product snapshots · 20 days · 3 sort modes (Jan 12–31, 2026)")

# ── row 1: top-level metrics ─────────────────────────────────────────────────
summary = run_query("""
    SELECT
        COUNT(DISTINCT product_id)                                      AS total_products,
        COUNT(DISTINCT brand)                                           AS total_brands,
        COUNT(DISTINCT snapshot_date)                                   AS days_tracked,
        ROUND(AVG(discount_pct)::numeric, 1)                           AS avg_discount,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
              (ORDER BY discount_pct)::numeric, 1)                     AS median_discount,
        ROUND(AVG(CASE WHEN rating > 0 THEN rating END)::numeric, 2)  AS avg_rating
    FROM products
""")

col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("Unique Products",   f"{summary['total_products'][0]:,}")
col2.metric("Brands",            f"{summary['total_brands'][0]:,}")
col3.metric("Days Tracked",      summary['days_tracked'][0])
col4.metric("Avg Discount",      f"{summary['avg_discount'][0]}%")
col5.metric("Median Discount",   f"{summary['median_discount'][0]}%",
            help="Median is 72% — half the catalog is discounted more than this")
col6.metric("Avg Rating",        summary['avg_rating'][0],
            help="Rated products only (rating > 0)")

st.divider()

# ── row 2: discount distribution ─────────────────────────────────────────────
st.subheader("How is discount % distributed across the catalog?")
st.caption("72% median discount means MRP is largely a fictional anchor in this category")

dist_data = run_query("""
    SELECT
        CASE
            WHEN discount_pct < 40              THEN '<40%'
            WHEN discount_pct BETWEEN 40 AND 60 THEN '40–60%'
            WHEN discount_pct BETWEEN 60 AND 75 THEN '60–75%'
            WHEN discount_pct > 75              THEN '75%+'
        END AS discount_bucket,
        COUNT(DISTINCT product_id) AS unique_products
    FROM products
    GROUP BY discount_bucket
    ORDER BY MIN(discount_pct)
""")

fig_dist = px.bar(
    dist_data,
    x="discount_bucket",
    y="unique_products",
    text="unique_products",
    labels={"discount_bucket": "Discount Range", "unique_products": "Unique Products"},
    color="discount_bucket",
    color_discrete_sequence=["#2a9d8f", "#457b9d", "#e9c46a", "#e76f51"]
)
fig_dist.update_traces(textposition="outside")
fig_dist.update_layout(showlegend=False)
st.plotly_chart(fig_dist, use_container_width=True)

st.divider()

# ── row 3: sort mode comparison ───────────────────────────────────────────────
st.subheader("Discount % by sort mode")
st.caption("Discount sort is a different product pool — not just a reordering of the same catalog")

col_box, col_table = st.columns([2, 1])

with col_box:
    sort_raw = run_query("SELECT source_sort, discount_pct FROM products")
    fig_sort = px.box(
        sort_raw,
        x="source_sort",
        y="discount_pct",
        color="source_sort",
        labels={"source_sort": "Sort mode", "discount_pct": "Discount %"},
        color_discrete_map={
            "discount":    "#e76f51",
            "popularity":  "#2a9d8f",
            "recommended": "#457b9d"
        }
    )
    fig_sort.update_layout(showlegend=False)
    st.plotly_chart(fig_sort, use_container_width=True)

with col_table:
    sort_summary = run_query("""
        SELECT
            source_sort                                                         AS sort_mode,
            COUNT(DISTINCT product_id)                                          AS products,
            ROUND(AVG(discount_pct)::numeric, 1)                               AS avg_discount,
            ROUND(AVG(CASE WHEN rating > 0 THEN rating END)::numeric, 2)       AS avg_rating,
            ROUND(AVG(rating_count)::numeric, 0)                               AS avg_reviews
        FROM products
        GROUP BY source_sort
        ORDER BY avg_discount DESC
    """)
    st.dataframe(sort_summary, use_container_width=True, hide_index=True)
    st.caption("Discount sort: avg 107 reviews vs 983 for Popularity — 10x fewer, reflecting low genuine demand")

st.divider()

# ── row 4: sell-through analysis ─────────────────────────────────────────────
st.subheader("Do heavy discounts actually clear inventory?")
st.caption("Counter-intuitive finding: 75%+ discount products have the LOWEST sell-through rate")

sellthrough_data = run_query("""
    WITH product_inventory AS (
        SELECT
            product_id,
            FIRST_VALUE(inventory) OVER (
                PARTITION BY product_id ORDER BY snapshot_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS first_inventory,
            LAST_VALUE(inventory) OVER (
                PARTITION BY product_id ORDER BY snapshot_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS last_inventory,
            MAX(inventory) OVER (PARTITION BY product_id) AS max_inventory,
            AVG(discount_pct) OVER (PARTITION BY product_id) AS avg_discount
        FROM products
    ),
    product_level AS (
        SELECT DISTINCT
            product_id,
            first_inventory,
            last_inventory,
            max_inventory,
            avg_discount,
            GREATEST(first_inventory - last_inventory, 0) AS depletion,
            CASE WHEN max_inventory > first_inventory THEN 1 ELSE 0 END AS restocked,
            CASE WHEN GREATEST(first_inventory - last_inventory, 0) > 0 THEN 1 ELSE 0 END AS sold_anything
        FROM product_inventory
    )
    SELECT
        CASE
            WHEN avg_discount < 40  THEN '<40%'
            WHEN avg_discount < 60  THEN '40–60%'
            WHEN avg_discount < 75  THEN '60–75%'
            ELSE '75%+'
        END AS discount_bucket,
        COUNT(*)                                    AS products,
        ROUND(AVG(sold_anything) * 100, 1)         AS sell_through_pct,
        ROUND(AVG(restocked) * 100, 1)             AS restock_pct,
        ROUND(MEDIAN(first_inventory)::numeric, 0) AS median_start_inventory
    FROM product_level
    GROUP BY discount_bucket
    ORDER BY MIN(avg_discount)
""")

fig_sell = px.bar(
    sellthrough_data,
    x="discount_bucket",
    y="sell_through_pct",
    text="sell_through_pct",
    labels={"discount_bucket": "Discount Range", "sell_through_pct": "Sell-Through %"},
    color="discount_bucket",
    color_discrete_sequence=["#2a9d8f", "#457b9d", "#e9c46a", "#e76f51"]
)
fig_sell.update_traces(texttemplate="%{text}%", textposition="outside")
fig_sell.update_layout(showlegend=False, yaxis_range=[0, 60])
st.plotly_chart(fig_sell, use_container_width=True)
st.dataframe(sellthrough_data, use_container_width=True, hide_index=True)

st.divider()

# ── row 5: discount vs rating scatter ────────────────────────────────────────
st.subheader("Does higher discount mean lower rating?")

scatter_data = run_query("""
    SELECT
        brand,
        ROUND(AVG(discount_pct)::numeric, 1)                               AS avg_discount,
        ROUND(AVG(CASE WHEN rating > 0 THEN rating END)::numeric, 2)       AS avg_rating,
        ROUND(AVG(rating_count)::numeric, 0)                               AS avg_reviews,
        COUNT(DISTINCT product_id)                                          AS product_count
    FROM products
    GROUP BY brand
    HAVING COUNT(DISTINCT product_id) >= 5
      AND AVG(CASE WHEN rating > 0 THEN rating END) IS NOT NULL
    ORDER BY avg_discount DESC
""")

fig_scatter = px.scatter(
    scatter_data,
    x="avg_discount",
    y="avg_rating",
    size="product_count",
    hover_name="brand",
    hover_data={"avg_reviews": True, "product_count": True},
    color="avg_rating",
    color_continuous_scale="RdYlGn",
    labels={
        "avg_discount":  "Avg Discount %",
        "avg_rating":    "Avg Rating",
        "product_count": "Products",
        "avg_reviews":   "Avg Reviews"
    }
)
fig_scatter.update_layout(coloraxis_colorbar_title="Rating")
st.plotly_chart(fig_scatter, use_container_width=True)

st.divider()

# ── row 6: fake-MRP detector ─────────────────────────────────────────────────
st.subheader("Fake-MRP brand detector")
st.caption(
    "Brands with high discount + low unique_prices_seen never repriced — "
    "their 89% off is a permanent label, not a sale"
)

pricing_data = run_query("""
    SELECT
        brand,
        COUNT(DISTINCT product_id)                                          AS products,
        ROUND(AVG(discount_pct)::numeric, 1)                               AS avg_discount,
        ROUND(AVG(CASE WHEN rating > 0 THEN rating END)::numeric, 2)       AS avg_rating,
        COUNT(DISTINCT ROUND(price::numeric, 0))                           AS unique_prices_seen,
        ROUND((MAX(discount_pct) - MIN(discount_pct))::numeric, 1)        AS discount_range
    FROM products
    GROUP BY brand
    HAVING COUNT(DISTINCT product_id) >= 5
    ORDER BY avg_discount DESC
""")

# highlight rows where unique_prices_seen = 1 (never repriced)
st.dataframe(
    pricing_data,
    use_container_width=True,
    hide_index=True,
    column_config={
        "unique_prices_seen": st.column_config.NumberColumn(
            "Unique Prices Seen",
            help="1 = price never changed in 20 days = fake-MRP brand"
        ),
        "discount_range": st.column_config.NumberColumn(
            "Discount Range",
            help="Close to 0 = discount never moved = permanent label"
        )
    }
)

st.divider()

# ── row 7: brand deep-dive ────────────────────────────────────────────────────
st.subheader("Brand deep-dive")

all_brands = run_query("SELECT DISTINCT brand FROM products ORDER BY brand")
selected_brand = st.selectbox("Pick a brand", all_brands["brand"].tolist())

# parameterised query — fixes SQL injection vulnerability
brand_trend = run_query("""
    SELECT
        snapshot_date,
        source_sort,
        ROUND(AVG(discount_pct)::numeric, 1)    AS avg_discount,
        ROUND(AVG(price)::numeric, 0)           AS avg_price,
        ROUND(AVG(CASE WHEN rating > 0 THEN rating END)::numeric, 2) AS avg_rating,
        COUNT(DISTINCT product_id)              AS products_listed,
        ROUND(AVG(inventory)::numeric, 0)       AS avg_inventory
    FROM products
    WHERE brand = :brand
    GROUP BY snapshot_date, source_sort
    ORDER BY snapshot_date
""", params={"brand": selected_brand})

fig_brand = px.line(
    brand_trend,
    x="snapshot_date",
    y="avg_discount",
    color="source_sort",
    markers=True,
    labels={
        "snapshot_date": "Date",
        "avg_discount":  "Avg Discount %",
        "source_sort":   "Sort mode"
    },
    title=f"{selected_brand} — discount trend over 20 days"
)
st.plotly_chart(fig_brand, use_container_width=True)

bcol1, bcol2 = st.columns(2)

with bcol1:
    st.dataframe(
        brand_trend[["snapshot_date", "source_sort", "avg_price",
                     "avg_rating", "products_listed", "avg_inventory"]],
        use_container_width=True,
        hide_index=True
    )

with bcol2:
    brand_summary = run_query("""
        SELECT
            ROUND(AVG(discount_pct)::numeric, 1)                            AS avg_discount,
            ROUND(MIN(discount_pct)::numeric, 1)                            AS min_discount,
            ROUND(MAX(discount_pct)::numeric, 1)                            AS max_discount,
            ROUND((MAX(discount_pct) - MIN(discount_pct))::numeric, 1)     AS discount_range,
            ROUND(AVG(CASE WHEN rating > 0 THEN rating END)::numeric, 2)   AS avg_rating,
            COUNT(DISTINCT product_id)                                       AS unique_products,
            COUNT(DISTINCT ROUND(price::numeric, 0))                        AS unique_prices_seen,
            COUNT(DISTINCT snapshot_date)                                    AS days_appeared
        FROM products
        WHERE brand = :brand
    """, params={"brand": selected_brand})
    st.dataframe(brand_summary, use_container_width=True, hide_index=True)
    st.caption(
        "discount_range ≈ 0 → permanent label (fake-MRP). "
        "unique_prices_seen = 1 → never repriced in 20 days."
    )
