import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
from sqlalchemy import create_engine

# ── connection ──────────────────────────────────────────────────────────────
@st.cache_resource
# def get_connection():
#     return sqlite3.connect("myntra_analysis.db", check_same_thread=False)
def get_engine():
    url = st.secrets['SUPABASE_URL']
    return create_engine(url)

@st.cache_data
# def run_query(query):
#     conn = get_connection()
#     return pd.read_sql(query, conn)
def run_query(query):
    engine = get_engine()
    return pd.read_sql(query, engine)

# ── page config ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="Myntra Women's Dresses Analysis", layout="wide")
st.title("Myntra Women's Dresses — Pricing Analysis")
st.caption("20 days of scraped data across 3 sort modes")

# ── row 1: top metrics ───────────────────────────────────────────────────────
summary = run_query("""
    SELECT 
        COUNT(DISTINCT product_id)          as total_products,
        COUNT(DISTINCT brand)               as total_brands,
        COUNT(DISTINCT snapshot_date)       as days_tracked,
        ROUND(AVG(discount_pct)::numeric, 1)         as avg_discount,
        ROUND(AVG(rating)::numeric, 2)               as avg_rating
    FROM products
""")

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Unique Products",  f"{summary['total_products'][0]:,}")
col2.metric("Brands",           f"{summary['total_brands'][0]:,}")
col3.metric("Days Tracked",     summary['days_tracked'][0])
col4.metric("Avg Discount",     f"{summary['avg_discount'][0]}%")
col5.metric("Avg Rating",       summary['avg_rating'][0])

st.divider()

# ── row 2: discount by sort mode ─────────────────────────────────────────────
st.subheader("Discount % by sort mode")
sort_data = run_query("""
    SELECT source_sort, discount_pct
    FROM products
""")

fig1 = px.box(
    sort_data, 
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
fig1.update_layout(showlegend=False)
st.plotly_chart(fig1, use_container_width=True)

st.divider()

# ── row 3: discount vs rating ─────────────────────────────────────────────────
st.subheader("Does higher discount mean lower rating?")
scatter_data = run_query("""
    SELECT 
        brand,
        ROUND(AVG(discount_pct)::numeric, 1) as avg_discount,
        ROUND(AVG(rating)::numeric, 2)       as avg_rating,
        COUNT(DISTINCT product_id)  as product_count
    FROM products
    WHERE rating > 0
    GROUP BY brand
    HAVING COUNT(DISTINCT product_id) >= 5
""")

fig2 = px.scatter(
    scatter_data,
    x="avg_discount",
    y="avg_rating",
    size="product_count",
    hover_name="brand",
    labels={
        "avg_discount": "Avg Discount %",
        "avg_rating":   "Avg Rating",
        "product_count": "No. of products"
    },
    color="avg_rating",
    color_continuous_scale="RdYlGn"
)
st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ── row 4: brand lookup ───────────────────────────────────────────────────────
st.subheader("Brand deep-dive")
all_brands = run_query("SELECT DISTINCT brand FROM products ORDER BY brand")
selected_brand = st.selectbox("Pick a brand", all_brands["brand"].tolist())

brand_data = run_query(f"""
    SELECT 
        snapshot_date,
        source_sort,
        ROUND(AVG(discount_pct)::numeric, 1) as avg_discount,
        ROUND(AVG(price)::numeric, 0)        as avg_price,
        ROUND(AVG(rating)::numeric, 2)       as avg_rating,
        COUNT(DISTINCT product_id)  as products_listed
    FROM products
    WHERE brand = '{selected_brand}'
    GROUP BY snapshot_date, source_sort
    ORDER BY snapshot_date
""")

fig3 = px.line(
    brand_data,
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
st.plotly_chart(fig3, use_container_width=True)

bcol1, bcol2 = st.columns(2)
with bcol1:
    st.dataframe(
        brand_data[["snapshot_date", "source_sort", "avg_price", "avg_rating", "products_listed"]],
        use_container_width=True,
        hide_index=True
    )
with bcol2:
    brand_summary = run_query(f"""
        SELECT
            ROUND(AVG(discount_pct)::numeric, 1) as avg_discount,
            ROUND(MIN(discount_pct)::numeric, 1) as min_discount,
            ROUND(MAX(discount_pct)::numeric, 1) as max_discount,
            ROUND((MAX(discount_pct) - MIN(discount_pct))::numeric, 1) as discount_range,
            ROUND(AVG(rating)::numeric, 2) as avg_rating,
            COUNT(DISTINCT product_id) as unique_products
        FROM products
        WHERE brand = '{selected_brand}'
    """)
    st.dataframe(brand_summary, use_container_width=True, hide_index=True)
    st.caption("discount_range close to 0 = Fake-MRP brand (never reprices)")
    