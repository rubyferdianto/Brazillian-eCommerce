import streamlit as st
import pandas as pd
import pyarrow.parquet as pq
from google.cloud import storage
import io

# --- CONFIGURATION ---
BUCKET_NAME = "my_bec_bucket"
PARQUET_FILES = {
    "customers": "olist_customers_dataset.parquet",
    "sellers": "olist_sellers_dataset.parquet",
    "orders": "olist_orders_dataset.parquet",
    "order_items": "olist_order_items_dataset.parquet",
    "products": "olist_products_dataset.parquet",
    "geolocation": "olist_geolocation_dataset.parquet",
    "payments": "olist_order_payments_dataset.parquet",
    "reviews": "olist_order_reviews_dataset.parquet",
    "category_translation": "product_category_name_translation.parquet"
}

# --- GCS HELPER ---
@st.cache_data(show_spinner=False)
def load_parquet_from_gcs(bucket_name, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    buffer = io.BytesIO()
    blob.download_to_file(buffer)
    buffer.seek(0)
    return pd.read_parquet(buffer)

# --- LOAD DATA ---
st.title("Brazilian E-Commerce Parquet Data Analysis (from GCS)")

data = {}
missing = []
for key, fname in PARQUET_FILES.items():
    try:
        data[key] = load_parquet_from_gcs(BUCKET_NAME, fname)
    except Exception as e:
        missing.append(fname)
        data[key] = None

if missing:
    st.warning(f"Missing or unreadable Parquet files: {', '.join(missing)}")

# --- BASIC DATA PREVIEW ---
st.header("Data Preview")
for key, df in data.items():
    if df is not None:
        st.subheader(key.capitalize())
        st.dataframe(df.head(5))

# --- EXAMPLE ANALYSIS: Orders by State ---
if data["orders"] is not None and data["customers"] is not None:
    st.header("Order Distribution by Customer State")
    merged = pd.merge(data["orders"], data["customers"], on="customer_id", how="left")
    state_counts = merged["customer_state"].value_counts().sort_values(ascending=False)
    st.bar_chart(state_counts)

# --- EXAMPLE ANALYSIS: Top Sellers ---
if data["order_items"] is not None and data["sellers"] is not None:
    st.header("Top 10 Sellers by Number of Items Sold")
    seller_counts = data["order_items"]["seller_id"].value_counts().head(10)
    sellers = data["sellers"].set_index("seller_id").loc[seller_counts.index]
    st.bar_chart(seller_counts)
    st.write(sellers[["seller_city", "seller_state"]])

# --- EXAMPLE ANALYSIS: Payment Types ---
if data["payments"] is not None:
    st.header("Payment Type Distribution")
    payment_counts = data["payments"]["payment_type"].value_counts()
    st.bar_chart(payment_counts)

# --- EXAMPLE ANALYSIS: Review Scores ---
if data["reviews"] is not None:
    st.header("Order Review Score Distribution")
    review_counts = data["reviews"]["review_score"].value_counts().sort_index()
    st.bar_chart(review_counts)

st.info("You can expand this app with more analysis and visualizations as needed!")