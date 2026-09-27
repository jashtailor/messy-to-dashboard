"""
Minimal Streamlit dashboard over warehouse.db.

Run with:
    streamlit run dashboard/app.py
"""

import os
import sqlite3

import pandas as pd
import streamlit as st

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "warehouse.db")

st.set_page_config(page_title="Messy to Dashboard", layout="wide")


@st.cache_data
def load_data(_db_mtime):
    conn = sqlite3.connect(DB_PATH)
    expenses = pd.read_sql("SELECT * FROM expenses", conn, parse_dates=["date"])
    rejected = pd.read_sql("SELECT * FROM rejected_rows", conn)
    runs = pd.read_sql("SELECT * FROM pipeline_runs", conn)
    conn.close()
    return expenses, rejected, runs


if not os.path.exists(DB_PATH):
    st.error("No warehouse.db found. Run `python generate_messy_data.py && python pipeline.py` first.")
    st.stop()

expenses, rejected, runs = load_data(os.path.getmtime(DB_PATH))

st.title("Messy to Dashboard")
st.caption("Synthetic expense data, cleaned by pipeline.py, served straight from warehouse.db")

if runs.empty:
    st.error("No pipeline runs recorded yet. Run `python pipeline.py` first.")
    st.stop()

latest_run = runs.iloc[-1]
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Rows extracted", int(latest_run["rows_extracted"]))
col2.metric("Rows loaded", int(latest_run["rows_loaded"]))
col3.metric("Rows rejected", int(latest_run["rows_rejected"]))
col4.metric("Fuzzy-matched rows", int(latest_run["rows_fuzzy_matched"]))
col5.metric("Total spend", f"${expenses['amount'].sum():,.2f}" if not expenses.empty else "$0.00")

st.divider()

left, right = st.columns(2)

with left:
    st.subheader("Spend by category")
    if expenses.empty:
        st.write("No clean expenses loaded yet.")
    else:
        by_category = expenses.groupby("category")["amount"].sum().sort_values(ascending=False)
        st.bar_chart(by_category)

with right:
    st.subheader("Rejection reasons")
    if rejected.empty:
        st.write("Nothing was rejected.")
    else:
        by_reason = rejected["reason"].value_counts()
        st.bar_chart(by_reason)

st.subheader("Records over time")
if expenses.empty:
    st.write("No clean expenses loaded yet.")
else:
    by_month = (
        expenses.set_index("date")
        .resample("MS")["amount"]
        .agg(["count", "sum"])
        .rename(columns={"count": "records", "sum": "total_spend"})
    )
    st.line_chart(by_month["records"])

st.subheader("Clean rows loaded by source")
if expenses.empty:
    st.write("No clean expenses loaded yet.")
else:
    by_source = expenses["source"].value_counts()
    st.bar_chart(by_source)

with st.expander("Browse clean expenses"):
    st.dataframe(expenses, width="stretch")

with st.expander("Browse rejected rows"):
    st.dataframe(rejected, width="stretch")
