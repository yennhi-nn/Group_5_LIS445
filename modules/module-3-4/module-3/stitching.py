"""
Module 3: Data Stitching
Kết nối MySQL (orders) + PostgreSQL (transactions),
merge theo order_id và tính tổng doanh thu thực tế cho từng customer_id.
"""
import pandas as pd
import mysql.connector
import psycopg2


def get_mysql_connection():
    return mysql.connector.connect(
        host="mysql_db",
        user="root",
        password="root",
        database="webstore",
    )


def get_postgres_connection():
    return psycopg2.connect(
        host="postgres_db",
        user="user",
        password="password",
        dbname="finance",
    )


def get_orders_df():
    conn = get_mysql_connection()
    try:
        df = pd.read_sql(
            "SELECT id AS order_id, customer_id, product_id, quantity, "
            "total_price, status, created_at FROM orders",
            conn,
        )
    finally:
        conn.close()
    return df


def get_transactions_df():
    conn = get_postgres_connection()
    try:
        df = pd.read_sql(
            "SELECT order_id, customer_id AS tx_customer_id, amount, "
            "status AS tx_status, created_at AS tx_created_at "
            "FROM transactions",
            conn,
        )
    finally:
        conn.close()
    return df


def get_stitched_data():
    """Merge orders + transactions theo order_id (inner join)."""
    df_orders = get_orders_df()
    df_tx = get_transactions_df()
    merged = pd.merge(df_orders, df_tx, on="order_id", how="inner")
    return merged


def get_customer_report():
    """Tính tổng doanh thu THỰC TẾ ĐÃ XỬ LÝ cho từng customer_id."""
    merged = get_stitched_data()
    if merged.empty:
        return {"merged": [], "summary": []}

    processed = merged[merged["tx_status"] == "SUCCESS"].copy()
    processed["amount"] = pd.to_numeric(processed["amount"], errors="coerce").fillna(0)

    summary = (
        processed.groupby("customer_id")
        .agg(total_orders=("order_id", "count"),
             total_revenue=("amount", "sum"))
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )

    # Convert datetime to string for JSON serialization
    merged_records = merged.copy()
    for col in merged_records.select_dtypes(include=["datetime64[ns]"]).columns:
        merged_records[col] = merged_records[col].astype(str)

    return {
        "merged_count": int(len(merged_records)),
        "summary": summary.to_dict(orient="records"),
    }
