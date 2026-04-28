"""
Module 3: Data Stitching
Kết nối MySQL (orders) + PostgreSQL (transactions),
merge theo order_id và tính tổng doanh thu thực tế cho từng customer_id.
"""
import os
import pandas as pd
import mysql.connector
import psycopg2


def get_mysql_connection():
    host = os.getenv("MYSQL_HOST", "127.0.0.1")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "root")
    database = os.getenv("MYSQL_DB", "webstore")
    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )


def get_postgres_connection():
    host = os.getenv("POSTGRES_HOST", "127.0.0.1")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    dbname = os.getenv("POSTGRES_DB", "finance")
    return psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
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
    # Load source tables
    df_orders = get_orders_df()
    df_tx = get_transactions_df()

    # Merge
    merged = pd.merge(df_orders, df_tx, on="order_id", how="inner")

    # Helper to convert dataframes to JSON-serializable records
    def _convert_dt(df):
        df2 = df.copy()
        for col in df2.select_dtypes(include=["datetime64[ns]"]).columns:
            df2[col] = df2[col].astype(str)
        return df2

    def _df_to_records(df):
        if df is None or df.empty:
            return []
        return _convert_dt(df).fillna("").to_dict(orient="records")

    # Prepare processed subset (only successful transactions)
    merged_records = _df_to_records(merged)
    orders_records = _df_to_records(df_orders)
    tx_records = _df_to_records(df_tx)

    if merged.empty:
        # No joined records — fallback: compute customer totals from orders
        summary_orders = (
            df_orders.groupby("customer_id")
            .agg(total_orders=("order_id", "count"),
                 total_revenue=("total_price", "sum"))
            .reset_index()
            .sort_values("total_revenue", ascending=False)
        )

        return {
            "merged_count": 0,
            "orders": orders_records,
            "transactions": tx_records,
            "merged": [],
            "summary": summary_orders.to_dict(orient="records"),
        }

    processed = merged[merged["tx_status"] == "SUCCESS"].copy()
    if processed.empty:
        # No successful transactions — still compute totals from orders
        summary_orders = (
            df_orders.groupby("customer_id")
            .agg(total_orders=("order_id", "count"),
                 total_revenue=("total_price", "sum"))
            .reset_index()
            .sort_values("total_revenue", ascending=False)
        )
        return {
            "merged_count": int(len(merged_records)),
            "orders": orders_records,
            "transactions": tx_records,
            "merged": merged_records,
            "summary": summary_orders.to_dict(orient="records"),
        }

    processed["amount"] = pd.to_numeric(processed["amount"], errors="coerce").fillna(0)

    summary = (
        processed.groupby("customer_id")
        .agg(total_orders=("order_id", "count"),
             total_revenue=("amount", "sum"))
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )

    return {
        "merged_count": int(len(merged_records)),
        "orders": orders_records,
        "transactions": tx_records,
        "merged": merged_records,
        "summary": summary.to_dict(orient="records"),
    }
