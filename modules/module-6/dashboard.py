"""
Module 6 – Dashboard UI (Streamlit)
Giao diện tương tự HTML gốc: nền trắng, chữ đen, đơn giản.
Chỉ có button được tô màu xanh cho dễ nhìn.
"""
import os
import math
import requests
import pandas as pd
import streamlit as st

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
REPORT_URL = os.getenv("REPORT_URL", "http://insight_service:5000/api/report")
KONG_URL   = os.getenv("KONG_URL",   "http://kong:8000/report/api/report")
API_KEY    = os.getenv("KONG_API_KEY", "noah-secret-key")
USE_KONG   = os.getenv("USE_KONG", "false").lower() == "true"
PER_PAGE   = 20

# ─────────────────────────────────────────────
# PAGE SETUP
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Module 3 – Góc Nhìn Toàn Cảnh",
    page_icon="📊",
    layout="wide",
)

# ─────────────────────────────────────────────
# CSS – nền trắng/xám nhạt, chữ đen, button xanh
# ─────────────────────────────────────────────
st.markdown("""
<style>
/* Nút bấm – xanh #007bff */
div.stButton > button {
    background-color: #007bff !important;
    color: #ffffff !important;
    border: none !important;
    border-radius: 4px !important;
    padding: 6px 14px !important;
    font-size: 14px !important;
    font-family: Arial, Helvetica, sans-serif !important;
    transition: background-color 0.18s;
}
div.stButton > button:hover {
    background-color: #0056b3 !important;
}
div.stButton > button:disabled {
    background-color: #adb5bd !important;
}

/* Hộp AI insight */
.ai-box {
    background: #fff;
    border: 1px solid #dee2e6;
    border-radius: 6px;
    padding: 14px 18px;
    font-size: 14px;
    font-family: Arial, Helvetica, sans-serif;
    white-space: pre-wrap;
    line-height: 1.6;
    color: #111;
}

/* Text nhỏ */
.info-text {
    font-size: 13px;
    color: #555;
    margin-bottom: 6px;
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def fetch_report(orders_page=1, summary_page=1, per_page=PER_PAGE):
    params = {
        "orders_page":      orders_page,
        "orders_per_page":  per_page,
        "summary_page":     summary_page,
        "summary_per_page": per_page,
    }
    if USE_KONG:
        url     = KONG_URL
        headers = {"apikey": API_KEY}
    else:
        url     = REPORT_URL
        headers = {}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=15)
        r.raise_for_status()
        return r.json(), None
    except requests.exceptions.ConnectionError:
        return None, "Không kết nối được tới service. Hãy chắc chắn Docker đang chạy."
    except requests.exceptions.HTTPError as e:
        return None, f"HTTP {e.response.status_code}: {e.response.text}"
    except Exception as e:
        return None, str(e)


def to_df(records):
    return pd.DataFrame(records) if records else pd.DataFrame()


def pagination_bar(key, meta):
    """Prev / Next / thông tin trang / Go to. Trả về số trang mới nếu đổi."""
    if not meta or meta.get("total_pages", 1) <= 1:
        return None

    total_pages = meta["total_pages"]
    current     = meta["page"]

    c1, c2, c3, c4, c5 = st.columns([1, 1, 3, 1, 1])
    with c1:
        prev = st.button("‹ Prev", key=f"{key}_prev", disabled=(current <= 1))
    with c2:
        nxt  = st.button("Next ›", key=f"{key}_next", disabled=(current >= total_pages))
    with c3:
        st.markdown(
            f"<span class='info-text'>Trang <b>{current}</b> / {total_pages}"
            f" &nbsp;—&nbsp; {meta.get('total', 0):,} bản ghi</span>",
            unsafe_allow_html=True,
        )
    with c4:
        goto = st.number_input(
            "Trang:", min_value=1, max_value=max(total_pages, 1),
            value=current, step=1, key=f"{key}_goto",
            label_visibility="collapsed",
        )
    with c5:
        go = st.button("Go", key=f"{key}_go")

    if prev:  return max(1, current - 1)
    if nxt:   return min(total_pages, current + 1)
    if go:    return int(max(1, min(total_pages, goto)))
    return None


# ─────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────
for k, v in [("orders_page", 1), ("summary_page", 1), ("per_page", PER_PAGE)]:
    if k not in st.session_state:
        st.session_state[k] = v

# ─────────────────────────────────────────────
# HEADER – giống <h1> trong HTML
# ─────────────────────────────────────────────
col_title, col_ctrl = st.columns([3, 1])
with col_title:
    st.markdown("# Góc Nhìn Toàn Cảnh – Module 3")
with col_ctrl:
    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)
    per_page_sel = st.selectbox(
        "Số dòng / trang", [10, 20, 50, 100],
        index=[10, 20, 50, 100].index(st.session_state.per_page)
        if st.session_state.per_page in [10, 20, 50, 100] else 1,
        key="per_page_sel",
        label_visibility="visible",
    )
    if per_page_sel != st.session_state.per_page:
        st.session_state.per_page   = per_page_sel
        st.session_state.orders_page  = 1
        st.session_state.summary_page = 1
        st.rerun()

# ─────────────────────────────────────────────
# FETCH DATA
# ─────────────────────────────────────────────
data, err = fetch_report(
    orders_page=st.session_state.orders_page,
    summary_page=st.session_state.summary_page,
    per_page=st.session_state.per_page,
)

if err:
    st.error(err)
    st.stop()

orders_meta  = data.get("orders_meta", {})
summary_meta = data.get("customer_summary_meta", {})

# ─────────────────────────────────────────────
# TỔNG QUAN (giống <div class="top"> trong HTML)
# ─────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("Số bản ghi ghép",  f"{data.get('merged_count', 0):,}")
c2.metric("Đơn hàng",         f"{orders_meta.get('total', 0):,}")
c3.metric("Thanh toán",       f"{len(data.get('transactions', [])):,}")
c4.metric("Khách hàng",       f"{summary_meta.get('total', 0):,}")

st.markdown("---")

# ─────────────────────────────────────────────
# TABS – Tổng quan | Tóm tắt KH | Đơn hàng | Thanh toán | Đã ghép | AI
# ─────────────────────────────────────────────
tab_ov, tab_cust, tab_ord, tab_tx, tab_merged, tab_ai = st.tabs([
    "Tổng quan",
    "Tóm tắt khách hàng",
    "Đơn hàng",
    "Thanh toán",
    "Đã ghép",
    "AI Insight",
])

# ══ Tab: Tổng quan ══════════════════════════════
with tab_ov:
    st.subheader("Biểu đồ doanh thu top 10 khách hàng")
    raw = data.get("customer_summary", [])
    if raw:
        df_chart = pd.DataFrame(raw).head(10)
        if "total_revenue" in df_chart.columns and "customer_id" in df_chart.columns:
            df_chart["total_revenue"] = pd.to_numeric(
                df_chart["total_revenue"].astype(str)
                    .str.replace(",", "").str.replace(" đ", ""),
                errors="coerce",
            ).fillna(0)
            st.bar_chart(
                df_chart.set_index("customer_id")[["total_revenue"]],
                use_container_width=True,
            )
    else:
        st.info("Chưa có dữ liệu để vẽ biểu đồ.")

# ══ Tab: Tóm tắt khách hàng ═════════════════════
with tab_cust:
    st.subheader("Tóm tắt khách hàng")
    df_sum = to_df(data.get("customer_summary", []))
    if df_sum.empty:
        st.info("Chưa có dữ liệu.")
    else:
        if "total_revenue" in df_sum.columns:
            df_sum = df_sum.rename(columns={
                "customer_id":   "Khách hàng",
                "total_orders":  "Số đơn",
                "total_revenue": "Tổng doanh thu",
            })
        st.dataframe(df_sum, use_container_width=True, hide_index=True)

    new = pagination_bar("summary", summary_meta)
    if new is not None:
        st.session_state.summary_page = new
        st.rerun()

# ══ Tab: Đơn hàng ═══════════════════════════════
with tab_ord:
    st.subheader("Đơn hàng")
    st.markdown(
        "<p class='info-text'>Server-side pagination – chỉ tải "
        f"{st.session_state.per_page} dòng mỗi lần, không gây treo với 20.000+ bản ghi.</p>",
        unsafe_allow_html=True,
    )
    df_ord = to_df(data.get("orders", []))
    if df_ord.empty:
        st.info("Chưa có đơn hàng.")
    else:
        # Đổi tên cột cho dễ đọc
        rename_map = {
            "order_id":    "order_id",
            "customer_id": "customer_id",
            "product_id":  "product_id",
            "quantity":    "số lượng",
            "total_price": "tổng tiền",
            "status":      "trạng thái",
            "created_at":  "thời gian",
        }
        df_ord = df_ord.rename(columns={k: v for k, v in rename_map.items() if k in df_ord.columns})
        st.dataframe(df_ord, use_container_width=True, hide_index=True)

    new = pagination_bar("orders", orders_meta)
    if new is not None:
        st.session_state.orders_page = new
        st.rerun()

# ══ Tab: Thanh toán ══════════════════════════════
with tab_tx:
    st.subheader("Thanh toán")
    df_tx = to_df(data.get("transactions", []))
    if df_tx.empty:
        st.info("Chưa có giao dịch.")
    else:
        rename_tx = {
            "order_id":      "order_id",
            "tx_customer_id":"customer_id(tx)",
            "amount":        "số tiền",
            "tx_status":     "trạng thái",
            "tx_created_at": "thời gian",
        }
        df_tx = df_tx.rename(columns={k: v for k, v in rename_tx.items() if k in df_tx.columns})
        st.dataframe(df_tx, use_container_width=True, hide_index=True)

# ══ Tab: Đã ghép ═════════════════════════════════
with tab_merged:
    st.subheader("Dữ liệu đã ghép (orders ⋈ transactions)")
    df_merged = to_df(data.get("merged", []))
    if df_merged.empty:
        st.info("Chưa có bản ghi ghép.")
    else:
        st.dataframe(df_merged, use_container_width=True, hide_index=True)

# ══ Tab: AI Insight ════════════════════════════════
with tab_ai:
    st.subheader("Nhận xét AI")
    ai = data.get("ai_insight", "")
    if not ai or ai == "Không có dữ liệu để phân tích.":
        st.info("Chưa có insight từ AI. Hãy cấu hình API key trong .env.")
    else:
        st.markdown(f'<div class="ai-box">{ai}</div>', unsafe_allow_html=True)
        st.download_button(
            label="Tải insight (TXT)",
            data=ai,
            file_name="ai_insight.txt",
            mime="text/plain",
        )
