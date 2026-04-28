"""
Insight Service - điều phối Module 3 (Data Stitching) và Module 4 (AI Insight).
Expose API: GET /api/report
"""
import sys
import os
from flask import Flask, jsonify, request
from dotenv import load_dotenv

# Cho phép import từ module-3 và module-4 (tên thư mục có dấu '-')
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR, "module-3"))
sys.path.append(os.path.join(BASE_DIR, "module-4"))

load_dotenv()

from stitching import get_customer_report  # noqa: E402
from insight import get_ai_analysis  # noqa: E402

app = Flask(__name__)


@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "service": "insight_service",
        "endpoints": ["/api/report", "/health"],
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/api/report", methods=["GET"])
def api_report():
    try:
        report = get_customer_report()
    except Exception as e:
        return jsonify({"error": f"Lỗi khi nối dữ liệu: {str(e)}"}), 500

    # Support pagination via query params
    def _paginate(items, page, per_page):
        total = len(items)
        try:
            page = int(page)
        except Exception:
            page = 1
        try:
            per_page = int(per_page)
        except Exception:
            per_page = 20
        if page < 1:
            page = 1
        if per_page < 1:
            per_page = 20
        start = (page - 1) * per_page
        end = start + per_page
        total_pages = (total + per_page - 1) // per_page if per_page else 1
        return items[start:end], {"total": total, "page": page, "per_page": per_page, "total_pages": total_pages}

    # Read pagination params
    orders_page = request.args.get("orders_page", 1)
    orders_per_page = request.args.get("orders_per_page", 20)
    summary_page = request.args.get("summary_page", 1)
    summary_per_page = request.args.get("summary_per_page", 20)

    summary_full = report.get("summary", [])
    orders_full = report.get("orders", [])

    orders_page_data, orders_meta = _paginate(orders_full, orders_page, orders_per_page)
    summary_page_data, summary_meta = _paginate(summary_full, summary_page, summary_per_page)

    ai_insight = get_ai_analysis(summary_full)

    return jsonify({
        "merged_count": report.get("merged_count", 0),
        "customer_summary": summary_page_data,
        "customer_summary_meta": summary_meta,
        "orders": orders_page_data,
        "orders_meta": orders_meta,
        "transactions": report.get("transactions", []),
        "merged": report.get("merged", []),
        "ai_insight": ai_insight,
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
