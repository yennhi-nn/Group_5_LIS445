"""
Insight Service - điều phối Module 3 (Data Stitching) và Module 4 (AI Insight).
Expose API: GET /api/report
"""
import sys
import os
from flask import Flask, jsonify, render_template
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

    summary = report.get("summary", [])
    ai_insight = get_ai_analysis(summary)

    return jsonify({
        "merged_count": report.get("merged_count", 0),
        "customer_summary": summary,
        "orders": report.get("orders", []),
        "transactions": report.get("transactions", []),
        "merged": report.get("merged", []),
        "ai_insight": ai_insight,
    })


@app.route("/dashboard", methods=["GET"])
def dashboard():
    return render_template("dashboard.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
