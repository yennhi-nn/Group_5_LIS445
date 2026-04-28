"""
Module 4: AI Insight
Gửi dữ liệu tổng hợp từ Module 3 sang AI để:
  1. Nhận xét tình hình tài chính.
  2. Đưa ra lời khuyên kinh doanh ngắn gọn.
Hỗ trợ nhiều provider: openai, gemini, anthropic, deepseek, grok.
"""
import os
import json

PROVIDER = os.getenv("AI_PROVIDER", "openai").lower()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
GROK_API_KEY = os.getenv("GROK_API_KEY")


def _build_prompt(data):
    sample = data[:20] if isinstance(data, list) else data
    return (
        "Bạn là chuyên gia phân tích kinh doanh. Dưới đây là tổng hợp doanh thu "
        "thực tế đã xử lý theo từng customer_id (đã merge orders + transactions):\n"
        f"{json.dumps(sample, ensure_ascii=False, default=str)}\n\n"
        "Hãy trả lời NGẮN GỌN bằng tiếng Việt, gồm 2 phần:\n"
        "1) NHẬN XÉT về tình hình tài chính (doanh thu cao/thấp, "
        "khách hàng nào mua nhiều nhất).\n"
        "2) LỜI KHUYÊN kinh doanh để tăng trưởng."
    )


def get_ai_analysis(data, provider=None):
    if not data:
        return "Không có dữ liệu để phân tích."

    provider = (provider or PROVIDER).lower()
    prompt = _build_prompt(data)

    try:
        if provider == "openai":
            if not OPENAI_API_KEY:
                return "Thiếu OPENAI_API_KEY trong biến môi trường."
            from openai import OpenAI
            client = OpenAI(api_key=OPENAI_API_KEY)
            resp = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
            )
            return resp.choices[0].message.content

        if provider == "gemini":
            if not GEMINI_API_KEY:
                return "Thiếu GEMINI_API_KEY trong biến môi trường."
            import google.generativeai as genai
            genai.configure(api_key=GEMINI_API_KEY)
            model = genai.GenerativeModel("gemini-2.0-flash")
            return model.generate_content(prompt).text

        if provider == "anthropic":
            if not ANTHROPIC_API_KEY:
                return "Thiếu ANTHROPIC_API_KEY."
            import anthropic
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            msg = client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=512,
                messages=[{"role": "user", "content": prompt}],
            )
            return msg.content[0].text

        if provider == "deepseek":
            if not DEEPSEEK_API_KEY:
                return "Thiếu DEEPSEEK_API_KEY."
            from openai import OpenAI
            client = OpenAI(api_key=DEEPSEEK_API_KEY,
                            base_url="https://api.deepseek.com")
            resp = client.chat.completions.create(
                model="deepseek-chat",
                messages=[{"role": "user", "content": prompt}],
            )
            return resp.choices[0].message.content

        if provider == "grok":
            if not GROK_API_KEY:
                return "Thiếu GROK_API_KEY."
            from openai import OpenAI
            client = OpenAI(api_key=GROK_API_KEY,
                            base_url="https://api.x.ai/v1")
            resp = client.chat.completions.create(
                model="grok-2",
                messages=[{"role": "user", "content": prompt}],
            )
            return resp.choices[0].message.content

        return f"Provider '{provider}' không được hỗ trợ."

    except Exception as e:
        return f"AI ({provider}) gặp lỗi: {str(e)}"
