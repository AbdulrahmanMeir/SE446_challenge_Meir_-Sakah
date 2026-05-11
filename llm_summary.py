"""LLM summary wrapper for News Pulse."""

import os

try:
    import openai
except ImportError:
    openai = None

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


def generate_summary(keywords):
    """Generate a one-paragraph summary from keywords with a safe fallback."""
    if not keywords:
        return "No keywords are available yet. Waiting for fresh headlines to arrive."

    safe_keywords = ", ".join(keywords[:6])
    fallback_storylines = ", ".join(keywords[:3]) if len(keywords) >= 3 else "politics, economy, technology"
    fallback = (
        f"News Pulse sees key themes like {safe_keywords}. "
        f"Storylines include {fallback_storylines}. "
        "This summary is based on headline keywords and keeps the dashboard stable."
    )

    if not openai or not OPENAI_API_KEY:
        return fallback

    prompt = (
        "Write a single short paragraph (max 80 words) summarizing these news keywords: "
        f"{safe_keywords}. "
        "If possible, mention at least three storylines such as politics, business, or tech."
    )

    try:
        openai.api_key = OPENAI_API_KEY
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=120,
            temperature=0.6,
        )
        text = response.choices[0].message.content.strip()
        return text
    except Exception:
        return fallback
