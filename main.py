#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IRAF Monitoring - News Automation Pipeline
============================================
Fetches news from Inoreader -> filters/translates/analyzes with DeepSeek
-> posts curated results + full report to a Telegram channel.

Run schedule: hourly via GitHub Actions cron.
"""

import os
import re
import sys
import json
import hashlib
import logging
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import requests

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("iraf")

INOREADER_APP_ID = os.environ.get("INOREADER_APP_ID", "")
INOREADER_APP_KEY = os.environ.get("INOREADER_APP_KEY", "")
INOREADER_EMAIL = os.environ.get("INOREADER_EMAIL", "")
INOREADER_PASSWORD = os.environ.get("INOREADER_PASSWORD", "")
INOREADER_TAG = os.environ.get("INOREADER_TAG", "")  # e.g. user/-/label/Afghanistan

DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_API_URL = "https://api.deepseek.com/chat/completions"
DEEPSEEK_MODEL = os.environ.get("DEEPSEEK_MODEL", "deepseek-chat")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID", "")  # e.g. @IrafMonitoring

SEEN_IDS_FILE = Path("seen_ids.txt")
REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)

MAX_ARTICLES_PER_RUN = int(os.environ.get("MAX_ARTICLES_PER_RUN", "0"))  # 0 = no limit
KEYWORD_OVERLAP_THRESHOLD = 0.7

CATEGORY_MAP = {
    "سیاسی": "#سیاسی 🏛️",
    "اقتصادی": "#اقتصادی 💰",
    "فرهنگی": "#فرهنگی 🎭",
    "مهاجرین": "#مهاجرین 🧳",
    "ورزشی": "#ورزشی ⚽",
}

# Deterministic geographic priority tiers (do NOT let the model decide this
# freely -- it was mixing priorities inconsistently). Tier 1 = highest.
GEO_PRIORITY_TIERS = {
    1: {"افغانستان"},
    2: {"ایران"},
    3: {"پاکستان", "هند", "چین"},
    4: {
        "تاجیکستان", "ازبکستان", "ترکمنستان", "قزاقستان", "قرقیزستان",
        "آسیای مرکزی",
    },
}
SPORTS_PRIORITY = 5  # ورزشی همیشه اولویت ۵، مستقل از کشور
DEFAULT_PRIORITY = 3  # fallback when no recognized country is present

FLAG_MAP = {
    "افغانستان": "🇦🇫",
    "ایران": "🇮🇷",
    "پاکستان": "🇵🇰",
    "هند": "🇮🇳",
    "چین": "🇨🇳",
    "تاجیکستان": "🇹🇯",
    "ازبکستان": "🇺🇿",
    "ترکمنستان": "🇹🇲",
    "قزاقستان": "🇰🇿",
    "قرقیزستان": "🇰🇬",
}


def flags_for_countries(countries: list) -> list:
    """Derives flag emoji from country names in code, so a mismatched flag
    from the model can never slip through."""
    return [FLAG_MAP[c] for c in (countries or []) if c in FLAG_MAP]


def compute_priority(category: str, countries: list) -> int:
    """Determines priority deterministically from the geographic rule,
    instead of trusting the model's own free-form priority number."""
    if category == "ورزشی":
        return SPORTS_PRIORITY

    best = None
    for country in (countries or []):
        for tier, members in GEO_PRIORITY_TIERS.items():
            if country in members:
                if best is None or tier < best:
                    best = tier
    return best if best is not None else DEFAULT_PRIORITY

EDITORIAL_RULES = """
قوانین ویرایشی (باید دقیقاً رعایت شوند):
1. اولویت جغرافیایی: افغانستان (اول) > ایران (دوم) > پاکستان/هند/چین (در ارتباط با افغانستان) (سوم) > کشورهای آسیای مرکزی (چهارم) > ورزش بین‌المللی/افغانستان (پنجم)
2. هیچ محتوای ضد ایران منتشر نشود.
3. طالبان باید همیشه با عنوان «حکومت سرپرست افغانستان» (که از سوی ایران به رسمیت شناخته نشده) خطاب شود، نه «طالبان».
4. هر خبر باید در یکی از دسته‌های زیر قرار گیرد: سیاسی، اقتصادی، فرهنگی، مهاجرین، ورزشی.
5. خروجی باید ترجمه شده به فارسی روان، خلاصه و بی‌طرف باشد.
"""

# --------------------------------------------------------------------------
# Utilities
# --------------------------------------------------------------------------

def sanitize_text(text: str) -> str:
    """Remove control characters and normalize unicode so text is safe to
    send to the DeepSeek API as JSON. This is the core fix for the 400
    Bad Request errors that occurred with raw Persian article content."""
    if not text:
        return ""
    # Normalize unicode (fixes weird combining characters from RSS feeds)
    text = unicodedata.normalize("NFC", text)
    # Strip control characters except newline/tab, which break JSON encoding
    text = "".join(
        ch for ch in text
        if ch in ("\n", "\t") or unicodedata.category(ch)[0] != "C"
    )
    # Collapse excessive whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalize_title(title: str) -> str:
    title = sanitize_text(title).lower()
    title = re.sub(r"[^\w\s]", "", title, flags=re.UNICODE)
    return title.strip()


def title_hash(title: str) -> str:
    return hashlib.md5(normalize_title(title).encode("utf-8")).hexdigest()


def keyword_overlap(a: str, b: str) -> float:
    words_a = set(normalize_title(a).split())
    words_b = set(normalize_title(b).split())
    if not words_a or not words_b:
        return 0.0
    intersection = words_a & words_b
    smaller = min(len(words_a), len(words_b))
    return len(intersection) / smaller if smaller else 0.0


def load_seen_ids() -> set:
    if SEEN_IDS_FILE.exists():
        return set(SEEN_IDS_FILE.read_text(encoding="utf-8").splitlines())
    return set()


def save_seen_ids(seen: set):
    SEEN_IDS_FILE.write_text("\n".join(sorted(seen)), encoding="utf-8")


# --------------------------------------------------------------------------
# Inoreader
# --------------------------------------------------------------------------

def inoreader_login() -> str:
    """Direct credential auth (OAuth was tried previously and abandoned)."""
    url = "https://www.inoreader.com/accounts/ClientLogin"
    resp = requests.post(
        url,
        data={"Email": INOREADER_EMAIL, "Passwd": INOREADER_PASSWORD},
        timeout=30,
    )
    resp.raise_for_status()
    for line in resp.text.splitlines():
        if line.startswith("Auth="):
            return line[len("Auth="):]
    raise RuntimeError("Inoreader login failed: no Auth token in response")


def fetch_articles(auth_token: str, limit: int = 50) -> list:
    url = f"https://www.inoreader.com/reader/api/0/stream/contents/{INOREADER_TAG}"
    headers = {
        "Authorization": f"GoogleLogin auth={auth_token}",
        "AppId": INOREADER_APP_ID,
        "AppKey": INOREADER_APP_KEY,
    }
    params = {"n": limit}
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    articles = []
    for item in data.get("items", []):
        title = sanitize_text(item.get("title", ""))
        summary = sanitize_text(
            item.get("summary", {}).get("content", "")
        )
        # Strip HTML tags from summary
        summary = re.sub(r"<[^>]+>", " ", summary)
        summary = sanitize_text(summary)

        link = ""
        canonical = item.get("canonical") or item.get("alternate") or []
        if canonical:
            link = canonical[0].get("href", "")

        source = sanitize_text(item.get("origin", {}).get("title", ""))

        if not title:
            continue

        articles.append({
            "id": item.get("id", title_hash(title)),
            "title": title,
            "summary": summary[:2000],  # cap length defensively
            "link": link,
            "source": source,
        })
    return articles


# --------------------------------------------------------------------------
# Duplicate detection
# --------------------------------------------------------------------------

def deduplicate(articles: list, seen: set) -> list:
    fresh = []
    seen_titles_this_run = []

    for art in articles:
        h = title_hash(art["title"])
        if h in seen:
            continue

        is_dup = False
        for prev_title in seen_titles_this_run:
            if keyword_overlap(art["title"], prev_title) >= KEYWORD_OVERLAP_THRESHOLD:
                is_dup = True
                break
        if is_dup:
            continue

        art["_hash"] = h
        fresh.append(art)
        seen_titles_this_run.append(art["title"])

    return fresh


# --------------------------------------------------------------------------
# DeepSeek
# --------------------------------------------------------------------------

def call_deepseek(system_prompt: str, user_prompt: str, max_tokens: int = 2000) -> str:
    """Central place for all DeepSeek calls. Always uses requests' `json=`
    parameter so the library handles UTF-8 encoding and escaping correctly
    -- this was the root cause of the previous 400 Bad Request errors
    (manually built JSON strings mangled Persian special characters)."""
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json; charset=utf-8",
    }
    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": sanitize_text(system_prompt)},
            {"role": "user", "content": sanitize_text(user_prompt)},
        ],
        "max_tokens": max_tokens,
        "temperature": 0.3,
    }

    try:
        resp = requests.post(
            DEEPSEEK_API_URL,
            headers=headers,
            json=payload,  # <-- key fix: let requests serialize + set charset
            timeout=90,
        )
        if resp.status_code != 200:
            log.error("DeepSeek %s error: %s", resp.status_code, resp.text[:1000])
            resp.raise_for_status()
        data = resp.json()
        choice = data["choices"][0]
        finish_reason = choice.get("finish_reason", "")
        if finish_reason == "length":
            log.warning("DeepSeek response was CUT OFF (finish_reason=length, max_tokens too low)")
        return choice["message"]["content"].strip()
    except requests.exceptions.RequestException as e:
        log.error("DeepSeek request failed: %s", e)
        raise


def analyze_article(article: dict) -> dict:
    """Filter relevance, translate, categorize a single article."""
    system_prompt = f"""
شما دستیار ویرایشی کانال خبری «IRAF Monitoring» هستید که اخبار مرتبط با
افغانستان و منطقه را رصد می‌کند.

{EDITORIAL_RULES}

نکته مهم درباره فیلتر ارتباط (relevant):
این خبر از فیدی می‌آید که از قبل روی موضوع افغانستان/منطقه تنظیم شده است،
پس پیش‌فرض این است که خبر مرتبط است ("relevant": true) مگر اینکه یکی از
موارد زیر باشد:
- تبلیغات، اسپم، یا کاملاً بی‌ربط به افغانستان/ایران/منطقه (مثلاً ورزش
  اروپایی بدون ارتباط با افغان‌ها، یا خبر محلی کاملاً بی‌ربط کشور ثالث)
- محتوای صریحاً ضد ایران
سخت‌گیری نکنید؛ در موارد مبهم یا خاکستری، "relevant" را true بگذارید و
اجازه دهید ویرایشگر انسانی بعداً تصمیم نهایی را بگیرد.

نکته مهم درباره کشورها:
"countries" و "country_flags" باید فقط از این نام‌های استاندارد استفاده کنند
(دقیقاً همین املا، حداکثر ۳ کشور، به ترتیب ارتباط):
افغانستان 🇦🇫 | ایران 🇮🇷 | پاکستان 🇵🇰 | هند 🇮🇳 | چین 🇨🇳 | تاجیکستان 🇹🇯 |
ازبکستان 🇺🇿 | ترکمنستان 🇹🇲 | قزاقستان 🇰🇿 | قرقیزستان 🇰🇬
اولویت را شما تعیین نکنید؛ آن در کد به‌صورت خودکار محاسبه می‌شود.

فقط و فقط یک شیء JSON معتبر برگردانید (بدون هیچ متن اضافه، بدون Markdown)
با این ساختار دقیق:
{{
  "relevant": true/false,
  "category": "سیاسی|اقتصادی|فرهنگی|مهاجرین|ورزشی",
  "title_fa": "عنوان ترجمه‌شده و خبری به فارسی (بدون هشتگ یا ایموجی)",
  "summary_fa": "خلاصه ۳ تا ۵ جمله‌ای روان و بی‌طرف به فارسی، در حد یک پاراگراف کامل خبری",
  "key_points": ["نکته کلیدی اول", "نکته کلیدی دوم", "نکته کلیدی سوم (حداکثر ۴ نکته)"],
  "countries": ["افغانستان", "پاکستان"],
  "country_flags": ["🇦🇫", "🇵🇰"],
  "reason": "دلیل کوتاه ارتباط یا عدم ارتباط"
}}

نکات مهم:
- "key_points" باید نکات مشخص و خبری باشند، نه تکرار خلاصه.
- اگر خبر مرتبط نیست، "relevant" را false بگذارید و بقیه فیلدها را خالی/آرایه خالی رها کنید.
"""
    user_prompt = f"""
عنوان: {article['title']}
منبع: {article['source']}
متن: {article['summary']}
"""
    raw = call_deepseek(system_prompt, user_prompt, max_tokens=1500)

    # DeepSeek sometimes wraps JSON in ```json fences, or adds stray text
    # despite instructions -- extract the outermost {...} block defensively.
    cleaned = re.sub(r"^```(json)?|```$", "", raw.strip(), flags=re.MULTILINE).strip()
    match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
    if match:
        cleaned = match.group(0)

    try:
        result = json.loads(cleaned)
    except json.JSONDecodeError:
        log.warning("Could not parse DeepSeek JSON for '%s': %s", article["title"], raw[:1500])
        return {"relevant": False}

    if result.get("relevant"):
        result["priority"] = compute_priority(result.get("category", ""), result.get("countries", []))
        result["country_flags"] = flags_for_countries(result.get("countries", []))

    return result


def generate_article_report(article_result: dict) -> dict:
    """Ask DeepSeek to write a dedicated analytical report for a single
    article that was just posted to the channel. Returns a dict with
    تیتر/لید/بدنه/جمع‌بندی fields."""
    past_reports = sorted(REPORTS_DIR.glob("*.md"))[-3:]
    style_context = ""
    for p in past_reports:
        style_context += p.read_text(encoding="utf-8")[:1500] + "\n---\n"

    key_points_text = "\n".join(f"- {kp}" for kp in article_result.get("key_points", []) or [])
    user_prompt = f"""
عنوان خبر: {article_result.get('title_fa', '')}
دسته: {article_result.get('category', '')}
خلاصه: {article_result.get('summary_fa', '')}
نکات کلیدی:
{key_points_text}
"""

    system_prompt = f"""
شما تحلیلگر ارشد کانال «IRAF Monitoring» هستید.
{EDITORIAL_RULES}

بر اساس خبری که در ادامه آمده، یک گزارش تحلیلی عمیق و مستقل به فارسی
بنویسید که زمینه، اهمیت و پیامدهای این خبر را برای مخاطب افغان/ایرانی
باز کند.
سبک نوشتاری گزارش‌های قبلی را در نظر بگیرید (در ادامه آمده):
{style_context}

فقط و فقط یک شیء JSON معتبر برگردانید (بدون متن اضافه، بدون Markdown fence)
با این ساختار دقیق:
{{
  "تیتر": "عنوان کوتاه و خبری گزارش تحلیلی",
  "لید": "یک پاراگراف لید که موضوع، اهمیت و زاویه اصلی را معرفی می‌کند",
  "بدنه": "دو تا چهار پاراگراف بدنه که با \\n\\n از هم جدا شده باشند، شامل زمینه، جزئیات و تحلیل",
  "جمع‌بندی": "یک پاراگراف جمع‌بندی که پیامد یا چشم‌انداز موضوع را جمع می‌کند"
}}
"""
    raw = call_deepseek(system_prompt, user_prompt, max_tokens=1500)
    cleaned = re.sub(r"^```(json)?|```$", "", raw.strip(), flags=re.MULTILINE).strip()
    match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
    if match:
        cleaned = match.group(0)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        log.warning("Could not parse per-article report JSON: %s", raw[:1500])
        return {}


def format_article_report_message(report: dict, category: str) -> str:
    """Builds the analytical report message that follows each article post:

    #دسته

    📝 گزارش تحلیلی

    تیتر: ...

    لید:
    ...

    بدنه:
    ...

    جمع‌بندی:
    ...
    """
    lines = [f"#{category}", "", "📝 گزارش تحلیلی", ""]
    if report.get("تیتر"):
        lines.append(f"تیتر: {report['تیتر']}")
        lines.append("")
    if report.get("لید"):
        lines.append("لید:")
        lines.append(report["لید"])
        lines.append("")
    if report.get("بدنه"):
        lines.append("بدنه:")
        lines.append(report["بدنه"])
        lines.append("")
    if report.get("جمع‌بندی"):
        lines.append("جمع‌بندی:")
        lines.append(report["جمع‌بندی"])
    return "\n".join(lines)


# --------------------------------------------------------------------------
# Telegram
# --------------------------------------------------------------------------

def send_telegram_message(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    # Telegram hard limit is 4096 chars; trim defensively
    text = text[:4090]
    payload = {
        "chat_id": TELEGRAM_CHANNEL_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    resp = requests.post(url, json=payload, timeout=30)
    if resp.status_code != 200:
        log.error("Telegram error: %s", resp.text[:500])
        resp.raise_for_status()


def format_article_message(art: dict) -> str:
    """Builds a single rich article post matching the requested channel template:

    #دسته

    🇦🇫🏛️ عنوان خبر

    📋 خلاصه:
    ...

    🔑 نکات کلیدی:
    • ...

    🌍 موضوع: کشورها | اولویت N

    🔗 منبع خبر
    """
    category = art.get("category", "")
    tag = CATEGORY_MAP.get(category, f"#{category}")
    category_emoji = tag.split()[-1] if " " in tag else ""
    flags = "".join(art.get("country_flags", []) or [])
    title = art.get("title_fa", "")
    summary = art.get("summary_fa", "")
    key_points = art.get("key_points", []) or []
    countries = art.get("countries", []) or []
    priority = art.get("priority", "")
    link = art.get("link", "")

    lines = [f"#{category}", ""]
    lines.append(f"{flags} {category_emoji} {title}".strip())
    lines.append("")
    if summary:
        lines.append("📋 خلاصه:")
        lines.append(summary)
        lines.append("")
    if key_points:
        lines.append("🔑 نکات کلیدی:")
        for point in key_points:
            lines.append(f"• {point}")
        lines.append("")
    if countries:
        lines.append(f"🌍 موضوع: {' و '.join(countries)} | اولویت {priority}")
    if link:
        lines.append("")
        lines.append(f'🔗 <a href="{link}">منبع خبر</a>')

    return "\n".join(lines)


def build_summary_message(analyzed_articles: list) -> list:
    """Returns a list of message strings, one per article (Telegram posts
    are more readable as separate messages with this richer template than
    crammed into a single digest)."""
    sorted_articles = sorted(analyzed_articles, key=lambda a: a.get("priority", 5))
    return [format_article_message(art) for art in sorted_articles]


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main():
    required_env = [
        "INOREADER_APP_ID", "INOREADER_APP_KEY", "INOREADER_EMAIL",
        "INOREADER_PASSWORD", "INOREADER_TAG", "DEEPSEEK_API_KEY",
        "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHANNEL_ID",
    ]
    missing = [v for v in required_env if not os.environ.get(v)]
    if missing:
        log.error("Missing required environment variables/secrets: %s", ", ".join(missing))
        sys.exit(1)

    log.info("Logging into Inoreader...")
    auth_token = inoreader_login()

    log.info("Fetching articles...")
    articles = fetch_articles(auth_token, limit=200)
    log.info("Fetched %d articles", len(articles))

    seen = load_seen_ids()
    fresh_articles = deduplicate(articles, seen)
    log.info("%d articles remain after de-duplication", len(fresh_articles))

    analyzed = []
    for art in fresh_articles:  # no cap: analyze every fresh article this run
        try:
            result = analyze_article(art)
        except Exception as e:
            log.error("Failed to analyze article '%s': %s", art["title"], e)
            continue

        seen.add(art["_hash"])  # mark as seen regardless of relevance, to avoid re-processing

        if result.get("relevant"):
            result["link"] = art["link"]
            analyzed.append(result)

    save_seen_ids(seen)

    if not analyzed:
        log.info("No relevant articles this run. Exiting.")
        return

    log.info("%d relevant articles analyzed. Sending to Telegram...", len(analyzed))

    sorted_articles = sorted(analyzed, key=lambda a: a.get("priority", 5))

    for art in sorted_articles:
        # 1) Post the article itself
        try:
            send_telegram_message(format_article_message(art))
            time.sleep(1.2)  # stay under Telegram's rate limit
        except Exception as e:
            log.error("Failed to send article message: %s", e)
            continue  # skip the report if the article post itself failed

        # 2) Immediately generate + post the analytical report for this article
        try:
            report = generate_article_report(art)
            if report:
                report_msg = format_article_report_message(report, art.get("category", ""))
                send_telegram_message(report_msg)
                time.sleep(1.2)

                report_path = REPORTS_DIR / f"report_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.md"
                report_path.write_text(report_msg, encoding="utf-8")
        except Exception as e:
            log.error("Report generation/sending failed for '%s': %s", art.get("title_fa", ""), e)

    log.info("Run complete.")


if __name__ == "__main__":
    main()
