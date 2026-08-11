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
SEEN_HISTORY_FILE = Path("seen_history.jsonl")
CROSS_SOURCE_OVERLAP_THRESHOLD = 0.5  # title+summary overlap vs recent history
REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)

MAX_ARTICLES_PER_RUN = int(os.environ.get("MAX_ARTICLES_PER_RUN", "0"))  # 0 = no limit
KEYWORD_OVERLAP_THRESHOLD = 0.5

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

# Full editorial/media policy of خبرگزاری ایراف, organized by domain, used
# specifically when writing the deeper analytical report for each article.
IRAF_MEDIA_POLICY = """
=== سیاسی ===
- حمایت از مردم افغانستان
- پذیرش طالبان به عنوان بخشی از جامعه افغانستان و یک واقعیت عینی
- تاکید بر ضرورت مشارکت همه اقوام افغانستان در قدرت سیاسی
- عدم ورود به فاز شناسایی حقوقی از طالبان تا زمان تحقق منافع جمهوری اسلامی ایران

=== امنیتی ===
- ضرورت کاهش مناقشات امنیتی میان افغانستان و همسایگانش
- ضرورت تامین امنیت و ممانعت از ناامنی درون افغانستان به منظور ممانعت از بی‌جاشدگی مردم در داخل یا مهاجرت آن‌ها به خارج از افغانستان
- توجه به تعهد حکومت افغانستان به این‌که اجازه استفاده گروه‌های غیردولتی و تروریستی از خاک خود را برای ناامن کردن مرزهای مشترک یا کشورهای همسایه ندهد
- تلقی امنیتی یکپارچه میان ایران و افغانستان؛ ناامنی در کابل به معنی ناامنی تهران در نظر گرفته می‌شود
- مخالفت با فعالیت داعش و سایر گروه‌های تروریستی در افغانستان و همکاری برای مقابله با تروریسم
- برجسته‌سازی داعش به عنوان تهدید امنیت منطقه‌ای
- برجسته‌سازی مبارزه طالبان با داعش
- برجسته‌سازی رویکرد دوگانه طالبان با سایر گروه‌های تروریستی
- مخالفت با رویکردهای ضدقومی در افغانستان تحت عناوین امنیتی

=== بین‌المللی ===
- مخالفت با مداخله امنیتی و سیاسی کشورهای غربی (ناتو و آمریکا) در امور منطقه و افغانستان
- بازنمایی مثبت از تلاش‌های منطقه‌ای برای تعدیل سیاست‌های طالبان
- نقد سیاست‌های آمریکا و ناتو در افغانستان طی سال‌های ۲۰۰۱ تا ۲۰۲۱
- مخالفت با مداخله کشورهای خارج از منطقه در مسائل افغانستان
- برجسته‌سازی خسارات حضور آمریکا و ناتو در افغانستان
- برجسته‌سازی اقدامات نظامیان آمریکا و ناتو در افغانستان

=== اقتصادی ===
- تاکید بر موفقیت‌های اقتصادی ایران و امکان استفاده از امکانات ایران برای توسعه اقتصادی و رفاه مردم افغانستان
- برجسته‌سازی ظرفیت‌های اتصالی افغانستان
- تلاش برای توسعه اقتصادی افغانستان
- علاقه‌مندی به ایجاد و تقویت پیوندهای اقتصادی میان بخش خصوصی و دولتی ایران و افغانستان
- احتیاط در توسعه مناسبات اقتصادی افغانستان با سایر بازیگران و کشورهای عربی و فرامنطقه‌ای

=== مهاجرین ===
- جداکردن حساب مهاجرین قانونی افغانستانی از اتباع افغان که ورود و حضور غیرقانونی در ایران دارند
- تاکید بر ضرورت نظام‌مند شدن وضعیت مهاجرین قانونی
- برساختن جمهوری اسلامی به عنوان میزبان اتباع افغانستان
- برجسته کردن اقدامات جمهوری اسلامی ایران برای رشد استعدادهای اتباع افغانستان مقیم ایران
- تاکید بر حفظ کرامت انسانی اتباع افغانستان مقیم ایران
- تاکید بر ضرورت مسئولیت‌پذیری حکومت افغانستان در قبال تردد اتباع افغانستان به خاک ایران
- مخالفت با رویکردهای مهاجر/افغان‌هراسانه

=== فرهنگی ===
- نقد ارزش‌های فرهنگی غربی و مخالفت با توسعه آن در افغانستان
- ترویج فرهنگ مقاومت در افغانستان
- حمایت از فرهنگ شیعه در افغانستان
- مخالفت با رویکردهای ضد زبان فارسی
- مخالفت با ایجاد محدودیت فرهنگی و زبانی برای اقوام مختلف افغانستان
- تاکید بر ضرورت بهره‌برداری و هم‌افزایی از ظرفیت‌های فرهنگی در میان مهاجرین
- برجسته‌سازی رویکردهای رسانه‌ای مخرب شبکه‌های ماهواره‌ای از جمله ایران اینترنشنال و افغانستان اینترنشنال

=== فلسطین و مقاومت اسلامی ===
- حمایت از مقاومت فلسطین
- محکومیت اقدامات رژیم صهیونیستی در فلسطین و کشورهای اسلامی
- برجسته‌سازی رویکردهای حمایتی طالبان از مفهوم یا جبهه مقاومت

=== ایران ===
- پایبندی به منافع ملی، امنیت و تمامیت ارضی جمهوری اسلامی ایران
- ترویج گفتمان انقلاب اسلامی، استقلال، عدالت و پیشرفت ملی
- حمایت از جبهه مقاومت و پوشش تحولات منطقه بر پایه این موضوع
- مقابله با جنگ شناختی، عملیات روانی و انتشار اخبار جعلی از طریق تحلیل و راستی‌آزمایی
- تقویت هویت اسلامی-ایرانی، زبان فارسی و ارزش‌های فرهنگی جامعه
- انعکاس دستاوردهای علمی، فناورانه، اقتصادی و فرهنگی کشور
"""

# Mandatory terminology substitutions -- always enforced regardless of domain.
IRAF_TERMINOLOGY_RULES = """
اصطلاحات الزامی (باید همیشه دقیقاً همین‌طور نوشته شوند):
- «طالبان» → «حکومت سرپرست افغانستان»
- «مجتبی خامنه‌ای» یا هر اشاره به رهبر جمهوری اسلامی ایران → «مقام معظم رهبری» یا «رهبر عالی‌قدر جمهوری اسلامی ایران»
- «رژیم جمهوری اسلامی» → «نظام جمهوری اسلامی ایران»
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


def normalize_words(text: str) -> set:
    text = sanitize_text(text).lower()
    text = re.sub(r"[^\w\s]", "", text, flags=re.UNICODE)
    return set(text.split())


def keyword_overlap(a: str, b: str) -> float:
    words_a = set(normalize_title(a).split())
    words_b = set(normalize_title(b).split())
    if not words_a or not words_b:
        return 0.0
    intersection = words_a & words_b
    smaller = min(len(words_a), len(words_b))
    return len(intersection) / smaller if smaller else 0.0


def combined_overlap(article_a: dict, article_b_words: set) -> float:
    """Compares title+summary keywords, not just the title -- this catches
    the same story rewritten with a different headline by another source."""
    text_a = article_a["title"] + " " + article_a.get("summary", "")[:400]
    words_a = normalize_words(text_a)
    if not words_a or not article_b_words:
        return 0.0
    intersection = words_a & article_b_words
    smaller = min(len(words_a), len(article_b_words))
    return len(intersection) / smaller if smaller else 0.0


def load_seen_ids() -> set:
    if SEEN_IDS_FILE.exists():
        return set(SEEN_IDS_FILE.read_text(encoding="utf-8").splitlines())
    return set()


def save_seen_ids(seen: set):
    SEEN_IDS_FILE.write_text("\n".join(sorted(seen)), encoding="utf-8")


def load_seen_history(max_age_hours: int = 72) -> list:
    """Loads recent (title+summary keyword-set) fingerprints used to catch
    the same story republished by a different source with a different
    headline. Pruned to a rolling time window so the file doesn't grow
    forever."""
    if not SEEN_HISTORY_FILE.exists():
        return []
    cutoff = datetime.now(timezone.utc).timestamp() - (max_age_hours * 3600)
    history = []
    for line in SEEN_HISTORY_FILE.read_text(encoding="utf-8").splitlines():
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if entry.get("ts", 0) >= cutoff:
            entry["words"] = set(entry.get("words", []))
            history.append(entry)
    return history


def save_seen_history(history: list, max_entries: int = 1500):
    trimmed = history[-max_entries:]
    lines = []
    for entry in trimmed:
        lines.append(json.dumps({
            "ts": entry["ts"],
            "words": sorted(entry["words"]),
        }, ensure_ascii=False))
    SEEN_HISTORY_FILE.write_text("\n".join(lines), encoding="utf-8")


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


def fetch_articles(auth_token: str, limit: int = 50, max_age_hours: int = 48) -> list:
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

    cutoff_ts = datetime.now(timezone.utc).timestamp() - (max_age_hours * 3600)

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
        published = item.get("published")  # unix timestamp from Inoreader

        if not title:
            continue

        # Safety net: skip stale articles even if they somehow weren't
        # caught by seen_ids (e.g. a bad backlog after downtime).
        if published and published < cutoff_ts:
            continue

        articles.append({
            "id": item.get("id", title_hash(title)),
            "title": title,
            "summary": summary[:2000],  # cap length defensively
            "link": link,
            "source": source,
            "published": published or 0,
        })

    # Newest first, so if a run limit is ever hit, freshest news wins.
    articles.sort(key=lambda a: a["published"], reverse=True)
    return articles


# --------------------------------------------------------------------------
# Duplicate detection
# --------------------------------------------------------------------------

def deduplicate(articles: list, seen: set, history: list) -> tuple:
    """Returns (fresh_articles, updated_history). Checks each incoming
    article against:
    1) exact title hash seen before (fast path)
    2) other articles already accepted in this same run (same-source or
       different-source republishing within one fetch)
    3) the persisted history of recent articles' title+summary keywords
       (catches the same story from a different source, published in an
       earlier run, with a different headline)
    """
    fresh = []
    accepted_word_sets_this_run = []

    for art in articles:
        h = title_hash(art["title"])
        if h in seen:
            continue

        combined_text = art["title"] + " " + art.get("summary", "")[:400]
        art_words = normalize_words(combined_text)

        is_dup = False

        # Compare against articles already accepted earlier in this run
        for prev_words in accepted_word_sets_this_run:
            if not art_words or not prev_words:
                continue
            intersection = art_words & prev_words
            smaller = min(len(art_words), len(prev_words))
            if smaller and (len(intersection) / smaller) >= KEYWORD_OVERLAP_THRESHOLD:
                is_dup = True
                break

        # Compare against persisted history from previous runs/sources
        if not is_dup:
            for entry in history:
                hist_words = entry.get("words", set())
                if not art_words or not hist_words:
                    continue
                intersection = art_words & hist_words
                smaller = min(len(art_words), len(hist_words))
                if smaller and (len(intersection) / smaller) >= CROSS_SOURCE_OVERLAP_THRESHOLD:
                    is_dup = True
                    break

        # Record this article's fingerprint in history regardless of
        # relevance later, so future similar articles (any source) are caught
        history.append({"ts": datetime.now(timezone.utc).timestamp(), "words": art_words})

        if is_dup:
            continue

        art["_hash"] = h
        fresh.append(art)
        accepted_word_sets_this_run.append(art_words)

    return fresh, history


# --------------------------------------------------------------------------
# DeepSeek
# --------------------------------------------------------------------------

class DeepSeekBalanceError(Exception):
    """Raised when the DeepSeek account is out of credit -- no point
    retrying further calls in this run once this happens."""
    pass


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
        if resp.status_code == 402:
            log.error("DeepSeek balance depleted: %s", resp.text[:500])
            raise DeepSeekBalanceError("DeepSeek account is out of credit (402 Insufficient Balance)")
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

{IRAF_TERMINOLOGY_RULES}
این اصطلاحات را در "title_fa" و "summary_fa" همیشه و بدون استثنا رعایت کنید.

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
    raw = call_deepseek(system_prompt, user_prompt, max_tokens=2200)

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



# --------------------------------------------------------------------------
# Telegram
# --------------------------------------------------------------------------

def send_telegram_message(text: str, max_retries: int = 3):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    # Telegram hard limit is 4096 chars; trim defensively
    text = text[:4090]
    payload = {
        "chat_id": TELEGRAM_CHANNEL_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }

    for attempt in range(max_retries + 1):
        resp = requests.post(url, json=payload, timeout=30)
        if resp.status_code == 200:
            return
        if resp.status_code == 429:
            retry_after = 5
            try:
                retry_after = resp.json().get("parameters", {}).get("retry_after", 5)
            except Exception:
                pass
            log.warning("Telegram rate-limited; waiting %ss before retry (%d/%d)", retry_after, attempt + 1, max_retries)
            time.sleep(retry_after + 1)
            continue
        log.error("Telegram error: %s", resp.text[:500])
        resp.raise_for_status()

    log.error("Telegram send failed after %d retries due to rate limiting", max_retries)


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
    history = load_seen_history()
    fresh_articles, history = deduplicate(articles, seen, history)
    save_seen_history(history)
    log.info("%d articles remain after de-duplication", len(fresh_articles))

    analyzed = []
    for art in fresh_articles:  # no cap: analyze every fresh article this run
        try:
            result = analyze_article(art)
        except DeepSeekBalanceError:
            log.error("Stopping run: DeepSeek balance is depleted. Please top up your DeepSeek account.")
            save_seen_ids(seen)
            return
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
        try:
            send_telegram_message(format_article_message(art))
            time.sleep(1.5)  # stay under Telegram's rate limit
        except Exception as e:
            log.error("Failed to send article message: %s", e)

    log.info("Run complete.")


if __name__ == "__main__":
    main()
