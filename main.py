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
import difflib
import logging
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, parse_qsl, urlencode

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

PUBLISHED_STORE_FILE = Path("published_articles.jsonl")  # single source of truth
PROCESSED_IDS_FILE = Path("processed_ids.txt")  # every article ever analyzed, relevant or not
TITLE_SIMILARITY_THRESHOLD = float(os.environ.get("TITLE_SIMILARITY_THRESHOLD", "0.90"))  # near-certain textual match -> auto-block
SEMANTIC_CANDIDATE_THRESHOLD = 0.45  # min title similarity to bother asking DeepSeek to confirm/deny
SEMANTIC_TIME_WINDOW_DAYS = 14  # how far back to look for fuzzy/semantic candidates
REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)

MAX_ARTICLES_PER_RUN = int(os.environ.get("MAX_ARTICLES_PER_RUN", "0"))  # 0 = no limit

CATEGORY_MAP = {
    "سیاسی": "#سیاسی 🏛️",
    "اقتصادی": "#اقتصادی 💰",
    "فرهنگی": "#فرهنگی 🎭",
    "مهاجرین": "#مهاجرین 🧳",
    "اجتماعی": "#اجتماعی 🧑‍🤝‍🧑",
    "ورزشی": "#ورزشی ⚽",
}

# Deterministic priority tiers -- combines geography AND topic relevance,
# never left to the model's free judgement (that was mixing/forcing
# irrelevant countries into unrelated stories, e.g. sports).
#
#   Tier 1: افغانستان -- ANY topic (سیاسی/فرهنگی/مهاجرین/اجتماعی/اقتصادی/ورزشی)
#   Tier 2: ایران -- ONLY IF: مرتبط با افغانستان, یا سیاست خارجی ایران,
#           یا روابط ایران-آمریکا/اسرائیل (gated by topic_qualifies)
#   Tier 3: پاکستان/هند -- ONLY IF: موضوعات سیاسی این دو کشور یا مرتبط با
#           افغانستان (gated by topic_qualifies)
#   Tier 4: کشورهای آسیای مرکزی -- هر موضوعی
AFGHANISTAN_COUNTRIES = {"افغانستان"}
IRAN_COUNTRIES = {"ایران"}
PK_IN_COUNTRIES = {"پاکستان", "هند"}
CENTRAL_ASIA_COUNTRIES = {
    "تاجیکستان", "ازبکستان", "ترکمنستان", "قزاقستان", "قرقیزستان", "آسیای مرکزی",
}
DEFAULT_PRIORITY = 5  # no recognized priority country, or topic didn't qualify

FLAG_MAP = {
    "افغانستان": "🇦🇫",
    "ایران": "🇮🇷",
    "پاکستان": "🇵🇰",
    "هند": "🇮🇳",
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


def compute_priority(countries: list, topic_qualifies: bool) -> int:
    """Determines priority deterministically. `topic_qualifies` comes from
    the model's judgement of whether an Iran/Pakistan/India story meets the
    narrower topical criteria (see IRAF_TOPIC_QUALIFICATION_RULES) -- but
    the COUNTRY tier itself, and Afghanistan's unconditional top priority,
    are always decided in code, never by the model."""
    countries = set(countries or [])

    if countries & AFGHANISTAN_COUNTRIES:
        return 1
    if countries & IRAN_COUNTRIES and topic_qualifies:
        return 2
    if countries & PK_IN_COUNTRIES and topic_qualifies:
        return 3
    if countries & CENTRAL_ASIA_COUNTRIES:
        return 4
    return DEFAULT_PRIORITY

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

# Determines whether an Iran/Pakistan/India story qualifies for the higher
# priority tier -- Afghanistan itself is ALWAYS priority 1 regardless of
# topic, so this only matters when "افغانستان" is NOT among the countries.
IRAF_TOPIC_QUALIFICATION_RULES = """
قوانین تعیین "topic_qualifies" (فقط وقتی افغانستان جزو کشورهای خبر نیست):

اگر "ایران" در "countries" باشد، "topic_qualifies" را true بگذارید فقط اگر
خبر یکی از این‌هاست:
- موضوعی که مستقیماً به افغانستان مربوط می‌شود (حتی اگر افغانستان را در
  countries نگذاشتید چون کشور اصلی خبر نبود)
- اظهارنظر یا سیاست ایران درباره‌ی سیاست خارجی‌اش (نه اخبار داخلی صرف مثل
  اقتصاد داخلی، حوادث محلی، ورزش داخلی ایران)
- روابط ایران با آمریکا و/یا اسرائیل (تحریم، مذاکره، تنش، جنگ، دیپلماسی)
در غیر این صورت (مثلاً خبر داخلی محض ایران بدون ربط به موارد بالا)، false.

اگر "پاکستان" یا "هند" در "countries" باشد، "topic_qualifies" را true
بگذارید فقط اگر خبر یکی از این‌هاست:
- موضوعات سیاسی داخلی یا خارجی این دو کشور (انتخابات، دولت، پارلمان،
  سیاست خارجی، روابط دیپلماتیک)
- هر خبری که مستقیماً به افغانستان مربوط می‌شود
در غیر این صورت (مثلاً ورزش، حوادث محلی بی‌ربط، سرگرمی)، false.

برای بقیه‌ی کشورها (تاجیکستان، ازبکستان، ترکمنستان، قزاقستان، قرقیزستان)
یا وقتی "countries" خالی است، "topic_qualifies" را false بگذارید (اهمیتی
ندارد -- این کشورها اولویت خودشان را بدون نیاز به این شرط می‌گیرند).
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


def normalize_persian_text(text: str) -> str:
    """Deep normalization for fuzzy comparison: unifies Arabic/Persian
    look-alike letters, strips diacritics, URLs, emoji, hashtags,
    punctuation, half-spaces, and collapses whitespace."""
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = (text
            .replace("ي", "ی").replace("ك", "ک")
            .replace("ة", "ه").replace("ۀ", "ه")
            .replace("أ", "ا").replace("إ", "ا").replace("آ", "ا"))
    # Strip Arabic diacritics/tashkeel
    text = re.sub(r"[\u064B-\u065F\u0670\u06D6-\u06ED]", "", text)
    # Strip URLs
    text = re.sub(r"https?://\S+", " ", text)
    # Strip emoji (broad ranges)
    text = re.sub(
        r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF\U0001F1E6-\U0001F1FF\U00002190-\U000021FF\U00002B00-\U00002BFF]",
        " ", text,
    )
    text = text.replace("#", " ").replace("\u200c", " ")  # hashtags, half-space (ZWNJ)
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)  # punctuation
    text = re.sub(r"\d+", " ", text)  # numbers often differ (dates/counts) without changing the story
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def normalize_url(url: str) -> str:
    """Strips tracking params (utm_*, fbclid, gclid, ...), normalizes host
    and trailing slash, so the same article linked with different tracking
    params is recognized as the same URL."""
    if not url:
        return ""
    try:
        parsed = urlparse(url)
    except Exception:
        return url.strip().rstrip("/")

    netloc = parsed.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    path = parsed.path.rstrip("/")

    blocked_params = {
        "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
        "fbclid", "gclid", "igshid", "ref", "ref_src",
    }
    query_pairs = sorted(
        (k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True)
        if k.lower() not in blocked_params
    )
    query = urlencode(query_pairs)

    normalized = f"https://{netloc}{path}"
    if query:
        normalized += f"?{query}"
    return normalized


def article_hash(title: str, content: str, source: str) -> str:
    """Layer 2 -- deterministic SHA-256 fingerprint of the normalized
    title+content+source. Catches exact/near-exact re-publications even if
    URL/GUID differ."""
    norm = (
        normalize_persian_text(title) + "|" +
        normalize_persian_text(content)[:600] + "|" +
        normalize_persian_text(source)
    )
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()


def title_similarity(a: str, b: str) -> float:
    """Layer 3 -- fuzzy title similarity (0..1) after deep normalization.
    Catches reworded/reordered/punctuation-different headlines about the
    same story."""
    na, nb = normalize_persian_text(a), normalize_persian_text(b)
    if not na or not nb:
        return 0.0
    return difflib.SequenceMatcher(None, na, nb).ratio()


def sanitize_text(text: str) -> str:
    """Remove control characters and normalize unicode so text is safe to
    send to the DeepSeek API as JSON. This is the core fix for the 400
    Bad Request errors that occurred with raw Persian article content."""
    if not text:
        return ""
    text = unicodedata.normalize("NFC", text)
    text = "".join(
        ch for ch in text
        if ch in ("\n", "\t") or unicodedata.category(ch)[0] != "C"
    )
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# --------------------------------------------------------------------------
# Persistent duplicate store (single source of truth -- survives across
# GitHub Actions runs via git commit; see pipeline.yml)
# --------------------------------------------------------------------------

def load_published_store() -> list:
    """Loads the FULL history (no time limit) -- required for URL/GUID/Hash
    checks, which must never expire per spec."""
    if not PUBLISHED_STORE_FILE.exists():
        return []
    records = []
    for line in PUBLISHED_STORE_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


def append_published_record(record: dict):
    """Appends immediately (not batched) so that even if the process
    crashes on the NEXT article, already-published articles are not lost
    and won't be reposted on the next run."""
    with open(PUBLISHED_STORE_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_processed_ids() -> set:
    """Every article ID (guid+hash) that has EVER been sent to DeepSeek for
    analysis, whether it turned out relevant or not. Without this, an
    irrelevant article that keeps reappearing in the Inoreader feed gets
    re-analyzed (and re-billed) on every single run until it ages out of
    the freshness window -- this was silently burning DeepSeek tokens."""
    if not PROCESSED_IDS_FILE.exists():
        return set()
    return set(PROCESSED_IDS_FILE.read_text(encoding="utf-8").splitlines())


def append_processed_id(article_key: str):
    with open(PROCESSED_IDS_FILE, "a", encoding="utf-8") as f:
        f.write(article_key + "\n")


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

        # Freshness filter. Missing/invalid published dates are treated
        # CAUTIOUSLY and skipped -- the previous logic let articles with no
        # published date bypass the filter entirely (a falsy `published`
        # made the whole condition False), which is exactly how old BBC
        # Persian-style articles with missing timestamps were slipping
        # through as if they were fresh.
        if not published or published < cutoff_ts:
            continue

        articles.append({
            "id": item.get("id") or hashlib.sha256(normalize_persian_text(title).encode("utf-8")).hexdigest(),
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
# Duplicate detection (multi-layer, persistent-store-backed)
# --------------------------------------------------------------------------
#
#   Layer 1: normalized URL / GUID exact match      (no time limit)
#   Layer 2: SHA-256 hash of normalized article      (no time limit)
#   Layer 3: fuzzy title similarity (difflib)         (last 14 days)
#   Layer 4: DeepSeek semantic check (advisory only,  (last 14 days,
#            never the sole authority)                 only for L3 candidates)
#
# The persistent JSONL store (published_articles.jsonl) is the single
# source of truth. DeepSeek is consulted but never trusted alone -- if it
# times out, errors, or returns bad JSON, the article is NOT auto-blocked
# by layer 4 (fail-open on layer 4 only); layers 1-3 remain fully in force
# regardless of DeepSeek's availability.

def find_similarity_candidates(article: dict, windowed_store: list) -> list:
    """Returns [(similarity, record), ...] sorted descending, for records
    within the recent time window whose title is at least loosely similar."""
    scored = []
    for rec in windowed_store:
        sim = title_similarity(article["title"], rec.get("title", ""))
        if sim >= SEMANTIC_CANDIDATE_THRESHOLD:
            scored.append((sim, rec))
    scored.sort(key=lambda x: -x[0])
    return scored[:5]


def semantic_duplicate_check(article: dict, candidates: list) -> dict:
    """Layer 4. Asks DeepSeek whether the new article covers the same
    real-world event as one of the candidate articles. Structured JSON
    output only; any failure fails OPEN (is_duplicate=False) since layers
    1-3 are the hard guarantees, not this layer."""
    if not candidates:
        return {"is_duplicate": False}

    prev_list = "\n".join(
        f"- ID: {rec.get('id')} | عنوان: {rec.get('title', '')}"
        for _, rec in candidates
    )
    system_prompt = """
شما یک سیستم تشخیص خبر تکراری هستید. فقط بررسی کنید که آیا خبر جدید دقیقاً
همان رویداد یکی از خبرهای قبلی است یا نه.

قوانین:
1. اگر همان رویداد است ولی با عنوان متفاوت یا بازنویسی متفاوت بیان شده، Duplicate است.
2. اگر فقط موضوع کلی مشابه است ولی رویداد، شخص، تاریخ یا مکان متفاوت است، Duplicate نیست.
3. صرفاً به‌خاطر شباهت موضوعی Duplicate اعلام نکنید.
4. اگر مطمئن نیستید، Duplicate اعلام نکنید (false بگذارید).
5. فقط یک شیء JSON معتبر برگردانید، بدون هیچ متن یا Markdown اضافه.
"""
    user_prompt = f"""
NEW ARTICLE:
Title: {article['title']}
Content: {article.get('summary', '')[:500]}

PREVIOUS ARTICLES:
{prev_list}

خروجی دقیقاً به این شکل:
{{"is_duplicate": true/false, "similarity": 0.00, "matched_article_id": "ID یا خالی", "reason": "توضیح کوتاه"}}
"""
    try:
        raw = call_deepseek(system_prompt, user_prompt, max_tokens=300)
    except DeepSeekBalanceError:
        raise
    except Exception as e:
        log.warning("Semantic duplicate check failed (%s); layers 1-3 remain authoritative, treating as not-duplicate for layer 4", e)
        return {"is_duplicate": False}

    cleaned = re.sub(r"^```(json)?|```$", "", raw.strip(), flags=re.MULTILINE).strip()
    match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
    if match:
        cleaned = match.group(0)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        log.warning("Semantic duplicate check returned invalid JSON; treating as not-duplicate for layer 4")
        return {"is_duplicate": False}


def check_duplicate(article: dict, url_set: set, guid_set: set, hash_set: set, windowed_store: list) -> tuple:
    """Runs all 4 layers in order (cheapest/most certain first) and returns
    (is_duplicate: bool, log_entry: dict) for full traceability."""
    norm_url = normalize_url(article.get("link", ""))
    guid = article.get("id", "")
    h = article_hash(article["title"], article.get("summary", ""), article.get("source", ""))
    article["_normalized_url"] = norm_url
    article["_guid"] = guid
    article["_hash"] = h

    log_entry = {
        "article_id": guid,
        "source": article.get("source", ""),
        "title": article["title"],
        "url": article.get("link", ""),
        "url_duplicate": bool(norm_url and norm_url in url_set),
        "guid_duplicate": bool(guid and guid in guid_set),
        "hash_duplicate": h in hash_set,
    }

    if log_entry["url_duplicate"] or log_entry["guid_duplicate"] or log_entry["hash_duplicate"]:
        log_entry["title_similarity"] = None
        log_entry["semantic_duplicate"] = None
        log_entry["final_decision"] = "SKIPPED_DUPLICATE"
        log.info("DUPLICATE[url/guid/hash] %s", json.dumps(log_entry, ensure_ascii=False))
        return True, log_entry

    candidates = find_similarity_candidates(article, windowed_store)
    top_sim = candidates[0][0] if candidates else 0.0
    log_entry["title_similarity"] = round(top_sim, 3)

    if top_sim >= TITLE_SIMILARITY_THRESHOLD:
        log_entry["matched_article_id"] = candidates[0][1].get("id")
        log_entry["semantic_duplicate"] = None
        log_entry["final_decision"] = "SKIPPED_DUPLICATE"
        log.info("DUPLICATE[title_similarity=%.2f] %s", top_sim, json.dumps(log_entry, ensure_ascii=False))
        return True, log_entry

    semantic = semantic_duplicate_check(article, candidates)
    log_entry["semantic_duplicate"] = semantic.get("is_duplicate", False)
    log_entry["deepseek_decision"] = semantic

    if semantic.get("is_duplicate"):
        log_entry["matched_article_id"] = semantic.get("matched_article_id")
        log_entry["final_decision"] = "SKIPPED_DUPLICATE"
        log.info("DUPLICATE[semantic] %s", json.dumps(log_entry, ensure_ascii=False))
        return True, log_entry

    log_entry["final_decision"] = "APPROVED"
    log.info("NOT_DUPLICATE %s", json.dumps(log_entry, ensure_ascii=False))
    return False, log_entry


def final_duplicate_gate(article: dict) -> bool:
    """The mandatory last check, right before Telegram send. Reloads the
    store fresh from disk (defends against another process having written
    to it since the run started) and re-checks the hard layers (1/2/3,
    skipping the slower semantic layer since layers 1-3 already ran)."""
    store = load_published_store()
    url_set = {r["normalized_url"] for r in store if r.get("normalized_url")}
    guid_set = {r["guid"] for r in store if r.get("guid")}
    hash_set = {r["article_hash"] for r in store if r.get("article_hash")}

    norm_url = article.get("_normalized_url", "")
    guid = article.get("_guid", "")
    h = article.get("_hash", "")

    if (norm_url and norm_url in url_set) or (guid and guid in guid_set) or (h and h in hash_set):
        return True  # is duplicate -> block

    cutoff = datetime.now(timezone.utc).timestamp() - (SEMANTIC_TIME_WINDOW_DAYS * 86400)
    windowed = [r for r in store if r.get("created_at_ts", 0) >= cutoff]
    for rec in windowed:
        if title_similarity(article["title"], rec.get("title", "")) >= TITLE_SIMILARITY_THRESHOLD:
            return True  # near-certain textual match -> block without needing another semantic call

    return False


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

نکته مهم و حیاتی درباره فیلتر ارتباط (relevant):
ارتباط را فقط بر اساس **محتوای واقعی خبر** تشخیص دهید، نه بر اساس این‌که
از کدام فید یا منبع آمده است. این‌که خبر از یک فید موضوعی خاص می‌آید،
دلیلی برای مرتبط دانستن آن نیست و نباید در قضاوت شما تأثیر بگذارد.
- اگر خبر واقعاً به افغانستان، ایران، یا منطقه ربط ندارد (مثلاً یک خبر
  ورزشی اروپایی، حادثه‌ی محلی یک کشور ثالث، یا خبر عمومی بی‌ربط)، آن را
  "relevant": false بگذارید، حتی اگر از همان فید آمده باشد.
- تبلیغات و اسپم همیشه false.
- محتوای صریحاً ضد ایران همیشه false.
- در قضاوت نهایی، به این سؤال ساده جواب بدهید: «اگر این خبر را به یک
  خواننده‌ی افغان یا ایرانی نشان دهم، آیا واقعاً برایش مرتبط و بامعناست؟»

نکته بسیار مهم و حیاتی درباره کشورها -- **ممنوعیت نزدیک‌سازی مصنوعی**:
"countries" باید فقط کشورهایی را شامل شود که **واقعاً و به‌طور مستقیم در
محتوای خبر نقش دارند** (مثلاً بازیگر رویداد، محل وقوع، یا طرف اصلی گفت‌وگو
هستند). هرگز کشوری مثل افغانستان یا ایران را صرفاً برای «نزدیک‌تر کردن»
خبر به کانال یا توجیه انتشار آن اضافه نکنید. یک خبر ورزشی اروپایی که هیچ
بازیکن، تیم، یا رویداد افغان/ایرانی در آن نیست، نباید هیچ‌کدام از این
کشورها را در "countries" داشته باشد -- حتی اگر از فید افغانستان آمده باشد.
اگر هیچ کشور مرتبطی در متن نیست، "countries" را آرایه‌ی خالی بگذارید.

نام‌های استاندارد مجاز (دقیقاً همین املا، حداکثر ۳ کشور، به ترتیب ارتباط):
افغانستان 🇦🇫 | ایران 🇮🇷 | پاکستان 🇵🇰 | هند 🇮🇳 | تاجیکستان 🇹🇯 |
ازبکستان 🇺🇿 | ترکمنستان 🇹🇲 | قزاقستان 🇰🇿 | قرقیزستان 🇰🇬
اولویت را شما تعیین نکنید؛ آن در کد به‌صورت خودکار محاسبه می‌شود.

{IRAF_TOPIC_QUALIFICATION_RULES}

فقط و فقط یک شیء JSON معتبر برگردانید (بدون هیچ متن اضافه، بدون Markdown)
با این ساختار دقیق:
{{
  "relevant": true/false,
  "category": "سیاسی|اقتصادی|فرهنگی|مهاجرین|اجتماعی|ورزشی",
  "title_fa": "عنوان ترجمه‌شده و خبری به فارسی (بدون هشتگ یا ایموجی)",
  "summary_fa": "خلاصه ۳ تا ۵ جمله‌ای روان و بی‌طرف به فارسی، در حد یک پاراگراف کامل خبری",
  "key_points": ["نکته کلیدی اول", "نکته کلیدی دوم", "نکته کلیدی سوم (حداکثر ۴ نکته)"],
  "countries": ["افغانستان"],
  "topic_qualifies": true/false,
  "reason": "دلیل کوتاه ارتباط یا عدم ارتباط، و دلیل انتخاب کشورها"
}}

نکات مهم:
- "key_points" باید نکات مشخص و خبری باشند، نه تکرار خلاصه.
- "topic_qualifies" را طبق قوانین بالا (IRAF_TOPIC_QUALIFICATION_RULES) پر کنید -- فقط برای خبرهای ایران/پاکستان/هند معنا دارد؛ برای بقیه false بگذارید.
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
        result["priority"] = compute_priority(result.get("countries", []), bool(result.get("topic_qualifies")))
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

    # Load the persistent store once. url_set/guid_set/hash_set are NEVER
    # time-limited (per spec). windowed_store is used only for the fuzzy
    # title / semantic layers, bounded to a recent window for speed.
    store = load_published_store()
    url_set = {r["normalized_url"] for r in store if r.get("normalized_url")}
    guid_set = {r["guid"] for r in store if r.get("guid")}
    hash_set = {r["article_hash"] for r in store if r.get("article_hash")}
    cutoff = datetime.now(timezone.utc).timestamp() - (SEMANTIC_TIME_WINDOW_DAYS * 86400)
    windowed_store = [r for r in store if r.get("created_at_ts", 0) >= cutoff]
    log.info("Loaded %d published records (%d within %dd window) for duplicate checks",
              len(store), len(windowed_store), SEMANTIC_TIME_WINDOW_DAYS)

    processed_ids = load_processed_ids()
    log.info("Loaded %d previously-processed article IDs (skipped without re-spending tokens)", len(processed_ids))

    published_count = 0
    skipped_duplicate_count = 0
    skipped_already_processed_count = 0
    to_publish = []  # (result, art) pairs approved for sending, before ordering

    for art in articles:
        article_key = art.get("id", "")
        if article_key and article_key in processed_ids:
            skipped_already_processed_count += 1
            continue  # already analyzed in a previous run (relevant or not) -- don't re-spend tokens

        try:
            is_dup, _log_entry = check_duplicate(art, url_set, guid_set, hash_set, windowed_store)
        except DeepSeekBalanceError:
            log.error("Stopping run: DeepSeek balance is depleted. Please top up your DeepSeek account.")
            return
        except Exception as e:
            log.error("Duplicate check failed for '%s': %s -- skipping to be safe", art["title"], e)
            continue

        if is_dup:
            skipped_duplicate_count += 1
            if article_key:
                append_processed_id(article_key)
                processed_ids.add(article_key)
            continue

        # Register this article's fingerprint immediately (in-memory only,
        # not yet persisted) so that if another source in THIS SAME run
        # covers the same story, it gets caught too -- regardless of
        # whether this particular instance turns out to be relevant.
        if art.get("_normalized_url"):
            url_set.add(art["_normalized_url"])
        if art.get("_guid"):
            guid_set.add(art["_guid"])
        if art.get("_hash"):
            hash_set.add(art["_hash"])
        windowed_store.append({
            "id": art.get("_guid", ""), "title": art["title"],
            "created_at_ts": datetime.now(timezone.utc).timestamp(),
        })

        try:
            result = analyze_article(art)
        except DeepSeekBalanceError:
            log.error("Stopping run: DeepSeek balance is depleted. Please top up your DeepSeek account.")
            return
        except Exception as e:
            log.error("Failed to analyze article '%s': %s", art["title"], e)
            continue  # NOT marked as processed -> will retry next run (transient errors shouldn't be permanent)

        # Mark as processed regardless of relevance -- this is the fix:
        # an irrelevant article must never be re-analyzed by DeepSeek again.
        if article_key:
            append_processed_id(article_key)
            processed_ids.add(article_key)

        if not result.get("relevant"):
            continue

        result["link"] = art["link"]
        to_publish.append((result, art))

    log.info("%d articles approved for publishing (after all duplicate layers)", len(to_publish))
    log.info("%d articles skipped as already-processed (token savings)", skipped_already_processed_count)

    to_publish.sort(key=lambda pair: pair[0].get("priority", 5))

    for result, art in to_publish:
        # FINAL_DUPLICATE_GATE -- mandatory last check right before Telegram,
        # reloaded fresh from disk as a defense against concurrent processes.
        if final_duplicate_gate(art):
            log.info("SKIPPED_DUPLICATE (final gate) '%s'", art["title"])
            skipped_duplicate_count += 1
            continue

        try:
            send_telegram_message(format_article_message(result))
            time.sleep(1.5)  # stay under Telegram's rate limit
        except Exception as e:
            log.error("Telegram send FAILED for '%s': %s -- NOT recording as published, will retry next run", art["title"], e)
            continue  # do NOT append to the store -> safe to retry next run

        # PUBLISH succeeded -> immediately persist (source of truth)
        now_ts = datetime.now(timezone.utc).timestamp()
        record = {
            "id": art.get("_guid", ""),
            "source": art.get("source", ""),
            "original_url": art.get("link", ""),
            "normalized_url": art.get("_normalized_url", ""),
            "guid": art.get("_guid", ""),
            "title": art.get("title", ""),
            "normalized_title": normalize_persian_text(art.get("title", "")),
            "article_hash": art.get("_hash", ""),
            "published_at": datetime.now(timezone.utc).isoformat(),
            "telegram_channel": TELEGRAM_CHANNEL_ID,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "created_at_ts": now_ts,
            "status": "PUBLISHED",
        }
        append_published_record(record)
        published_count += 1

    log.info("Run complete. Published: %d, Skipped as duplicate: %d", published_count, skipped_duplicate_count)


if __name__ == "__main__":
    main()
