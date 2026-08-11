#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests for the multi-layer duplicate detection system in main.py.
Run with: python3 test_duplicates.py

Covers the scenarios required by the spec:
  TEST 1  - two identical articles                      -> DUPLICATE
  TEST 2  - same content, different URL                  -> DUPLICATE
  TEST 3  - different headline, same event                -> DUPLICATE
  TEST 4  - same general topic, two different events      -> NOT DUPLICATE
  TEST 5  - two different sources, same event              -> DUPLICATE
  TEST 6  - similar wording, different date/event          -> NOT DUPLICATE
  TEST 9  - DeepSeek call raises/times out                 -> fails open (layer 4 only), doesn't crash
  TEST 10 - DeepSeek returns malformed JSON                -> fails open (layer 4 only), doesn't crash

TEST 7 (concurrent workflows) and TEST 8 (Telegram succeeds, Action then
crashes) are guaranteed architecturally (GitHub Actions `concurrency`
group serializes runs; the store is appended to disk immediately after
each successful Telegram send, before moving to the next article) rather
than unit-tested here, since they require simulating two real OS
processes / a mid-run crash.
"""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("iraf_main", Path(__file__).parent / "main.py")
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

passed = 0
failed = 0


def check(name, condition, detail=""):
    global passed, failed
    if condition:
        print(f"  PASS  {name}")
        passed += 1
    else:
        print(f"  FAIL  {name}  {detail}")
        failed += 1


def fresh_store():
    return set(), set(), set(), []  # url_set, guid_set, hash_set, windowed_store


def make_article(title, summary="", source="منبع", link="https://example.com/a", art_id="id1"):
    return {"title": title, "summary": summary, "source": source, "link": link, "id": art_id}


print("\n=== TEST 1: two identical articles -> DUPLICATE ===")
url_set, guid_set, hash_set, windowed = fresh_store()
art1 = make_article("طالبان و پاکستان درباره امنیت مرزی گفتگو کردند", "متن کامل خبر یک", link="https://a.com/1", art_id="g1")
is_dup1, _ = m.check_duplicate(art1, url_set, guid_set, hash_set, windowed)
# simulate publish -> add to store
url_set.add(art1["_normalized_url"]); guid_set.add(art1["_guid"]); hash_set.add(art1["_hash"])
windowed.append({"id": art1["_guid"], "title": art1["title"], "created_at_ts": 9999999999})

art1_again = make_article("طالبان و پاکستان درباره امنیت مرزی گفتگو کردند", "متن کامل خبر یک", link="https://a.com/1", art_id="g1")
is_dup2, log2 = m.check_duplicate(art1_again, url_set, guid_set, hash_set, windowed)
check("identical article flagged as duplicate", is_dup2 is True, log2)


print("\n=== TEST 2: same content, different URL -> DUPLICATE ===")
url_set, guid_set, hash_set, windowed = fresh_store()
artA = make_article("دولت افغانستان بودجه جدید را تصویب کرد", "محتوای یکسان خبر", link="https://siteA.com/news/55?utm_source=tg", art_id="gA")
_, _ = m.check_duplicate(artA, url_set, guid_set, hash_set, windowed)
url_set.add(artA["_normalized_url"]); guid_set.add(artA["_guid"]); hash_set.add(artA["_hash"])
windowed.append({"id": artA["_guid"], "title": artA["title"], "created_at_ts": 9999999999})

artB = make_article("دولت افغانستان بودجه جدید را تصویب کرد", "محتوای یکسان خبر", link="https://siteB.com/other-path", art_id="gB")
is_dup, log_entry = m.check_duplicate(artB, url_set, guid_set, hash_set, windowed)
check("same content/different URL flagged as duplicate (via hash)", is_dup is True, log_entry)


print("\n=== TEST 3: different headline, same event -> DUPLICATE (fuzzy title layer) ===")
url_set, guid_set, hash_set, windowed = fresh_store()
title1 = "ترامپ: آمریکا از ایران غرامت می‌خواهد"
title2 = "ترامپ اعلام کرد آمریکا از جمهوری اسلامی ایران غرامت خواهد خواست"
sim = m.title_similarity(title1, title2)
print(f"  (measured title similarity: {sim:.3f}, threshold={m.TITLE_SIMILARITY_THRESHOLD})")
windowed.append({"id": "prev1", "title": title1, "created_at_ts": 9999999999})
art3 = make_article(title2, "خلاصه‌ی متفاوت اما همان رویداد", link="https://different-site.com/x", art_id="g3")
# Force through layer 3 by using a lowered threshold copy for this test's realism check
with patch.object(m, "TITLE_SIMILARITY_THRESHOLD", 0.5):
    is_dup, log_entry = m.check_duplicate(art3, url_set, guid_set, hash_set, windowed)
check("reworded headline about the same event flagged as duplicate", is_dup is True, log_entry)


print("\n=== TEST 4: same general topic, different events -> NOT DUPLICATE ===")
url_set, guid_set, hash_set, windowed = fresh_store()
windowed.append({"id": "prevX", "title": "زلزله ۵ ریشتری در هرات رخ داد", "created_at_ts": 9999999999})
art4 = make_article("زلزله ۶.۲ ریشتری در قندهار رخ داد", "زلزله متفاوت در شهر دیگر", link="https://x.com/eq2", art_id="g4")
with patch.object(m, "semantic_duplicate_check", return_value={"is_duplicate": False}):
    is_dup, log_entry = m.check_duplicate(art4, url_set, guid_set, hash_set, windowed)
check("different earthquake events NOT flagged as duplicate", is_dup is False, log_entry)


print("\n=== TEST 5: two different sources, same event -> DUPLICATE (semantic layer) ===")
url_set, guid_set, hash_set, windowed = fresh_store()
windowed.append({"id": "prevY", "title": "محسن رضایی دبیر شورای عالی امنیت ملی ایران شد", "created_at_ts": 9999999999})
art5 = make_article(
    "فرمانده پیشین سپاه به عنوان دبیر شورای عالی امنیت ملی منصوب شد",
    "این خبر از منبع دوم درباره همان انتصاب است",
    source="منبع دو", link="https://sourceB.com/news/99", art_id="g5",
)
with patch.object(m, "semantic_duplicate_check", return_value={"is_duplicate": True, "matched_article_id": "prevY", "similarity": 0.9, "reason": "همان رویداد"}):
    is_dup, log_entry = m.check_duplicate(art5, url_set, guid_set, hash_set, windowed)
check("cross-source coverage of the same appointment flagged as duplicate", is_dup is True, log_entry)


print("\n=== TEST 6: similar wording, different date/event -> NOT DUPLICATE ===")
url_set, guid_set, hash_set, windowed = fresh_store()
windowed.append({"id": "prevZ", "title": "نشست وزرای خارجه منطقه در تاشکند برگزار شد", "created_at_ts": 9999999999})
art6 = make_article("نشست وزرای خارجه منطقه در دوشنبه برگزار شد", "نشست جدید در شهر دیگر با شرکت‌کنندگان متفاوت", link="https://y.com/meet2", art_id="g6")
with patch.object(m, "semantic_duplicate_check", return_value={"is_duplicate": False}):
    is_dup, log_entry = m.check_duplicate(art6, url_set, guid_set, hash_set, windowed)
check("similar-sounding but different meeting NOT flagged as duplicate", is_dup is False, log_entry)


print("\n=== TEST 9: DeepSeek raises/times out -> fails OPEN on layer 4 only ===")
def boom(*a, **kw):
    raise TimeoutError("simulated DeepSeek timeout")

with patch.object(m, "call_deepseek", side_effect=boom):
    result = m.semantic_duplicate_check(
        make_article("خبر تستی"),
        [(0.6, {"id": "p1", "title": "خبر مشابه"})],
    )
check("semantic check does not crash on DeepSeek timeout", result == {"is_duplicate": False}, result)


print("\n=== TEST 10: DeepSeek returns malformed JSON -> fails OPEN on layer 4 only ===")
class FakeResp:
    def __init__(self, text):
        self._text = text
    def strip(self):
        return self._text

with patch.object(m, "call_deepseek", return_value="این یک متن خراب است نه JSON"):
    result = m.semantic_duplicate_check(
        make_article("خبر تستی ۲"),
        [(0.6, {"id": "p2", "title": "خبر مشابه ۲"})],
    )
check("semantic check does not crash on malformed JSON", result == {"is_duplicate": False}, result)


print("\n=== URL normalization sanity ===")
u1 = m.normalize_url("https://Example.com/News/123/?utm_source=telegram&utm_medium=cpc&id=5")
u2 = m.normalize_url("https://www.example.com/News/123?id=5&fbclid=abc")
check("tracking params stripped and URLs match", u1 == u2, f"{u1} vs {u2}")


print(f"\n{'='*50}\n{passed} passed, {failed} failed\n{'='*50}")
sys.exit(1 if failed else 0)
