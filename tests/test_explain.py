from datetime import UTC, datetime

from explain import llm
from explain.run import attach, build_evidence, build_prompt, cache_key, candidates, ground, prune
from explain.sources import news_query, parse_rss

NOW = datetime(2026, 11, 15, 6, 0, tzinfo=UTC)


def page(name, ratio, hour="2026-11-15T04:00:00Z"):
    return {"page": name, "views": 5000, "baseline": 100.0, "ratio": ratio, "hour": hour}


def evidence():
    summary = {"description": "Indian actor", "extract": "Dharmendra is an actor.", "url": "https://x/s"}
    edits = [{"time": "2026-11-14T10:00:00Z", "comment": "added date of death"}]
    news = [{"title": "Veteran actor Dharmendra dies at 89", "url": "https://n/1", "domain": "thehindu.com",
             "date": "20261114"}]
    return build_evidence(summary, edits, news, "hi", "धर्मेंद्र")


def test_candidates_round_robin_and_skip_cached():
    data = {"trending": {"en": [page("A", 50), page("B", 40)], "hi": [page("C", 9), page("D", 8)]}}
    cache = {cache_key("en", "A", "2026-11-15T04:00:00Z"): {}}
    order = [(lang, p["page"]) for lang, p in candidates(data, cache)]
    assert order == [("hi", "C"), ("en", "B"), ("hi", "D")]  # every #1 before any #2; cached A skipped


def test_cache_key_is_per_day():
    assert cache_key("en", "X", "2026-11-15T04:00:00Z") == cache_key("en", "X", "2026-11-15T09:00:00Z")


def test_evidence_ids_and_prompt():
    ev = evidence()
    assert [e["id"] for e in ev] == ["S", "E1", "N1"]
    prompt = build_prompt("hi", page("धर्मेंद्र", 30), ev)
    assert "[N1]" in prompt and "Hindi" in prompt and "Never guess" in prompt


def test_grounded_answer_is_kept_with_sources():
    out = ground({"category": "death", "reason_en": "Actor Dharmendra died.", "reason_native": "…",
                  "confidence": "high", "evidence_ids": ["N1", "E1"]}, evidence())
    assert out["status"] == "explained"
    assert [s["id"] for s in out["sources"]] == ["N1", "E1"]


def test_answer_without_news_or_edit_proof_is_rejected():
    out = ground({"category": "death", "reason_en": "He died.", "reason_native": "…",
                  "confidence": "high", "evidence_ids": ["S", "N7"]}, evidence())
    assert out["status"] == "unclear" and out["reason_en"] == ""
    assert out["hallucinated_ids"] == ["N7"]


def test_low_confidence_and_unknown_are_unclear():
    base = {"reason_en": "x", "reason_native": "x", "evidence_ids": ["N1"]}
    assert ground({**base, "category": "death", "confidence": "low"}, evidence())["status"] == "unclear"
    assert ground({**base, "category": "unknown", "confidence": "high"}, evidence())["status"] == "unclear"


def test_anniversary_may_rest_on_summary():
    out = ground({"category": "festival_anniversary", "reason_en": "Today is Children's Day.",
                  "reason_native": "x", "confidence": "medium", "evidence_ids": ["S"]}, evidence())
    assert out["status"] == "explained"


def test_attach_uses_native_reason_and_skips_unclear():
    data = {"trending": {"hi": [page("C", 9), page("D", 8)]}}
    cache = {
        cache_key("hi", "C", "2026-11-15T04:00:00Z"): {"status": "explained", "reason_native": "हिंदी कारण",
                                                       "reason_en": "reason", "category": "sports",
                                                       "confidence": "high", "sources": []},
        cache_key("hi", "D", "2026-11-15T04:00:00Z"): {"status": "unclear"},
    }
    assert attach(data, cache) == 1
    assert data["trending"]["hi"][0]["why"]["reason"] == "हिंदी कारण"
    assert "why" not in data["trending"]["hi"][1]


def test_prune_drops_old_days():
    cache = {"en|A|2026-10-01": {}, "en|B|2026-11-14": {}}
    assert list(prune(cache, NOW)) == ["en|B|2026-11-14"]


def test_news_query():
    assert news_query("Dharmendra_(actor)") == "Dharmendra"
    assert news_query("Shubman Gill") == '"Shubman Gill"'
    assert news_query("Q") is None and news_query(None) is None


def test_generate_skips_unavailable_model_and_reads_json(monkeypatch):
    monkeypatch.setenv("GEMINI_MODEL", "old-model,good-model")

    class Resp:
        def __init__(self, code, body=None):
            self.status_code, self._body, self.text = code, body, ""

        def json(self):
            return self._body

        def raise_for_status(self):
            pass

    answer = '{"category":"sports","reason_en":"r","reason_native":"r","confidence":"high","evidence_ids":["N1"]}'
    body = {"candidates": [{"content": {"parts": [{"text": answer}]}}],
            "usageMetadata": {"promptTokenCount": 900, "candidatesTokenCount": 40}}

    class Session:
        def post(self, url, **kw):
            return Resp(404) if "old-model" in url else Resp(200, body)

    out, meta = llm.generate(Session(), "key", "prompt")
    assert out["category"] == "sports" and meta["model"] == "good-model" and meta["tokens_in"] == 900


RSS = """<?xml version="1.0"?><rss><channel>
<item><title>Shubman Gill hits century in Kolkata Test - The Hindu</title>
<link>https://news.google.com/rss/articles/abc</link><pubDate>Sat, 14 Nov 2026 10:00:00 GMT</pubDate>
<source url="https://www.thehindu.com">The Hindu</source></item>
<item><title>Second headline</title><link>https://n/2</link><pubDate>bad date</pubDate></item>
</channel></rss>"""


def test_parse_rss_strips_publisher_and_reads_dates():
    items = parse_rss(RSS)
    assert items[0] == {"title": "Shubman Gill hits century in Kolkata Test", "url": "https://news.google.com/rss/articles/abc",
                        "domain": "The Hindu", "date": "20261114"}
    assert items[1]["date"] == "" and len(parse_rss(RSS, limit=1)) == 1
    assert parse_rss("not xml") == []


def test_generate_turns_off_thinking_and_skips_slow_model(monkeypatch):
    import requests

    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-flash-lite,gemini-2.5-flash")
    answer = '{"category":"other","reason_en":"r","reason_native":"r","confidence":"medium","evidence_ids":[]}'
    sent = []

    class Resp:
        status_code, text = 200, ""

        def json(self):
            return {"candidates": [{"content": {"parts": [{"text": answer}]}}]}

        def raise_for_status(self):
            pass

    class Session:
        def post(self, url, json, **kw):
            sent.append(json["generationConfig"].get("thinkingConfig"))
            if "lite" in url:
                raise requests.Timeout("slow")
            return Resp()

    out, meta = llm.generate(Session(), "key", "prompt")
    assert meta["model"] == "gemini-2.5-flash" and sent == [{"thinkingBudget": 0}, {"thinkingBudget": 0}]
