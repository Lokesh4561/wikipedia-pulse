import pytest

from explain.evaluate import results_markdown, row, sample, score, stats


def entry(lang, status, latency=1.0, hallucinated=()):
    return {"lang": lang, "page": "Some_Page", "status": status, "category": "sports", "confidence": "high",
            "reason_en": "r", "reason_native": "r", "sources": [{"url": "https://n/1"}],
            "evidence": {"news": 5, "edits": 1}, "model": "m", "latency_s": latency, "tokens_in": 900,
            "tokens_out": 40, "hallucinated_ids": list(hallucinated), "created_at": "2026-11-15T10:00:00Z"}


def cache():
    c = {f"en|E{i}|2026-11-15": entry("en", "explained") for i in range(30)}
    c |= {f"hi|H{i}|2026-11-15": entry("hi", "explained") for i in range(5)}
    c |= {f"te|U{i}|2026-11-15": entry("te", "unclear", hallucinated=["N9"] if i == 0 else ()) for i in range(20)}
    return c


def test_sample_mixes_status_and_languages():
    picked = sample(cache(), n=20)
    statuses = [e["status"] for _, e in picked]
    assert statuses.count("unclear") == 4 and statuses.count("explained") == 16
    assert sum(e["lang"] == "hi" for _, e in picked) == 5  # small languages are not drowned out by English
    assert sample(cache(), n=20) == picked  # fixed seed: reproducible


def test_stats_over_whole_cache():
    s = stats(cache())
    assert s["total"] == 55 and s["coverage"] == round(35 / 55, 3)
    assert s["hallucinated_citation_rate"] == round(1 / 55, 3) and s["latency_p50_s"] == 1.0
    two = stats({"a|x|d": entry("en", "explained", 0.85), "b|y|d": entry("en", "explained", 1.1)})
    assert two["latency_p95_s"] == 1.1 >= two["latency_p50_s"]


def test_row_has_links_and_empty_verdict():
    r = row("en|Some_Page|2026-11-15", entry("en", "explained"))
    assert r["wiki_url"] == "https://en.wikipedia.org/wiki/Some_Page" and r["source_1"] == "https://n/1"
    assert r["verdict"] == ""


def test_score_and_markdown():
    rows = ([{"lang": "en", "status": "explained", "verdict": "correct"}] * 8
            + [{"lang": "hi", "status": "explained", "verdict": "Partly"}]
            + [{"lang": "hi", "status": "explained", "verdict": "wrong"}]
            + [{"lang": "te", "status": "unclear", "verdict": "correct"}] * 3
            + [{"lang": "te", "status": "unclear", "verdict": "wrong"}]
            + [{"lang": "en", "status": "explained", "verdict": ""}])  # unlabelled rows are ignored
    s = score(rows, stats(cache()))
    assert (s["accuracy"], s["accuracy_incl_partly"], s["right_to_abstain"]) == (0.8, 0.9, 0.75)
    md = results_markdown(s)
    assert "**80%** (8/10)" in md and "en 8/8" in md and "₹0" in md


def test_score_rejects_typos():
    with pytest.raises(ValueError):
        score([{"lang": "en", "status": "explained", "verdict": "corect"}])
