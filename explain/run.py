"""Explain why pages are trending, using an LLM grounded in evidence.

Runs in the hourly publish workflow, right after the dashboard export is downloaded:

    python -m explain.run --data dashboard/data.json

For each newly trending page it gathers evidence (Wikipedia summary, recent edit comments, news
headlines), asks Gemini for a one-sentence reason in English and the page's language, and keeps the
answer only if it cites real evidence. Answers are cached in the Databricks volume, so each page is
explained once per day and the free API quota is respected. Every answer is stored with its sources,
model, latency and token counts, which is what the evaluation script scores.

Environment: DATABRICKS_HOST, DATABRICKS_TOKEN, GEMINI_API_KEY (optional: GEMINI_MODEL, EXPLAIN_MAX_NEW)
"""

import argparse
import json
import os
import sys
import time
from datetime import UTC, datetime, timedelta

import requests

from explain import llm, sources
from ingest.fetch_pageviews import Volume

CACHE_PATH = "/Volumes/workspace/wiki_pulse/exports/explanations.json"
TOP_PER_LANGUAGE = 5   # only explain each language's top trending pages
KEEP_DAYS = 30
EVIDENCE_HOURS = 72    # how far back to look for edits and news
MAX_EDITS, MAX_NEWS = 8, 10

LANGUAGE_NAMES = {
    "en": "English", "hi": "Hindi", "bn": "Bengali", "te": "Telugu", "mr": "Marathi", "ta": "Tamil",
    "ur": "Urdu", "gu": "Gujarati", "kn": "Kannada", "ml": "Malayalam", "pa": "Punjabi", "or": "Odia",
    "as": "Assamese", "sa": "Sanskrit",
}


def cache_key(lang: str, page: str, hour_iso: str) -> str:
    """One explanation per page per day: a page trending for several hours is explained once."""
    return f"{lang}|{page}|{hour_iso[:10]}"


def candidates(data: dict, cache: dict, top_per_language: int = TOP_PER_LANGUAGE) -> list[tuple[str, dict]]:
    """Trending pages without an explanation yet: every language's #1 first, then every #2, and so on."""
    picked = []
    for lang, pages in data.get("trending", {}).items():
        for pos, p in enumerate(pages[:top_per_language]):
            if cache_key(lang, p["page"], p["hour"]) not in cache:
                picked.append((pos, -p["ratio"], lang, p))
    picked.sort(key=lambda x: (x[0], x[1]))
    return [(lang, p) for _, _, lang, p in picked]


def build_evidence(summary: dict | None, edits: list[dict], news: list[dict], lang: str, page: str) -> list[dict]:
    """Number every piece of evidence so the model can cite it (S, E1.., N1..)."""
    ev = []
    if summary and (summary["description"] or summary["extract"]):
        ev.append({"id": "S", "kind": "summary", "url": summary["url"],
                   "text": f"Wikipedia summary: {summary['description']}. {summary['extract']}"})
    history = f"https://{lang}.wikipedia.org/w/index.php?title={page}&action=history"
    for i, e in enumerate(edits[:MAX_EDITS], 1):
        ev.append({"id": f"E{i}", "kind": "edit", "url": history,
                   "text": f"{e['time']} edit comment: {e['comment']}"})
    for i, n in enumerate(news[:MAX_NEWS], 1):
        ev.append({"id": f"N{i}", "kind": "news", "url": n["url"], "title": n["title"],
                   "text": f"{n['date']} news ({n['domain']}): {n['title']}"})
    return ev


def build_prompt(lang: str, p: dict, evidence: list[dict]) -> str:
    language = LANGUAGE_NAMES.get(lang, lang)
    lines = "\n".join(f"[{e['id']}] {e['text']}" for e in evidence) or "(no evidence found)"
    return f"""You explain why a Wikipedia page suddenly became popular.

Page: "{p['page'].replace('_', ' ')}" on {language} Wikipedia
Spike: {p['views']:,} views in the hour starting {p['hour']} (UTC), about {round(p['ratio'])}x its usual \
{round(p['baseline']):,} views an hour.

Evidence (use ONLY this; some of it may be irrelevant):
{lines}

Rules:
- Give the most likely reason for the spike, using only the evidence above. List every id you relied on in
  evidence_ids. The Wikipedia summary [S] says what the page is about, not why it is popular now.
- If the evidence does not show a clear recent reason, set category "unknown", confidence "low",
  evidence_ids [] and reason_en "No clear reason found in recent news or edits." Never guess from general
  knowledge.
- reason_en: one plain sentence, at most 25 words, that states the event itself, for example
  "Actor X died on 14 November at 89." Do not mention page views, traffic, readers or popularity.
- reason_native: the same sentence written in {language}.
- confidence: "high" only if news or edits state the reason directly."""


def ground(answer: dict, evidence: list[dict]) -> dict:
    """Keep an answer only if it cites real news or edit evidence (anniversaries may rest on the summary)."""
    ids = {e["id"] for e in evidence}
    cited = [i for i in answer.get("evidence_ids", []) if i in ids]
    has_proof = any(i[0] in "EN" for i in cited)
    category = answer.get("category", "unknown")
    explained = category != "unknown" and answer.get("confidence") != "low" and (
        has_proof or (category == "festival_anniversary" and "S" in cited))
    by_id = {e["id"]: e for e in evidence}
    return {
        "status": "explained" if explained else "unclear",
        "category": category if explained else "unknown",
        "confidence": answer.get("confidence", "low") if explained else "low",
        "reason_en": answer.get("reason_en", "").strip() if explained else "",
        "reason_native": answer.get("reason_native", "").strip() if explained else "",
        "cited": cited,
        "hallucinated_ids": [i for i in answer.get("evidence_ids", []) if i not in ids],
        "sources": [{"id": i, "title": by_id[i].get("title") or by_id[i]["kind"], "url": by_id[i]["url"]}
                    for i in cited if by_id[i]["kind"] != "summary"][:3],
    }


def explain_page(session, api_key: str, lang: str, p: dict, now: datetime) -> dict:
    since = now - timedelta(hours=EVIDENCE_HOURS)
    summary = sources.wiki_summary(session, lang, p["page"])
    edits, en_title = sources.wiki_recent_edits(session, lang, p["page"], since)
    # English news about the English title, plus news in the page's own language about its own title
    query = sources.news_query(en_title or p["page"])
    news = sources.news_headlines(session, query, "en") if query else []
    native_query = sources.news_query(p["page"]) if lang != "en" else None
    if native_query:
        seen = {n["title"] for n in news}
        news += [n for n in sources.news_headlines(session, native_query, lang, limit=4) if n["title"] not in seen]
    evidence = build_evidence(summary, edits, news, lang, p["page"])
    answer, meta = llm.generate(session, api_key, build_prompt(lang, p, evidence))
    return {
        "lang": lang, "page": p["page"], "hour": p["hour"], "ratio": p["ratio"], "views": p["views"],
        **ground(answer, evidence), **meta,
        "evidence": {"edits": len(edits), "news": len(news), "news_query": query},
        "created_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def attach(data: dict, cache: dict) -> int:
    """Add a `why` field to trending pages that have a grounded explanation. Returns how many."""
    n = 0
    for lang, pages in data.get("trending", {}).items():
        for p in pages:
            e = cache.get(cache_key(lang, p["page"], p["hour"]))
            if e and e["status"] == "explained":
                p["why"] = {"reason": e["reason_native"] if lang != "en" else e["reason_en"],
                            "reason_en": e["reason_en"], "category": e["category"],
                            "confidence": e["confidence"], "sources": e["sources"]}
                n += 1
    return n


def prune(cache: dict, now: datetime, keep_days: int = KEEP_DAYS) -> dict:
    cutoff = (now - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    return {k: v for k, v in cache.items() if k.rsplit("|", 1)[-1] >= cutoff}


def load_cache(volume: Volume) -> dict:
    try:
        return json.loads(volume.download(CACHE_PATH))
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            return {}
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data", default="dashboard/data.json")
    parser.add_argument("--max-new", type=int, default=int(os.getenv("EXPLAIN_MAX_NEW") or 8),
                        help="LLM calls per run (keeps within the Gemini free tier)")
    parser.add_argument("--budget-seconds", type=int, default=240,
                        help="stop starting new pages after this long, so publishing is never delayed")
    args = parser.parse_args()

    host, token = os.getenv("DATABRICKS_HOST", "").strip(), os.getenv("DATABRICKS_TOKEN", "").strip()
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not host or not token:
        print("DATABRICKS_HOST and DATABRICKS_TOKEN must be set.")
        return 1

    with open(args.data, encoding="utf-8") as f:
        data = json.load(f)
    volume, session, now = Volume(host, token), sources.new_session(), datetime.now(UTC)
    cache = load_cache(volume)

    todo = candidates(data, cache)
    print(f"{len(cache)} cached explanations, {len(todo)} trending pages to explain, up to {args.max_new} this run")
    if not api_key:
        print("GEMINI_API_KEY is not set: showing cached explanations only.")
        todo = []

    started = time.monotonic()
    for n, (lang, p) in enumerate(todo[: args.max_new], 1):
        if time.monotonic() - started > args.budget_seconds:
            print(f"  time budget of {args.budget_seconds}s used, the rest wait for the next run")
            break
        print(f"  [{n}] {lang}:{p['page']} ...")
        try:
            e = explain_page(session, api_key, lang, p, now)
        except llm.QuotaExceeded as exc:
            print(f"  Gemini free quota reached, stopping for this run: {exc}")
            break
        except Exception as exc:  # noqa: BLE001 - one bad page must not stop the others
            print(f"  error     {lang}:{p['page']}: {exc}")
            continue
        cache[cache_key(lang, p["page"], p["hour"])] = e
        volume.upload(CACHE_PATH, json.dumps(cache, ensure_ascii=False).encode("utf-8"))  # keep progress
        print(f"  {e['status']:<9} {lang}:{p['page']} [{e['category']}, {e['confidence']}] "
              f"{e['reason_en'] or '-'} ({e['model']}, {e['latency_s']}s, news {e['evidence']['news']}, "
              f"edits {e['evidence']['edits']})")

    cache = prune(cache, now)
    volume.upload(CACHE_PATH, json.dumps(cache, ensure_ascii=False).encode("utf-8"))
    shown = attach(data, cache)
    with open(args.data, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    print(f"Done: {shown} trending pages on the dashboard now have an AI explanation")
    return 0


if __name__ == "__main__":
    sys.exit(main())
