"""Evaluate the AI explanations with human labels.

1. Sample (runs in GitHub Actions, needs Databricks secrets):
       python -m explain.evaluate sample --n 50
   writes evals/to_label.csv (open in Excel) and evals/stats.json (numbers over ALL explanations).

2. Label: for every row fill the `verdict` column, then save as evals/labels.csv (CSV UTF-8).
       explained rows: correct | partly | wrong   (is the reason true and the real cause of the spike?)
       unclear rows:   correct | wrong            (correct = no clear reason really existed; wrong = it missed one)

3. Score (runs anywhere, no secrets):
       python -m explain.evaluate score evals/labels.csv
   prints the metrics and writes evals/RESULTS.md for the README.
"""

import argparse
import csv
import json
import math
import os
import random
import sys
from collections import Counter, defaultdict
from statistics import median

COLUMNS = ["key", "lang", "page", "wiki_url", "status", "category", "confidence", "reason_en", "reason_native",
           "source_1", "source_2", "news_found", "edits_found", "model", "latency_s", "tokens_in", "tokens_out",
           "verdict", "notes"]


def row(key: str, e: dict) -> dict:
    sources = e.get("sources", [])
    return {
        "key": key, "lang": e["lang"], "page": e["page"].replace("_", " "),
        "wiki_url": f"https://{e['lang']}.wikipedia.org/wiki/{e['page']}",
        "status": e["status"], "category": e["category"], "confidence": e["confidence"],
        "reason_en": e.get("reason_en", ""), "reason_native": e.get("reason_native", ""),
        "source_1": sources[0]["url"] if len(sources) > 0 else "",
        "source_2": sources[1]["url"] if len(sources) > 1 else "",
        "news_found": e.get("evidence", {}).get("news", 0), "edits_found": e.get("evidence", {}).get("edits", 0),
        "model": e.get("model", ""), "latency_s": e.get("latency_s", ""),
        "tokens_in": e.get("tokens_in", ""), "tokens_out": e.get("tokens_out", ""),
        "verdict": "", "notes": "",
    }


def stats(cache: dict) -> dict:
    """Numbers over every cached explanation (not just the labelled sample)."""
    entries = list(cache.values())
    latencies = sorted(e["latency_s"] for e in entries if isinstance(e.get("latency_s"), int | float))
    n = len(entries) or 1
    return {
        "total": len(entries),
        "explained": sum(e["status"] == "explained" for e in entries),
        "coverage": round(sum(e["status"] == "explained" for e in entries) / n, 3),
        "hallucinated_citation_rate": round(sum(bool(e.get("hallucinated_ids")) for e in entries) / n, 3),
        "latency_p50_s": round(median(latencies), 2) if latencies else None,
        "latency_p95_s": round(latencies[max(0, math.ceil(0.95 * len(latencies)) - 1)], 2) if latencies else None,
        "avg_tokens": round(sum(e.get("tokens_in", 0) + e.get("tokens_out", 0) for e in entries) / n),
        "languages": len({e["lang"] for e in entries}),
        "first": min((e.get("created_at", "") for e in entries), default=""),
        "last": max((e.get("created_at", "") for e in entries), default=""),
    }


def sample(cache: dict, n: int = 50, unclear_share: float = 0.2, seed: int = 42) -> list[tuple[str, dict]]:
    """Mostly explained answers (what users see), some unclear ones (did it wrongly give up?), spread over
    languages so no single language dominates."""
    rng = random.Random(seed)
    by_status = defaultdict(list)
    for key, e in sorted(cache.items()):
        by_status[e["status"]].append((key, e))
    want_unclear = min(len(by_status["unclear"]), round(n * unclear_share))
    want_explained = min(len(by_status["explained"]), n - want_unclear)

    def spread(items, k):
        by_lang = defaultdict(list)
        for item in items:
            by_lang[item[1]["lang"]].append(item)
        for lst in by_lang.values():
            rng.shuffle(lst)
        out, langs = [], sorted(by_lang)
        while len(out) < k and any(by_lang.values()):
            for lang in langs:
                if by_lang[lang] and len(out) < k:
                    out.append(by_lang[lang].pop())
        return out

    return spread(by_status["explained"], want_explained) + spread(by_status["unclear"], want_unclear)


def score(rows: list[dict], cache_stats: dict | None = None) -> dict:
    labelled = [r for r in rows if r.get("verdict", "").strip()]
    expl = [r for r in labelled if r["status"] == "explained"]
    uncl = [r for r in labelled if r["status"] == "unclear"]
    v = Counter(r["verdict"].strip().lower() for r in expl)
    u = Counter(r["verdict"].strip().lower() for r in uncl)
    bad = [r for r in labelled if r["verdict"].strip().lower() not in ("correct", "partly", "wrong")]
    if bad:
        raise ValueError(f"unknown verdicts (use correct/partly/wrong): {[r['verdict'] for r in bad][:5]}")
    by_lang = defaultdict(Counter)
    for r in expl:
        by_lang[r["lang"]][r["verdict"].strip().lower()] += 1
    ne, nu = len(expl) or 1, len(uncl) or 1
    return {
        "labelled": len(labelled), "explained_labelled": len(expl), "unclear_labelled": len(uncl),
        "correct": v["correct"], "partly": v["partly"], "wrong": v["wrong"],
        "accuracy": round(v["correct"] / ne, 3),
        "accuracy_incl_partly": round((v["correct"] + v["partly"]) / ne, 3),
        "right_to_abstain": round(u["correct"] / nu, 3) if uncl else None,
        "by_language": {lang: dict(c) for lang, c in sorted(by_lang.items())},
        "cache": cache_stats or {},
    }


def pct(x) -> str:
    return "n/a" if x is None else f"{x * 100:.0f}%"


def results_markdown(s: dict) -> str:
    c = s["cache"]
    lines = [
        "## AI evaluation",
        "",
        f"{s['explained_labelled']} AI explanations shown on the dashboard and {s['unclear_labelled']} that the "
        "system declined to show were checked by hand against the news and Wikipedia.",
        "",
        "| Metric | Result |",
        "|---|---|",
        f"| Shown explanations that are correct | **{pct(s['accuracy'])}** ({s['correct']}/{s['explained_labelled']}) |",
        f"| Correct or partly correct | {pct(s['accuracy_incl_partly'])} |",
        f"| Wrong | {s['wrong']}/{s['explained_labelled']} |",
        f"| \"Unclear\" cases where declining was right | {pct(s['right_to_abstain'])} |",
    ]
    if c:
        lines += [
            f"| Trending pages explained (coverage) | {pct(c['coverage'])} of {c['total']} |",
            f"| Answers citing evidence that does not exist | {pct(c['hallucinated_citation_rate'])} (rejected by the grounding check) |",
            f"| Latency per explanation (median / p95) | {c['latency_p50_s']} s / {c['latency_p95_s']} s |",
            f"| Tokens per explanation | ~{c['avg_tokens']:,} |",
            f"| Languages | {c['languages']} |",
            "| Cost | ₹0 (free tiers) |",
        ]
    if s["by_language"]:
        lines += ["", "By language (shown explanations): " + ", ".join(
            f"{lang} {d.get('correct', 0)}/{sum(d.values())}" for lang, d in s["by_language"].items())]
    if c.get("first"):
        lines += ["", f"Data: explanations from {c['first'][:10]} to {c['last'][:10]}."]
    return "\n".join(lines) + "\n"


def read_csv(path: str) -> list[dict]:
    with open(path, encoding="utf-8-sig", newline="") as f:  # utf-8-sig: Excel's "CSV UTF-8" adds a BOM
        return list(csv.DictReader(f))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sample")
    s.add_argument("--n", type=int, default=50)
    s.add_argument("--out", default="evals/to_label.csv")
    sc = sub.add_parser("score")
    sc.add_argument("labels", nargs="?", default="evals/labels.csv")
    sc.add_argument("--stats", default="evals/stats.json")
    sc.add_argument("--out", default="evals/RESULTS.md")
    args = parser.parse_args()
    os.makedirs("evals", exist_ok=True)

    if args.cmd == "sample":
        from explain.run import CACHE_PATH, load_cache
        from ingest.fetch_pageviews import Volume

        cache = load_cache(Volume(os.environ["DATABRICKS_HOST"].strip(), os.environ["DATABRICKS_TOKEN"].strip()))
        picked = sample(cache, args.n)
        with open(args.out, "w", encoding="utf-8-sig", newline="") as f:  # BOM so Excel shows Indian scripts
            w = csv.DictWriter(f, fieldnames=COLUMNS)
            w.writeheader()
            w.writerows(row(k, e) for k, e in picked)
        st = stats(cache)
        with open("evals/stats.json", "w", encoding="utf-8") as f:
            json.dump(st, f, indent=2)
        print(f"{len(cache)} explanations in {CACHE_PATH}; wrote {len(picked)} rows to {args.out}")
        print(json.dumps(st, indent=2))
        return 0

    rows = read_csv(args.labels)
    cache_stats = None
    if os.path.exists(args.stats):
        with open(args.stats, encoding="utf-8") as f:
            cache_stats = json.load(f)
    result = score(rows, cache_stats)
    md = results_markdown(result)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(md)
    print(md)
    print(f"Saved {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
