"""Evidence collectors for "why is this page trending?".

Three free, key-less sources:
  - Wikipedia page summary (what the page is about)
  - recent edit comments on the page (editors often record the news: "died on ...", "won ...")
  - news headlines from the GDELT DOC 2.0 API (global news in 65+ languages)
"""

import re
import time
from datetime import datetime
from urllib.parse import quote

import requests

USER_AGENT = "wikipedia-pulse/1.0 (https://github.com/Lokesh4561/wikipedia-pulse)"
GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_MIN_INTERVAL = 5.5  # GDELT asks for at most one request every 5 seconds

_last_gdelt_call = 0.0


def new_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


def _get(session: requests.Session, url: str, params: dict | None = None, retries: int = 3):
    """GET with retries on network errors, 429 and 5xx. Returns None for 404."""
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code == 404:
                return None
            if resp.status_code == 429 or resp.status_code >= 500:
                raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
            resp.raise_for_status()
            return resp
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError):
            if attempt == retries:
                raise
            time.sleep(6 * attempt)
    return None


def wiki_summary(session, lang: str, title: str) -> dict | None:
    resp = _get(session, f"https://{lang}.wikipedia.org/api/rest_v1/page/summary/{quote(title, safe='')}")
    if resp is None:
        return None
    d = resp.json()
    return {
        "description": d.get("description", ""),
        "extract": (d.get("extract") or "")[:700],
        "url": d.get("content_urls", {}).get("desktop", {}).get("page", ""),
    }


def wiki_recent_edits(session, lang: str, title: str, since: datetime) -> tuple[list[dict], str | None]:
    """Edit comments since `since` (newest first) and the page's English title, if it has one."""
    resp = _get(session, f"https://{lang}.wikipedia.org/w/api.php", params={
        "action": "query", "format": "json", "formatversion": 2, "redirects": 1, "titles": title,
        "prop": "revisions|langlinks", "rvprop": "timestamp|comment", "rvlimit": 25, "lllang": "en",
    })
    if resp is None:
        return [], None
    pages = resp.json().get("query", {}).get("pages", [])
    if not pages or pages[0].get("missing"):
        return [], None
    page = pages[0]
    edits = []
    for rev in page.get("revisions", []):
        ts = datetime.fromisoformat(rev["timestamp"])
        comment = (rev.get("comment") or "").strip()
        if ts >= since and comment:
            edits.append({"time": rev["timestamp"], "comment": comment[:200]})
    if lang == "en":
        en_title = page.get("title")
    else:
        links = page.get("langlinks", [])
        en_title = links[0].get("title") if links else None
    return edits, en_title


def news_query(title: str | None) -> str | None:
    """Turn a page title into a GDELT search phrase: 'Dharmendra_(actor)' -> 'Dharmendra'."""
    if not title:
        return None
    name = re.sub(r"\s*\([^)]*\)", "", title.replace("_", " ")).strip()
    name = re.sub(r"[\"“”]", "", name)
    if len(name) < 4:  # GDELT rejects very short keywords
        return None
    return f'"{name}"' if " " in name else name


def gdelt_news(session, query: str, timespan: str = "3d", max_records: int = 8) -> list[dict]:
    """Recent news headlines matching `query`. Returns [] when GDELT has nothing or misbehaves."""
    global _last_gdelt_call
    wait = GDELT_MIN_INTERVAL - (time.monotonic() - _last_gdelt_call)
    if wait > 0:
        time.sleep(wait)
    _last_gdelt_call = time.monotonic()
    try:
        resp = _get(session, GDELT_URL, params={
            "query": query, "mode": "ArtList", "format": "json", "maxrecords": max_records,
            "timespan": timespan, "sort": "HybridRel",
        }, retries=2)
        _last_gdelt_call = time.monotonic()
        if resp is None:
            return []
        articles = resp.json().get("articles", [])  # GDELT returns plain text, not JSON, for bad queries
    except (ValueError, requests.RequestException) as exc:
        print(f"    news lookup failed for {query}: {exc}")
        return []
    return [{"title": a.get("title", "").strip(), "url": a.get("url", ""), "domain": a.get("domain", ""),
             "date": a.get("seendate", "")[:8]} for a in articles if a.get("title")]
