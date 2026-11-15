"""Evidence collectors for "why is this page trending?".

Three free, key-less sources:
  - Wikipedia page summary (what the page is about)
  - recent edit comments on the page (editors often record the news: "died on ...", "won ...")
  - news headlines from Google News RSS (English and Indian-language editions)
"""

import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime
from urllib.parse import quote

import requests

USER_AGENT = "wikipedia-pulse/1.0 (https://github.com/Lokesh4561/wikipedia-pulse)"


def new_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


def _get(session: requests.Session, url: str, params: dict | None = None, retries: int = 2,
         timeout: int = 15):
    """GET with retries on network errors, 429 and 5xx. Returns None for 404."""
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code == 404:
                return None
            if resp.status_code == 429 or resp.status_code >= 500:
                raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
            resp.raise_for_status()
            return resp
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError):
            if attempt == retries:
                raise
            time.sleep(3)
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
    """Turn a page title into a news search phrase: 'Dharmendra_(actor)' -> 'Dharmendra'."""
    if not title:
        return None
    name = re.sub(r"\s*\([^)]*\)", "", title.replace("_", " ")).strip()
    name = re.sub(r"[\"“”]", "", name)
    if len(name) < 3:
        return None
    return f'"{name}"' if " " in name else name


GOOGLE_NEWS_URL = "https://news.google.com/rss/search"
# Google News editions for India; languages without one fall back to English news
NEWS_EDITIONS = {"en": "en", "hi": "hi", "bn": "bn", "te": "te", "mr": "mr", "ta": "ta", "ml": "ml",
                 "gu": "gu", "kn": "kn", "pa": "pa", "ur": "ur"}


def parse_rss(xml_text: str, limit: int = 8) -> list[dict]:
    """Headlines from a Google News RSS feed (pure function, unit-tested)."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    items = []
    for item in root.iter("item"):
        title = (item.findtext("title") or "").strip()
        source = item.find("source")
        publisher = source.text.strip() if source is not None and source.text else ""
        if publisher and title.endswith(" - " + publisher):  # Google appends " - Publisher" to titles
            title = title[: -len(publisher) - 3]
        try:
            date = parsedate_to_datetime(item.findtext("pubDate") or "").strftime("%Y%m%d")
        except (TypeError, ValueError):
            date = ""
        if title:
            items.append({"title": title, "url": (item.findtext("link") or "").strip(),
                          "domain": publisher, "date": date})
        if len(items) >= limit:
            break
    return items


def news_headlines(session, query: str, lang: str = "en", days: int = 3, limit: int = 8) -> list[dict]:
    """Recent news headlines from Google News (free, no key). Returns [] on any problem."""
    edition = NEWS_EDITIONS.get(lang, "en")
    try:
        resp = _get(session, GOOGLE_NEWS_URL, params={
            "q": f"{query} when:{days}d", "hl": f"{edition}-IN" if edition == "en" else edition,
            "gl": "IN", "ceid": f"IN:{edition}",
        }, retries=1, timeout=15)
    except requests.RequestException as exc:
        print(f"    news lookup failed for {query}: {exc}")
        return []
    return parse_rss(resp.text, limit) if resp is not None else []
