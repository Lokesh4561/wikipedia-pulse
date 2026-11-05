"""Fetch Wikimedia hourly pageview files and land them in a Databricks Unity Catalog volume.

Databricks Free Edition can only reach a small set of trusted domains, so ingestion runs
outside Databricks (GitHub Actions) and pushes raw files into the landing volume through
the Databricks Files API. Auto Loader picks them up from there.

Idempotent: files already in the volume are skipped, and hours not yet published (404) are
skipped and retried on the next run.

Usage:
    python -m ingest.fetch_pageviews --hours 48
Environment:
    DATABRICKS_HOST   e.g. https://dbc-xxxx.cloud.databricks.com
    DATABRICKS_TOKEN  personal access token
"""

import argparse
import os
import sys
import time
from datetime import UTC, datetime, timedelta
from urllib.parse import quote

import requests

SOURCE_BASE = "https://dumps.wikimedia.org/other/pageviews"
VOLUME_DIR = "/Volumes/workspace/wiki_pulse/landing/pageviews"
USER_AGENT = "wikipedia-pulse/1.0 (https://github.com/Lokesh4561/wikipedia-pulse)"
PUBLISH_DELAY_HOURS = 1  # Wikimedia publishes each hourly file roughly an hour after it ends


def file_name(hour: datetime) -> str:
    """Wikimedia file name for an hour, e.g. pageviews-20261103-140000.gz"""
    return f"pageviews-{hour:%Y%m%d-%H}0000.gz"


def source_url(hour: datetime) -> str:
    return f"{SOURCE_BASE}/{hour:%Y}/{hour:%Y-%m}/{file_name(hour)}"


def volume_path(hour: datetime) -> str:
    """Partitioned by date so listing stays fast as data grows."""
    return f"{VOLUME_DIR}/date={hour:%Y-%m-%d}/{file_name(hour)}"


def hours_to_fetch(now: datetime, hours: int) -> list[datetime]:
    """The most recent `hours` complete hours that should already be published, oldest first."""
    latest = now.astimezone(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(
        hours=PUBLISH_DELAY_HOURS + 1
    )
    return [latest - timedelta(hours=i) for i in reversed(range(hours))]


class Volume:
    """Minimal client for the Databricks Files API."""

    def __init__(self, host: str, token: str):
        self.base = host.rstrip("/") + "/api/2.0/fs"
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {token}"

    def exists(self, path: str) -> bool:
        resp = self.session.head(f"{self.base}/files{quote(path)}", timeout=30)
        if resp.status_code == 404:
            return False
        resp.raise_for_status()
        return True

    def upload(self, path: str, data: bytes) -> None:
        parent = path.rsplit("/", 1)[0]
        self.session.put(f"{self.base}/directories{quote(parent)}", timeout=30).raise_for_status()
        resp = self.session.put(
            f"{self.base}/files{quote(path)}",
            params={"overwrite": "true"},
            data=data,
            headers={"Content-Type": "application/octet-stream"},
            timeout=600,
        )
        resp.raise_for_status()

    def download(self, path: str) -> bytes:
        resp = self.session.get(f"{self.base}/files{quote(path)}", timeout=120)
        resp.raise_for_status()
        return resp.content


def download(url: str, retries: int = 4) -> bytes | None:
    """Download a file; None if it is not published yet. Retries on network errors, 429 and 5xx."""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=300)
            if resp.status_code == 404:
                return None
            if resp.status_code == 429 or resp.status_code >= 500:
                raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
            resp.raise_for_status()
            return resp.content
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError):
            if attempt == retries:
                raise
            wait = 10 * 2 ** (attempt - 1)
            print(f"    download failed (attempt {attempt}/{retries}), retrying in {wait}s")
            time.sleep(wait)
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--hours", type=int, default=48, help="how many recent hours to check (self-heals gaps)")
    args = parser.parse_args()

    host, token = os.getenv("DATABRICKS_HOST", "").strip(), os.getenv("DATABRICKS_TOKEN", "").strip()
    if not host or not token:
        print("DATABRICKS_HOST and DATABRICKS_TOKEN must be set.")
        return 1

    volume = Volume(host, token)
    counts = {"uploaded": 0, "skipped": 0, "not_published": 0}
    total_mb = 0.0

    for hour in hours_to_fetch(datetime.now(UTC), args.hours):
        target = volume_path(hour)
        if volume.exists(target):
            counts["skipped"] += 1
            print(f"  skip      {file_name(hour)} (already landed)")
            continue
        data = download(source_url(hour))
        if data is None:
            counts["not_published"] += 1
            print(f"  waiting   {file_name(hour)} (not published yet)")
            continue
        volume.upload(target, data)
        size_mb = len(data) / 1_048_576
        total_mb += size_mb
        counts["uploaded"] += 1
        print(f"  uploaded  {file_name(hour)}  {size_mb:6.1f} MB")

    print(f"Done: {counts}, {total_mb:.1f} MB landed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
