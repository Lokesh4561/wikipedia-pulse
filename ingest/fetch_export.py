"""Download the dashboard export written by the Databricks `export` task.

Usage:
    python -m ingest.fetch_export --out dashboard/data.json
Environment:
    DATABRICKS_HOST, DATABRICKS_TOKEN
"""

import argparse
import json
import os
import sys
from datetime import UTC, datetime, timedelta

from ingest.fetch_pageviews import Volume

EXPORT_PATH = "/Volumes/workspace/wiki_pulse/exports/dashboard.json"
MAX_AGE_HOURS = 6


def check_export(data: dict, now: datetime) -> list[str]:
    """Return problems with an export; an empty list means it is safe to publish."""
    problems = []
    for key in ("generated_at", "latest_hour", "languages", "trending", "top", "stats"):
        if key not in data:
            problems.append(f"missing key: {key}")
    if not data.get("languages"):
        problems.append("no languages")
    generated = data.get("generated_at")
    if generated:
        age = now - datetime.fromisoformat(generated)  # Python 3.11+ accepts a trailing Z
        if age > timedelta(hours=MAX_AGE_HOURS):
            problems.append(f"export is stale: {age} old")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", default="dashboard/data.json")
    args = parser.parse_args()

    host, token = os.getenv("DATABRICKS_HOST", "").strip(), os.getenv("DATABRICKS_TOKEN", "").strip()
    if not host or not token:
        print("DATABRICKS_HOST and DATABRICKS_TOKEN must be set.")
        return 1

    raw = Volume(host, token).download(EXPORT_PATH)
    data = json.loads(raw)
    problems = check_export(data, datetime.now(UTC))
    if problems:
        # Still publish a stale export (better than nothing), but surface it in the run log
        print("Warning:", "; ".join(problems))
    with open(args.out, "wb") as f:
        f.write(raw)
    print(f"Saved {len(raw) / 1024:.1f} KB to {args.out}, latest hour {data.get('latest_hour')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
