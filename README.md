# Wikipedia Pulse

A lakehouse on **Databricks** that tracks what India and the world read on Wikipedia, hour by hour,
built on Wikimedia's public hourly pageview files (billions of rows per month).

> Work in progress: building step by step.

## Architecture

```
Wikimedia hourly dumps ──► GitHub Actions (hourly) ──► Unity Catalog volume (landing)
                                                             │  Auto Loader
                                                             ▼
                                 bronze (raw lines) ──► silver (parsed, typed) ──► gold (trends, spikes)
```

**Why ingestion runs outside Databricks:** Databricks Free Edition only allows outbound traffic to a
small set of trusted domains, so a GitHub Actions job downloads each hourly file and pushes it into
the landing volume through the Databricks Files API. This is the same "external landing zone" pattern
used when a platform cannot reach a source directly.

## Ingestion

- `ingest/fetch_pageviews.py`: checks the most recent N hours, skips files already landed
  (idempotent), skips hours Wikimedia has not published yet, and retries with backoff on errors.
- Files land in `/Volumes/workspace/wiki_pulse/landing/pageviews/date=YYYY-MM-DD/`.
- `.github/workflows/ingest.yml` runs every hour; run it manually with a larger `hours` value to backfill.

## Run locally

```bash
pip install -r requirements-dev.txt
pytest -q
export DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=<personal access token>
python -m ingest.fetch_pageviews --hours 3
```

## Data source

[Wikimedia pageview dumps](https://dumps.wikimedia.org/other/pageviews/), licensed CC0.
