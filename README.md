# Wikipedia Pulse

A lakehouse on **Databricks** that tracks what India and the world read on Wikipedia, hour by hour.
It processes Wikimedia's public hourly pageview files: **~4.4 million rows every hour, 100+ million a day**.

## Architecture

```
Wikimedia hourly dumps
        │  GitHub Actions, hourly (:37)
        ▼
Unity Catalog volume  /Volumes/workspace/wiki_pulse/landing/pageviews/date=YYYY-MM-DD/
        │  Databricks Workflow "wiki_pulse_hourly" (serverless, hourly)
        ▼
 bronze_pageviews ──► silver_pageviews ──► gold_language_hourly
 (raw lines,          (parsed + typed,       gold_top_pages_hourly
  Auto Loader)         rejects table)        (incremental rebuild)
```

| Layer | Table | What it holds |
|---|---|---|
| Bronze | `bronze_pageviews` | Every raw line with its source file and hour. Auto Loader, exactly-once via checkpoint. |
| Silver | `silver_pageviews` | Language, project, mobile/desktop, page, views. Liquid clustered on `(hour_utc, language)`. |
| Silver | `silver_rejects` | Malformed lines with a reason, instead of failing the job. |
| Gold | `gold_language_hourly` | Views per language per hour, mobile share, Indian languages flagged. |
| Gold | `gold_top_pages_hourly` | Top 25 articles per hour for English and 13 Indian languages. |

## Engineering highlights

- **External landing zone:** Databricks Free Edition only allows outbound traffic to a few trusted
  domains, so a GitHub Actions job downloads each hourly file and pushes it into the volume through
  the Databricks Files API (idempotent: skips landed files, waits for unpublished hours, retries with backoff).
- **Incremental at every layer:** Auto Loader in bronze, Delta streaming with `availableNow` in silver,
  and gold rebuilds only the hours that received new data (`replaceWhere`), so cost stays flat as history grows.
- **Idempotent writes:** silver uses Delta `txnAppId`/`txnVersion` in `foreachBatch`, so a retried
  micro-batch is never double counted.
- **Data quality:** malformed lines are quarantined in `silver_rejects`; non-article pages
  (main pages, `Special:`, `File:`, `Category:`) are excluded from rankings.
- **Orchestration:** a Databricks Workflow runs bronze → silver → gold every hour on serverless compute.
- **CI:** ruff + pytest on every push.

## First findings

- About **70% of Wikipedia views come from mobile** (18M mobile vs 8M desktop over two hours).
- Mobile share varies a lot between Indian languages: Kannada (80%) and Hindi (79%) readers are mostly
  on phones, while Gujarati (28%) and Urdu (34%) readers are mostly on desktop.

## Repository layout

```
ingest/fetch_pageviews.py      # Wikimedia -> Databricks volume (runs in GitHub Actions)
notebooks/00_setup.sql         # schema and volumes
notebooks/01_bronze.py         # Auto Loader -> bronze
notebooks/02_silver.py         # parse, validate, quarantine -> silver
notebooks/03_gold.py           # incremental gold tables
.github/workflows/ingest.yml   # hourly ingestion
.github/workflows/ci.yml       # lint + tests
```

## Run locally

```bash
pip install -r requirements-dev.txt
pytest -q
export DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=<personal access token>
python -m ingest.fetch_pageviews --hours 3
```

## Roadmap

- Spike detection: pages trending far above their 24-hour baseline
- Live dashboard
- Real-time stream of Wikipedia edits with Spark Structured Streaming
- Deploy the job with Databricks Asset Bundles

## Data source

[Wikimedia pageview dumps](https://dumps.wikimedia.org/other/pageviews/), released under CC0.
