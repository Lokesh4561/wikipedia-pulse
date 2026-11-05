# Databricks notebook source
# MAGIC %md
# MAGIC # Export: dashboard snapshot
# MAGIC Writes a small JSON snapshot of the gold tables (last 24 hours) to a volume. A GitHub Actions
# MAGIC workflow downloads it and publishes the static dashboard, so viewers never wake up compute.

# COMMAND ----------

import json
from datetime import UTC, datetime, timedelta

from pyspark.sql import Window as W
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.wiki_pulse.exports")

SILVER = "workspace.wiki_pulse.silver_pageviews"
G_LANG = "workspace.wiki_pulse.gold_language_hourly"
G_TOP = "workspace.wiki_pulse.gold_top_pages_hourly"
TREND = "workspace.wiki_pulse.gold_trending_hourly"
OUT = "/Volumes/workspace/wiki_pulse/exports/dashboard.json"

# code -> (English name, native name)
NAMES = {
    "en": ("English", "English"), "hi": ("Hindi", "हिन्दी"), "bn": ("Bengali", "বাংলা"),
    "te": ("Telugu", "తెలుగు"), "mr": ("Marathi", "मराठी"), "ta": ("Tamil", "தமிழ்"),
    "ur": ("Urdu", "اردو"), "gu": ("Gujarati", "ગુજરાતી"), "kn": ("Kannada", "ಕನ್ನಡ"),
    "ml": ("Malayalam", "മലയാളം"), "pa": ("Punjabi", "ਪੰਜਾਬੀ"), "or": ("Odia", "ଓଡ଼ିଆ"),
    "as": ("Assamese", "অসমীয়া"), "sa": ("Sanskrit", "संस्कृतम्"),
}
LANGS = list(NAMES)
TOP_N = 10


def iso(ts):
    """Spark returns naive UTC datetimes here (session time zone is UTC)."""
    return ts.replace(tzinfo=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

# COMMAND ----------

gold = spark.table(G_LANG).where(F.col("language").isin(LANGS))
latest = gold.agg(F.max("hour_utc")).first()[0]
start = latest - timedelta(hours=23)
hours = [start + timedelta(hours=i) for i in range(24)]

# Views per language and hour, last 24 hours
series = {code: [None] * 24 for code in LANGS}
mobile = {code: [0, 0] for code in LANGS}
for r in gold.where(F.col("hour_utc") >= start).collect():
    i = int((r.hour_utc - start).total_seconds() // 3600)
    series[r.language][i] = r.total_views
    mobile[r.language][0] += r.mobile_views
    mobile[r.language][1] += r.total_views

languages = []
for code in LANGS:
    total = sum(v for v in series[code] if v)
    if not total:
        continue
    languages.append({
        "code": code, "name": NAMES[code][0], "native": NAMES[code][1],
        "views_24h": total, "mobile_share": round(mobile[code][0] / mobile[code][1], 3),
        "series": series[code],
    })
languages.sort(key=lambda x: -x["views_24h"])

# Trending over the last 6 hours: each page once (its strongest hour), best ratios first
recent = spark.table(TREND).where((F.col("hour_utc") > latest - timedelta(hours=6)) & F.col("language").isin(LANGS))
best = W.partitionBy("language", "page_title").orderBy(F.desc("spike_ratio"), F.desc("views"))
order = W.partitionBy("language").orderBy(F.desc("spike_ratio"), F.desc("views"))
trend_rows = (recent.withColumn("n", F.row_number().over(best)).where("n = 1")
              .withColumn("pos", F.row_number().over(order)).where(f"pos <= {TOP_N}")
              .orderBy("language", "pos").collect())
trending = {code: [] for code in LANGS}
for r in trend_rows:
    trending[r.language].append({"page": r.page_title, "views": r.views, "baseline": r.baseline_avg,
                                 "ratio": r.spike_ratio, "hour": iso(r.hour_utc)})

# Most read in the latest hour
top = {code: [] for code in LANGS}
for r in (spark.table(G_TOP).where((F.col("hour_utc") == latest) & (F.col("rank") <= TOP_N)
                                   & F.col("language").isin(LANGS)).orderBy("language", "rank").collect()):
    top[r.language].append({"page": r.page_title, "views": r.views})

stats = {
    "rows_24h": spark.table(SILVER).where(F.col("hour_utc") >= start).count(),
    "hours_total": spark.table(G_LANG).select("hour_utc").distinct().count(),
    "first_hour": iso(spark.table(G_LANG).agg(F.min("hour_utc")).first()[0]),
}

snapshot = {
    "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "latest_hour": iso(latest),
    "hours": [iso(h) for h in hours],
    "languages": languages,
    "trending": trending,
    "top": top,
    "stats": stats,
}
with open(OUT, "w", encoding="utf-8") as f:
    json.dump(snapshot, f, ensure_ascii=False, separators=(",", ":"))

print(f"Wrote {OUT}: latest hour {snapshot['latest_hour']}, {stats['rows_24h']:,} rows in the last 24 hours, "
      f"{sum(len(v) for v in trending.values())} trending pages")
