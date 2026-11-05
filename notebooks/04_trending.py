# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: trending pages (spike detection)
# MAGIC A page is **trending** in an hour when its views are at least 5x its average over the previous
# MAGIC 24 hours, with a minimum volume so tiny pages don't qualify by chance.
# MAGIC
# MAGIC - Candidates first: only big-enough pages get their 24-hour history looked up, so the job stays cheap.
# MAGIC - Incremental via a watermark table. When an hour arrives late (a filled gap), the 24 hours after it
# MAGIC   are re-scored too, because their baselines changed.

# COMMAND ----------

from datetime import UTC, datetime, timedelta

from pyspark.sql import Window as W
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

SILVER = "workspace.wiki_pulse.silver_pageviews"
G_LANG = "workspace.wiki_pulse.gold_language_hourly"
TREND = "workspace.wiki_pulse.gold_trending_hourly"
WATERMARKS = "workspace.wiki_pulse.pipeline_watermarks"

INDIAN = ["hi", "bn", "te", "mr", "ta", "ur", "gu", "kn", "ml", "pa", "or", "as", "sa"]
TOP_LANGS = ["en"] + INDIAN
MAIN_PAGES = ["Main_Page", "-", "मुखपृष्ठ", "మొదటి_పేజీ", "முதற்_பக்கம்", "ಮುಖ್ಯ_ಪುಟ",
              "പ്രധാന_താൾ", "প্রধান_পাতা", "મુખપૃષ્ઠ"]
NAMESPACE = r"^[^_:]+:[^_]"

MIN_VIEWS_EN, MIN_VIEWS_OTHER = 1000, 50  # ignore tiny pages
MIN_RATIO = 5.0                            # views vs 24h average
MIN_HISTORY_HOURS = 18                     # need most of the previous day to judge a spike

spark.sql(f"""CREATE TABLE IF NOT EXISTS {TREND} (
    hour_utc TIMESTAMP, language STRING, rank INT, page_title STRING, views BIGINT,
    baseline_avg DOUBLE, spike_ratio DOUBLE, built_at TIMESTAMP) CLUSTER BY (hour_utc, language)""")
spark.sql(f"CREATE TABLE IF NOT EXISTS {WATERMARKS} (job STRING, last_built TIMESTAMP)")

# COMMAND ----------

# Hours (re)built in gold since this job last ran, plus the 24 hours after each (their baselines changed)
last = spark.sql(f"SELECT max(last_built) FROM {WATERMARKS} WHERE job = 'trending'").first()[0] or datetime(1970, 1, 1, tzinfo=UTC)
new_gold = spark.table(G_LANG).where(F.col("built_at") > F.lit(last))
new_watermark = new_gold.agg(F.max("built_at")).first()[0]
new_hours = new_gold.select(F.col("hour_utc").alias("nh")).distinct()
affected = [r.hour_utc for r in spark.table(G_LANG).select("hour_utc").distinct()
            .join(new_hours, (F.col("hour_utc") >= F.col("nh"))
                  & (F.col("hour_utc") <= F.col("nh") + F.expr("INTERVAL 24 HOURS")), "left_semi")
            .collect()]
print(f"Hours to score: {len(affected)}")

if affected:
    lo, hi = min(affected) - timedelta(hours=24), max(affected)

    # Article views per page and hour, for the window we need
    page_hour = (spark.table(SILVER)
        .where(F.col("hour_utc").between(lo, hi) & (F.col("project") == "wikipedia")
               & F.col("language").isin(TOP_LANGS)
               & ~F.col("page_title").isin(MAIN_PAGES) & ~F.col("page_title").rlike(NAMESPACE))
        .groupBy("hour_utc", "language", "page_title")
        .agg(F.sum("views").cast("bigint").alias("views")))

    # 1) Candidates: big enough pages in the hours we are scoring
    min_views = F.when(F.col("language") == "en", MIN_VIEWS_EN).otherwise(MIN_VIEWS_OTHER)
    cand = page_hour.where(F.col("hour_utc").isin(affected) & (F.col("views") >= min_views))

    # 2) Their history over the previous 24 hours (only for candidate pages)
    hist = page_hour.join(cand.select("language", "page_title").distinct(), ["language", "page_title"], "left_semi")
    c, h = cand.alias("c"), hist.alias("h")
    in_window = ((F.col("h.language") == F.col("c.language")) & (F.col("h.page_title") == F.col("c.page_title"))
                 & (F.col("h.hour_utc") >= F.col("c.hour_utc") - F.expr("INTERVAL 24 HOURS"))
                 & (F.col("h.hour_utc") < F.col("c.hour_utc")))
    scored = (c.join(h, in_window, "left")
        .groupBy("c.hour_utc", "c.language", "c.page_title", "c.views")
        .agg((F.coalesce(F.sum("h.views"), F.lit(0)) / 24).alias("baseline_avg")))

    # 3) How many of the previous 24 hours exist for that language (skip if too little history)
    lang_hours = spark.table(G_LANG).select(F.col("hour_utc").alias("lh"), F.col("language").alias("ll"))
    keys = cand.select("hour_utc", "language").distinct()
    history = (keys.join(lang_hours, (F.col("ll") == F.col("language"))
                         & (F.col("lh") >= F.col("hour_utc") - F.expr("INTERVAL 24 HOURS"))
                         & (F.col("lh") < F.col("hour_utc")), "left")
        .groupBy("hour_utc", "language").agg(F.count("lh").alias("history_hours")))

    rank_window = W.partitionBy("hour_utc", "language").orderBy(F.desc("spike_ratio"), F.desc("views"))
    trending = (scored.join(history, ["hour_utc", "language"])
        .where(F.col("history_hours") >= MIN_HISTORY_HOURS)
        .withColumn("spike_ratio", F.round(F.col("views") / (F.col("baseline_avg") + 1), 1))
        .where(F.col("spike_ratio") >= MIN_RATIO)
        .withColumn("rank", F.row_number().over(rank_window))
        .where("rank <= 20")
        .select("hour_utc", "language", "rank", "page_title", "views",
                F.round("baseline_avg", 1).alias("baseline_avg"), "spike_ratio",
                F.current_timestamp().alias("built_at")))

    # Replacing all affected hours also clears stale rows for hours that no longer qualify
    hours_sql = ", ".join(f"timestamp'{x:%Y-%m-%d %H:%M:%S}'" for x in affected)
    trending.write.mode("overwrite").option("replaceWhere", f"hour_utc IN ({hours_sql})").saveAsTable(TREND)

    spark.sql(f"""MERGE INTO {WATERMARKS} t
                  USING (SELECT 'trending' AS job, timestamp'{new_watermark}' AS last_built) s
                  ON t.job = s.job WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")

# COMMAND ----------

display(spark.sql(f"""
    SELECT min(hour_utc) AS first_hour, max(hour_utc) AS last_hour, count(DISTINCT hour_utc) AS hours
    FROM {G_LANG}
"""))
display(spark.sql(f"""
    SELECT hour_utc, language, rank, replace(page_title, '_', ' ') AS page, views, baseline_avg, spike_ratio
    FROM {TREND}
    WHERE hour_utc >= (SELECT max(hour_utc) FROM {TREND}) - INTERVAL 6 HOURS
      AND language IN ('en', 'hi', 'te', 'ta') AND rank <= 3
    ORDER BY hour_utc DESC, language, rank
"""))
