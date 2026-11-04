# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: language trends and top pages
# MAGIC - `gold_language_hourly`: views per language and hour, mobile vs desktop, Indian languages flagged
# MAGIC - `gold_top_pages_hourly`: top 25 articles per hour for English and 13 Indian languages
# MAGIC
# MAGIC Incremental: only hours that received new silver rows since the last build are rebuilt
# MAGIC (`replaceWhere`), so cost stays flat as history grows.

# COMMAND ----------

from pyspark.sql import Window as W
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

SILVER = "workspace.wiki_pulse.silver_pageviews"
G_LANG = "workspace.wiki_pulse.gold_language_hourly"
G_TOP = "workspace.wiki_pulse.gold_top_pages_hourly"

INDIAN = {"hi": "Hindi", "bn": "Bengali", "te": "Telugu", "mr": "Marathi", "ta": "Tamil",
          "ur": "Urdu", "gu": "Gujarati", "kn": "Kannada", "ml": "Malayalam", "pa": "Punjabi",
          "or": "Odia", "as": "Assamese", "sa": "Sanskrit"}
NAMES = {"en": "English", **INDIAN}
TOP_LANGS = list(NAMES)
MAIN_PAGES = ["Main_Page", "-", "मुखपृष्ठ", "మొదటి_పేజీ", "முதற்_பக்கம்", "ಮುಖ್ಯ_ಪುಟ",
              "പ്രധാന_താൾ", "প্রধান_পাতা", "મુખપૃષ્ઠ"]
NAMESPACE = r"^[^_:]+:[^_]"  # Special:Search, File:x.jpg, Category:... (not "Mission:_Impossible")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {G_LANG} (
    hour_utc TIMESTAMP, language STRING, language_name STRING, is_indian BOOLEAN,
    desktop_views BIGINT, mobile_views BIGINT, total_views BIGINT, mobile_share DOUBLE,
    built_at TIMESTAMP) CLUSTER BY (hour_utc)""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {G_TOP} (
    hour_utc TIMESTAMP, language STRING, rank INT, page_title STRING, views BIGINT,
    built_at TIMESTAMP) CLUSTER BY (hour_utc, language)""")

# COMMAND ----------

last_built = spark.sql(f"SELECT coalesce(max(built_at), timestamp'1970-01-01') FROM {G_LANG}").first()[0]
affected = [r.hour_utc for r in spark.table(SILVER)
            .where(F.col("ingested_at") > F.lit(last_built))
            .select("hour_utc").distinct().collect()]
print(f"Hours to rebuild: {sorted(affected)}")

if affected:
    hours_sql = ", ".join(f"timestamp'{h:%Y-%m-%d %H:%M:%S}'" for h in affected)
    replace = f"hour_utc IN ({hours_sql})"
    now = F.current_timestamp()
    wiki = spark.table(SILVER).where(F.col("hour_utc").isin(affected) & (F.col("project") == "wikipedia"))

    # Top pages per language and hour
    articles = (wiki
        .where(F.col("language").isin(TOP_LANGS)
               & ~F.col("page_title").isin(MAIN_PAGES)
               & ~F.col("page_title").rlike(NAMESPACE))
        .groupBy("hour_utc", "language", "page_title")
        .agg(F.sum("views").cast("bigint").alias("views")))
    rank_window = W.partitionBy("hour_utc", "language").orderBy(F.desc("views"), "page_title")
    top = (articles.withColumn("rank", F.row_number().over(rank_window))
           .where("rank <= 25")
           .select("hour_utc", "language", "rank", "page_title", "views", now.alias("built_at")))
    top.write.mode("overwrite").option("replaceWhere", replace).saveAsTable(G_TOP)

    # Views per language and hour (written last: it holds the build watermark)
    name_map = F.create_map(*[F.lit(x) for kv in NAMES.items() for x in kv])
    lang = (wiki.groupBy("hour_utc", "language")
        .agg(F.sum(F.when(~F.col("is_mobile"), F.col("views")).otherwise(0)).cast("bigint").alias("desktop_views"),
             F.sum(F.when(F.col("is_mobile"), F.col("views")).otherwise(0)).cast("bigint").alias("mobile_views"))
        .withColumn("total_views", F.col("desktop_views") + F.col("mobile_views"))
        .withColumn("mobile_share", F.round(F.col("mobile_views") / F.col("total_views"), 3))
        .withColumn("language_name", F.coalesce(F.element_at(name_map, F.col("language")), F.col("language")))
        .withColumn("is_indian", F.col("language").isin(list(INDIAN)))
        .select("hour_utc", "language", "language_name", "is_indian", "desktop_views",
                "mobile_views", "total_views", "mobile_share", now.alias("built_at")))
    lang.write.mode("overwrite").option("replaceWhere", replace).saveAsTable(G_LANG)

# COMMAND ----------

display(spark.sql(f"""
    SELECT language_name, SUM(total_views) AS views, ROUND(AVG(mobile_share) * 100) AS mobile_pct
    FROM {G_LANG} WHERE is_indian OR language = 'en'
    GROUP BY ALL ORDER BY views DESC
"""))
display(spark.sql(f"""
    SELECT language, rank, replace(page_title, '_', ' ') AS page, views
    FROM {G_TOP}
    WHERE hour_utc = (SELECT max(hour_utc) FROM {G_TOP}) AND language IN ('en', 'hi', 'te') AND rank <= 5
    ORDER BY language, rank
"""))
