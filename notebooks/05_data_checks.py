# Databricks notebook source
# MAGIC %md
# MAGIC # Data checks: is every hour present at every layer?
# MAGIC Lists the files in the landing volume and compares row counts per hour across bronze, silver and gold.
# MAGIC A missing hour in all layers points to ingestion; a drop between layers points to that layer.

# COMMAND ----------

import re

landed = sorted(f.name for d in dbutils.fs.ls("/Volumes/workspace/wiki_pulse/landing/pageviews/")
                for f in dbutils.fs.ls(d.path))
print(len(landed), "files landed:", [re.search(r"-(\d{8}-\d{2})", n).group(1) for n in landed])

display(spark.sql("""
    SELECT b.hour_utc, b.bronze_rows, s.silver_rows, g.gold_views
    FROM (SELECT hour_utc, count(*) AS bronze_rows FROM workspace.wiki_pulse.bronze_pageviews GROUP BY 1) b
    LEFT JOIN (SELECT hour_utc, count(*) AS silver_rows FROM workspace.wiki_pulse.silver_pageviews GROUP BY 1) s
        USING (hour_utc)
    LEFT JOIN (SELECT hour_utc, sum(total_views) AS gold_views FROM workspace.wiki_pulse.gold_language_hourly GROUP BY 1) g
        USING (hour_utc)
    ORDER BY 1
"""))
