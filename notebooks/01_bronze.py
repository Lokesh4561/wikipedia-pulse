# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: raw pageview lines
# MAGIC Auto Loader picks up only **new** hourly files from the landing volume (exactly-once,
# MAGIC tracked in the checkpoint) and stores each raw line with its source file and hour.

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql("CREATE VOLUME IF NOT EXISTS workspace.wiki_pulse.checkpoints")

LANDING = "/Volumes/workspace/wiki_pulse/landing/pageviews"
CHECKPOINT = "/Volumes/workspace/wiki_pulse/checkpoints/bronze_pageviews"
TABLE = "workspace.wiki_pulse.bronze_pageviews"

stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "text")
    .option("pathGlobFilter", "*.gz")
    .load(LANDING)
    .select(
        F.col("value").alias("raw_line"),
        F.col("_metadata.file_name").alias("source_file"),
        F.to_timestamp(
            F.regexp_extract("_metadata.file_name", r"pageviews-(\d{8}-\d{2})", 1), "yyyyMMdd-HH"
        ).alias("hour_utc"),
        F.current_timestamp().alias("ingested_at"),
    )
)

query = (
    stream.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)  # process everything new, then stop
    .toTable(TABLE)
)
query.awaitTermination()

# COMMAND ----------

display(spark.sql(f"""
    SELECT source_file, hour_utc, COUNT(*) AS lines
    FROM {TABLE}
    GROUP BY ALL
    ORDER BY hour_utc DESC
    LIMIT 24
"""))
