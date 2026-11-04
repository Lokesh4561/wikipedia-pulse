# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: parsed, typed pageviews
# MAGIC Splits each raw line (`en.m Virat_Kohli 1520 0`) into language, project, device, page and views.
# MAGIC Malformed lines go to a rejects table instead of failing the job. Writes are idempotent
# MAGIC (`txnAppId` / `txnVersion`), so a retried micro-batch is never counted twice.

# COMMAND ----------

from pyspark.sql import functions as F

BRONZE = "workspace.wiki_pulse.bronze_pageviews"
SILVER = "workspace.wiki_pulse.silver_pageviews"
REJECTS = "workspace.wiki_pulse.silver_rejects"
CHECKPOINT = "/Volumes/workspace/wiki_pulse/checkpoints/silver_pageviews"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER} (
        hour_utc TIMESTAMP, language STRING, project STRING, is_mobile BOOLEAN,
        page_title STRING, views INT, source_file STRING, ingested_at TIMESTAMP
    ) CLUSTER BY (hour_utc, language)
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {REJECTS} (
        raw_line STRING, source_file STRING, reason STRING, ingested_at TIMESTAMP
    )
""")

# Wikimedia domain-code suffixes -> project name (no suffix = Wikipedia)
PROJECTS = {"b": "wikibooks", "d": "wiktionary", "f": "wikimediafoundation", "n": "wikinews",
            "q": "wikiquote", "s": "wikisource", "v": "wikiversity", "voy": "wikivoyage",
            "w": "mediawiki", "wd": "wikidata", "z": "wikipedia"}
project_map = F.create_map(*[F.lit(x) for kv in PROJECTS.items() for x in kv])

# COMMAND ----------


def parse(df):
    parts = F.split("raw_line", " ")
    domain_parts = F.split(F.try_element_at(parts, F.lit(1)), r"\.")
    suffixes = F.slice(domain_parts, 2, 10)
    project_code = F.try_element_at(F.filter(suffixes, lambda s: s != "m"), F.lit(1))
    return df.select(
        "raw_line", "source_file", "hour_utc", "ingested_at",
        F.size(parts).alias("n_fields"),
        F.lower(F.try_element_at(domain_parts, F.lit(1))).alias("language"),
        F.coalesce(F.element_at(project_map, F.coalesce(project_code, F.lit("z"))),
                   F.lit("other")).alias("project"),
        F.array_contains(suffixes, "m").alias("is_mobile"),
        F.try_element_at(parts, F.lit(2)).alias("page_title"),
        F.expr("try_cast(try_element_at(split(raw_line, ' '), 3) AS INT)").alias("views"),
    )


def write_batch(batch_df, batch_id):
    parsed = parse(batch_df)
    valid = (F.col("n_fields") == 4) & F.col("views").isNotNull() & F.col("page_title").isNotNull()

    (parsed.filter(valid)
        .select("hour_utc", "language", "project", "is_mobile", "page_title", "views",
                "source_file", "ingested_at")
        .write.mode("append")
        .option("txnAppId", "silver_pageviews").option("txnVersion", batch_id)  # idempotent retries
        .saveAsTable(SILVER))

    (parsed.filter(~valid)
        .select("raw_line", "source_file",
                F.when(F.col("n_fields") != 4, "wrong_field_count").otherwise("bad_views").alias("reason"),
                "ingested_at")
        .write.mode("append")
        .option("txnAppId", "silver_rejects").option("txnVersion", batch_id)
        .saveAsTable(REJECTS))


(spark.readStream.table(BRONZE)
    .writeStream
    .foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
    .awaitTermination())

# COMMAND ----------

display(spark.sql(f"""
    SELECT project, is_mobile, COUNT(*) AS pages, SUM(views) AS views
    FROM {SILVER} GROUP BY ALL ORDER BY views DESC LIMIT 10
"""))
display(spark.sql(f"SELECT reason, COUNT(*) AS rows FROM {REJECTS} GROUP BY ALL"))
