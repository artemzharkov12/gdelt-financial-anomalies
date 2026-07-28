"""
stream_yt_stats_to_bronze.py
----------------------------
Streaming ingestion pipeline: reads YouTube-statistics JSON snapshots from a
Unity Catalog Volume via Auto Loader and appends enriched records to the
bronze Delta table.

Execution context: run via ``%run`` or as a Databricks job task on a cluster
with an active SparkSession.

Side effects:
    - Starts a Structured Streaming query (trigger=availableNow) that appends
      rows to ``bronze_music_stats_table``.
    - Checkpoint state is persisted at ``checkpoint_path``.
    - Schema inference state is persisted at ``schema_path``.
"""

import sys

sys.path.insert(0, "../../../00_setup/pawel_project")


from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from music_pipeline_setup import (
    bronze_music_stats_table,
    catalog_name,
    json_landing_path,
    music_schema,
    volume_name,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
checkpoint_path: str = (
    f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/_checkpoints/yt_stats_bronze"
)
schema_path: str = (
    f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/_schema/yt_stats_bronze"
)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
# Explicit fixed schema — prevents Auto Loader from re-inferring on each run
# and guarantees stable column types across file batches.
fixed_schema: StructType = StructType([
    StructField("_ingested_at", StringType(), True),
    StructField("video_id", StringType(), True),
    StructField("album", StringType(), True),
    StructField("published_at", TimestampType(), True),
    StructField("video_title", StringType(), True),
    StructField("view_count",    StringType(), True),   # cast to LongType below
    StructField("like_count",     StringType(), True),   # cast to LongType below
    StructField("comment_count",  StringType(), True),   # cast to IntegerType below
    StructField("author", StringType(), True),
    StructField("song_title", StringType(), True)
])


# ---------------------------------------------------------------------------
# Streaming read — Auto Loader
# ---------------------------------------------------------------------------
df_stream: DataFrame = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.schemaEvolutionMode", "rescue")  # rescue unexpected columns for future schema evolution
    .option("multiline", "true")
    .schema(fixed_schema)
    .load(json_landing_path)
)

# ---------------------------------------------------------------------------
# Enrichment — metadata columns and type casts
# ---------------------------------------------------------------------------
# Source file path surfaced from Spark's hidden _metadata struct.
cols_to_add: dict[str, Column] = {
    "_source_File": col("_metadata.file_path"),
}

# view_count / like_count: LongType to safely hold counts exceeding 2 billion
# (e.g. "In the End" by Linkin Park: ~2B views, ~14M likes — beyond Integer range).
# comment_count: IntegerType — counts remain well within 32-bit range.
cols_to_cast: dict[str, Column] = {
    "_ingested_at":  col("_ingested_at").cast(TimestampType()),
    "view_count":    col("view_count").cast(LongType()),
    "like_count":    col("like_count").cast(LongType()),
    "comment_count": col("comment_count").cast(IntegerType()),
}


df_stream_enriched: DataFrame = (
    df_stream
    .withColumns(cols_to_add)
    .withColumns(cols_to_cast)
)


# ---------------------------------------------------------------------------
# Streaming write — bronze Delta table
# ---------------------------------------------------------------------------
(
    df_stream_enriched
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .table(bronze_music_stats_table)
)




