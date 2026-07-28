"""
music_pipeline_setup.py
-----------------------
Shared configuration and Unity Catalog setup utilities for the music analytics pipeline.

Defines all path constants, Unity Catalog table names, and idempotent setup
helper functions used across project notebooks.

Public exports:
    catalog_name, music_schema, volume_name,
    volume_path, json_landing_path,
    music_metadata_dir, music_metadata_file,
    bronze_music_metadata_table, bronze_music_stats_table,
    spark, w, yt_api_key,
    setup_catalog, setup_schema_and_volume,
    setup_music_metadata_and_json_dirs, setup_bronze_music_metadata_table
"""

from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient
import os

w = WorkspaceClient()
spark = SparkSession.getActiveSession()

# Unity Catalog identifiers
catalog_name: str = "dbr_dev"
music_schema: str = "music_analytics"
volume_name: str = "raw_landing_zone"

# Derived volume paths and fully-qualified table names
volume_path: str = f"{music_schema}.{volume_name}"
json_landing_path: str = f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/yt_snapshots"

music_metadata_dir: str = f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/music_metadata"
music_metadata_file: str = f"{music_metadata_dir}/music_discography.csv"
bronze_music_metadata_table: str = f"{catalog_name}.{music_schema}.bronze_music_metadata"
bronze_music_stats_table: str = f"{catalog_name}.{music_schema}.bronze_music_stats"

def setup_catalog() -> None:
    """Create the target catalog if it does not exist and set it as active.

    Creates ``catalog_name`` via ``CREATE CATALOG IF NOT EXISTS`` and then
    switches the session default catalog to it with ``USE CATALOG``.
    """
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
    spark.sql(f"USE CATALOG {catalog_name}")

def setup_schema_and_volume() -> None:
    """Create the music analytics schema and raw landing volume if absent.

    Both operations are idempotent (``IF NOT EXISTS``). The volume is created
    inside ``catalog_name.music_schema`` using the qualified ``volume_path``.
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {music_schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_path}")

def setup_music_metadata_and_json_dirs() -> None:
    """Create Volume directories for JSON snapshots and music metadata.

    Ensures that ``json_landing_path`` and ``music_metadata_dir`` exist inside
    the Unity Catalog Volume. Uses ``dbutils.fs.mkdirs``, which is a no-op
    when the directory already exists.
    """
    for dir_path in [json_landing_path, music_metadata_dir]:
        w.dbutils.fs.mkdirs(dir_path)

def setup_bronze_music_metadata_table() -> None:
    """Create the bronze music-metadata Delta table if it does not exist.

    Creates an empty ``bronze_music_metadata_table`` using
    ``CREATE TABLE IF NOT EXISTS``. Schema is inferred on the first write.
    """
    spark.sql(f"CREATE TABLE IF NOT EXISTS {bronze_music_metadata_table}")

    
# YouTube Data API v3 key — retrieved from Databricks secrets at import time.
yt_api_key: str = w.dbutils.secrets.get(
    scope="pawelnowak2004pri219_scope",
    key="pawelnowak-youtube-api",
)