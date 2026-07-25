# Shared config for the music analytics pipeline.
# Defines all path constants, table names, and setup helper functions used across notebooks.

from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient
import os

w = WorkspaceClient()

# Define catalog, schema, and volume names
catalog_name = "dbr_dev"
music_schema = "music_analytics"
volume_name = "raw_landing_zone"

# Volume paths and table name
volume_path: str = f"{music_schema}.{volume_name}"
json_landing_path: str = f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/jsons/"

music_metadata_dir: str = f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/music_metadata/"
music_metadata_file = f"{music_metadata_dir}/music_discography.csv/"
bronze_music_metadata_table = f"{catalog_name}.{music_schema}.bronze_music_metadata"

# Create the catalog and set it as the active catalog
def setup_catalog():
    spark = SparkSession.getActiveSession()
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
    spark.sql(f"USE CATALOG {catalog_name}")

# Create the schema and volume if they do not exist
def setup_schema_and_volume():
    spark = SparkSession.getActiveSession()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {music_schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_path}")

# Create directories for JSON files and music metadata
def setup_music_metadata_and_json_dirs():
    for dir_path in [json_landing_path, music_metadata_dir]:
        w.dbutils.fs.mkdirs(dir_path)

# Create the bronze Delta table if it doesn't exist
def setup_bronze_music_metadata_table():
    spark = SparkSession.getActiveSession()
    spark.sql(f"CREATE TABLE IF NOT EXISTS {bronze_music_metadata_table}")

    
# Retrieve the YouTube API key from Databricks secrets
yt_api_key = w.dbutils.secrets.get(scope="pawelnowak2004pri219_scope", key="pawelnowak-youtube-api")