import sys

sys.path.insert(0, "../../../00_setup/pawel_project")


from config import bronze_music_stats_table, json_landing_path, catalog_name,music_schema, volume_name

# checkpoint_path = f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/_checkpoints/yt_stats_bronze"
# schema_path = f"/Volumes/{catalog_name}/{music_schema}/{volume_name}/_schema/yt_stats_bronze"

# from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, IntegerType



# # For now our schema is really fixed
# fixed_schema = StructType([
#     StructField("_ingested_at", StringType(), True),
#     StructField("video_id", StringType(), True),
#     StructField("album", StringType(), True),
#     StructField("published_at", TimestampType(), True),
#     StructField("video_title", StringType(), True),
#     StructField("view_count", StringType(), True),
#     StructField("like_count", StringType(), True),
#     StructField("comment_count", StringType(), True),
#     StructField("author", StringType(), True),
#     StructField("song_title", StringType(), True)
# ])


# # stream-read the json files
# df_stream = (
#     spark.readStream
#     .format("cloudFiles")
#     .option("cloudFiles.format", "json")
#     .option("cloudFiles.schemaLocation", schema_path)
#     .option("cloudFiles.schemaEvolutionMode", "rescue")  # We might want to include new columns in the future, so that's we we rescue them.
#     .option("multiline", "true")
#     .schema(fixed_schema)
#     .load(json_landing_path)
# )

# from pyspark.sql.functions import current_timestamp, col

# # Add necessary metadata columns
# cols_to_add = {
#     "_source_File":col("_metadata.file_path")
# }

# # cast view_count and like_count and comment_count to int
# cols_to_cast = {
#     "_ingested_at":col("_ingested_at").cast(TimestampType()),
#     "view_count":col("view_count").cast(LongType()), # view and like count are expected to be very enormous (e.g. In the End by Linkin Park has over 2bilions views and 14 millions likes which is way more than Integer can hold)
#     "like_count":col("like_count").cast(LongType()),
#     "comment_count":col("comment_count").cast(IntegerType()) # comment count is relatively small and can be safely casted to Integer
# }


# df_stream_enriched = (
#     df_stream
#     .withColumns(cols_to_add)
#     .withColumns(cols_to_cast)
# )


# (df_stream_enriched
#     .writeStream
#     .format("delta")
#     .outputMode("append")
#     .option("checkpointLocation", checkpoint_path)
#     .trigger(availableNow=True)
#     .table(bronze_music_stats_table)
# )




