from pyspark.sql.functions import col, regexp_extract


def find_video_ids(table) -> list:
    # Read the youtube video id from the url 
    table_widh_ids = table.withColumn(
        "video_id", 
        regexp_extract(col("url"), r"v=([a-zA-Z0-9_-]{11})", 1)
    )

    video_id_rows = table_widh_ids.select("video_id").distinct().collect()
    video_ids_list:list[str] = [row.video_id for row in video_id_rows if row.video_id]

    return [table_widh_ids, video_ids_list]