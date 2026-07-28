import sys

sys.path.insert(0, "../../../00_setup/pawel_project")


from extract_video_ids import find_video_ids
import requests
from config import yt_api_key, bronze_music_metadata_table, spark
from pyspark.sql.functions import col
from datetime import datetime

# API endpoint
yt_video_url: str ="https://www.googleapis.com/youtube/v3/videos" 


# The youtube API can take up to 50 videos per batch (more videos will result in 400 error code by API)
def read_data_from_api(BATCH_SIZE: int = 50) -> list[dict]:
    metadata_table = spark.read.table(bronze_music_metadata_table)
    metadata_table, video_ids_list = find_video_ids(metadata_table)

    # Precompute video_id -> [album, title, author] mapping for fast lookup
    video_id_attr_map = {
        row.video_id: [row.album, row.title, row.author]
        for row in metadata_table.select("video_id", "album", "title", "author")
        .filter(col("video_id").isNotNull())
        .toLocalIterator()
    }

    # Split the videos into predefined-sized batches
    id_batches = [video_ids_list[i:i + BATCH_SIZE] for i in range(0, len(video_ids_list)+1, BATCH_SIZE)]

    all_video_responses = []

    for idx, batch in enumerate(id_batches):
        batch_ids_str = ",".join(batch)
        
        params = {
            "part": "snippet, statistics",
            "id": batch_ids_str,
            "key": yt_api_key
        }
        
        response = requests.get(yt_video_url, params=params)
        
        if response.status_code == 200:
            items = response.json().get("items", [])

            for item in items:
                item_snippet = item.get("snippet", {})
                item_statistics = item.get("statistics", {})

                video_id = item.get("id")
                published_at = item_snippet.get("publishedAt")
                title:str = item_snippet.get("title")

                viewCount:str = item_statistics.get("viewCount")
                likeCount:str = item_statistics.get("likeCount")
                dislikeCount:str = item_statistics.get("dislikeCount")
                commentCount:str = item_statistics.get("commentCount")

                album, song_title, author = video_id_attr_map.get(video_id, [None, None, None])

                video_response = {
                    "_ingested_at": datetime().now().isoformat(),
                    "video_id": video_id,
                    "album": album,
                    "published_at": published_at,
                    "video_title": title,
                    "view_count": viewCount,
                    "like_count": likeCount,
                    "comment_count": commentCount,
                    "author": author,
                    "song_title": song_title,
                }
                all_video_responses.append(video_response)

        else:
            print("Error ")

    return all_video_responses