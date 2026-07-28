import json
import sys

sys.path.insert(0, "../../../00_setup/pawel_project")

from config import json_landing_path
from datetime import datetime 
from fetch_data_from_youtube_api import read_data_from_api

all_video_responses = read_data_from_api()

dbutils.fs.mkdirs(json_landing_path)

timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S") # Ingestion datetime
json_file_path = f"{json_landing_path}/yt_stats_{timestamp_str}.json"

with open(json_file_path, "w", encoding="utf-8") as f:
    json.dump(all_video_responses, f, ensure_ascii=False, indent=4)