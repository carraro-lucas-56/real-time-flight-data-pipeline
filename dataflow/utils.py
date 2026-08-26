import json 
import logging

logger = logging.getLogger(__name__)

def parse_json(data: str) -> dict | None:
    try:
        return json.loads(data)
    except Exception as e:
        logger.error("Failed to parse the json")
        return None

def window_file_name(window, pane, shard_index, total_shards, compression, destination):
    window_start = window.start.to_utc_datetime()

    return f"{window_start:%Y-%m-%dT%H:%M:%S}.json"
