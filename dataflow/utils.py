import json 
import logging

import apache_beam as beam
from apache_beam.pvalue import TaggedOutput

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

REQUIRED_FIELDS=[
    "icao24", 
    "last_contact",
    "on_ground",
]

NON_NEGATIVE_FIELDS=[
    "velocity",
    "baro_altitude"
]

def is_valid(record: dict) -> bool:
    # Required fields
    if (not all(record.get(field) is not None for field in REQUIRED_FIELDS)):
        return False

    # Coordinates
    if not -90 <= record.get("latitude",0) <= 90:
        return False

    if not -180 <= record.get("longitude",0) <= 180:
        return False

    # non-negative fields
    if not all(
        record.get(field,0) >= 0
        for field in NON_NEGATIVE_FIELDS
    ):
        return False

    # Checking if the last_contact field is legit (wasn't artifically added in the extraction)
    if not record["has_valid_last_contact"]:
        return False
    
    return True

class IsInvalidRecord(beam.DoFn):
    def process(self, element: dict | None):
        if is_valid(element):
            yield element
        else:
            yield TaggedOutput("BAD_RECORD", element)

class AddWindowStart(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        yield {
            **element,
            "window_start": window.start.to_utc_datetime()
        }    




