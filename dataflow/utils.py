import json 
import logging

import reverse_geocoder as rg
import apache_beam as beam
from apache_beam.pvalue import TaggedOutput

from dataflow.constants import *

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

def is_valid(record: dict) -> bool:
    # Required fields
    if (not all(record.get(field) is not None for field in REQUIRED_FIELDS)):
        return False

    # Coordinates
    if record.get("latitude",0) is None or not -90 <= record.get("latitude",0) <= 90:
        return False

    if record.get("longitude",0) is None or not -180 <= record.get("longitude",0) <= 180:
        return False

    # non-negative fields
    if not all(
        record.get(field) is not None and record.get(field) >= 0
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

def has_proper_geolocation_data(record: dict) -> bool:
    return all(record.get(field) is not None for field in COORDINATES_FIELDS)

def is_in_the_air(record: dict) -> bool:
    return not record.get("on_groud", False) 

def select_desired_fields(record: dict) -> dict:
    return {field: record[field] for field in DESIRED_FIELDS_METRIC_1}

def get_south_america_country(lat: float, lon: float) -> str:
    """
    Identifies if a latitude/longitude coordinate is inside a South American country.
    Returns the country name if True, otherwise returns 'other'.
    """
    
    try:
        coordinates = (lat, lon)
        
        # search() returns a list of dicts. stream=True minimizes memory overhead.
        # mode=1 runs in single-process mode, which is much safer inside distributed tasks.
        result = rg.search(coordinates, mode=1)[0]
        
        country_code = result.get('cc')
        logger.info(f"country_code: {country_code}")
        print(f"country_code: {country_code}")
        
        return countries_dict.get(country_code, 'other')

    except Exception as e:
        logger.warning(f"Something unusual when getting cc {str(e)}")

        # Safety fallback for invalid coordinates or processing errors
        return 'other'
    
def add_curr_country(record: dict):
    "Adds the country the aircraft is currently flying over."

    return {**record,
            "curr_country" : get_south_america_country(record["latitude"],record["longitude"])}

def add_missing_countries(record: dict) -> dict:
    return {country: record.get(country,0) for country in countries_list}
