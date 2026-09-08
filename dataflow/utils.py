import json 
import logging

import reverse_geocoder as rg
import apache_beam as beam
from apache_beam.pvalue import TaggedOutput
from apache_beam.metrics import Metrics

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

    return f"{window_start:%Y-%m-%dT%H:%M:%S}-{shard_index}.json"

def is_valid(record: dict) -> bool:
    # Required fields
    if (not all(record.get(field) is not None for field in REQUIRED_FIELDS)):
        return False

    # Coordinates (they can be null or not be present but cannot be out of boundaries)
    if record.get("latitude") is not None and not -90 <= record.get("latitude",0) <= 90:
        return False

    if record.get("longitude") is not None and not -180 <= record.get("longitude",0) <= 180:
        return False

    # non-negative fields (they can be null or not be present but cannot be negative)
    if not all(
        record.get(field) is None or record.get(field,0) >= 0
        for field in NON_NEGATIVE_FIELDS
    ):
        return False

    # Checking if the last_contact field is legit (wasn't artifically added in the extraction)
    if not record["has_valid_last_contact"]:
        return False
    
    return True

class IsInvalidRecord(beam.DoFn):
    def __init__(self):
        super().__init__()
        self.valid_records = Metrics.counter(self.__class__, "valid_records")
        self.invalid_records = Metrics.counter(self.__class__, "invalid_records")

    def process(self, element: dict | None):
        if is_valid(element):
            self.valid_records.inc()
            yield element
        else:
            self.invalid_records.inc()
            yield TaggedOutput("BAD_RECORD", element)

class AddWindowStart(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        yield {
            **element,
            "window_start": window.start.to_utc_datetime()
        }    

def select_desired_fields(record: dict, fields: list[str]) -> dict:
    return {field: record.get(field) for field in fields}

def get_most_recent_update():
    """
    returns a transformation that collect the most up to 
    date record of each aircraft in the window    
    """

    return (
        beam.Map(lambda x: (x["icao24"], x))
        | beam.CombinePerKey(
            max,
            key=lambda x: int(x["last_contact"])
        )
        | beam.Values()
    )

# ---------------- HELPER FUNCTIONS FOR METRIC 1 ---------------- 

def has_proper_geolocation_data(record: dict) -> bool:
    return all(record.get(field) is not None for field in COORDINATES_FIELDS)

def is_in_the_air(record: dict) -> bool:
    return not record.get("on_ground", True) 

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
    "Add the countries which no aircrafts are currently flying over"
    return {country: record.get(country,0) for country in countries_list}

# ---------------- HELPER FUNCTIONS FOR METRIC 2 ---------------- 

def has_velocity(record: dict) -> bool:
    return record.get("velocity") is not None


# ---------------- MONITORING FUNCTIONS ----------------

class CalculateMonitoringMetrics(beam.DoFn):

    def process(self, element, window=beam.DoFn.WindowParam):
        _, records = element

        records = list(records)

        yield {
            "window_start": window.start.to_utc_datetime(),
            "window_end": window.end.to_utc_datetime(),
            "records_received": len(records),
            "invalid_records" : len([record for record in records if not is_valid(record)]),
            "records_missing_geolocation" : len([record for record in records if not has_proper_geolocation_data(record)]),
            "records_missing_velocity" : len([record for record in records if not has_velocity(record)]),
            "aircrafts_in_air" : len([record for record in records if is_in_the_air(record)])
        }

def display_monitoring_metrics(metrics: dict) -> None:
    for k, v in metrics.items():
        logger.info(f"{k} : {v}")
    

