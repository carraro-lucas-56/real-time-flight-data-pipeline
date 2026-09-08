import os
import json
import logging

from dotenv import load_dotenv
import apache_beam as beam
from apache_beam.transforms.window import FixedWindows
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.fileio import WriteToFiles

from dataflow.utils import (parse_json,
                            window_file_name,
                            IsInvalidRecord,
                            AddWindowStart,
                            is_in_the_air,
                            has_proper_geolocation_data,
                            select_desired_fields,
                            add_curr_country,
                            add_missing_countries,
                            has_velocity,
                            get_most_recent_update,
                            CalculateMonitoringMetrics,
                            display_monitoring_metrics)

from dataflow.constants import (FIELDS_METRIC_1,
                                FIELDS_METRIC_2)

logger = logging.getLogger()

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
TEMP_LOCATION = os.getenv("TEMP_LOCATION")
PUB_SUB_TOPIC = os.getenv("PUB_SUB_TOPIC")
STAGING_FOLDER = os.getenv("STAGING_FOLDER")
BRONZE_FOLDER = os.getenv("BRONZE_FOLDER")
SILVER_FOLDER = os.getenv("SILVER_FOLDER")
GOLD_FOLDER = os.getenv("GOLD_FOLDER")
BAD_RECORDS_FOLDER = os.getenv("BAD_RECORDS_FOLDER")
AIRCRAFTS_PER_COUNTRY_TABLE_ID=os.getenv("AIRCRAFTS_PER_COUNTRY_TABLE_ID")
AIRCRAFTS_VELOCITY_TABLE_ID=os.getenv("AIRCRAFTS_VELOCITY_TABLE_ID")

options = PipelineOptions(
    runner="DataflowRunner",
    # runner="DirectRunner",
    streaming=True,
    project=PROJECT_ID,
    region="us-east1",
    worker_zone="us-east1-d",
    staging_location=STAGING_FOLDER,
    temp_location=TEMP_LOCATION,
    # max_num_workers=10,

    setup_file="./setup.py",
    prebuild_sdk_container=True
)


with beam.Pipeline(options=options) as pipeline:

    # ---------------------- BRONZE LAYER ---------------------- 
    bronze_layer = (
        pipeline
         | "Read from pub/sub" >> beam.io.ReadFromPubSub(PUB_SUB_TOPIC, timestamp_attribute='last_contact')
         | "Docode byte to str" >> beam.Map(lambda x : x.decode('utf-8'))
         | "Convert json string to dict object" >> beam.Map(parse_json)
         | "Create bronze window" >> beam.WindowInto(FixedWindows(60))
    )

    # Save bronze to GCS
    (bronze_layer
        | "Use single key" >> beam.Map(lambda record: ('window_data', record))
        | "Collapse window" >> beam.GroupByKey()
        | "Serialize window" >> beam.Map(lambda kv: json.dumps(list(kv[1])))
        | "Write Bronze to GCS" >> WriteToFiles(BRONZE_FOLDER,
                                                file_naming=window_file_name)
    )

    monitoring = (
        bronze_layer
        | "Create monitoring key" >> beam.Map(lambda x: ("metrics", x))
        | "Group monitoring by window" >> beam.GroupByKey()
        | "Calculate window metrics" >> beam.ParDo(CalculateMonitoringMetrics())
        | "Display monitoring metrics" >> beam.Map(display_monitoring_metrics)
    )

    # ---------------------- SILVER LAYER ---------------------- 
    silver_layer = (
        bronze_layer
        | "filter undesired records" >> beam.ParDo(IsInvalidRecord()).with_outputs(
            "BAD_RECORD",
            main="GOOD_RECORD"
        )
    )    

    (silver_layer["BAD_RECORD"]
        | "Use single key 3" >> beam.Map(lambda record: ('window_data', record))
        | "Collapse window 3" >> beam.GroupByKey()
        | "Serialize window 3" >> beam.Map(lambda kv: json.dumps(list(kv[1])))
        | "Write bad records to GCS" >> WriteToFiles(BAD_RECORDS_FOLDER,
                                                     file_naming=window_file_name)
    )

    cleaned_data = (
        silver_layer["GOOD_RECORD"]
    )

    # Save silver to GCS
    (cleaned_data
        | "Use single key 2" >> beam.Map(lambda record: ('window_data', record))
        | "Collapse window 2" >> beam.GroupByKey()
        | "Serialize window 2" >> beam.Map(lambda kv: json.dumps(list(kv[1])))
        | "Write silver to GCS" >> WriteToFiles(SILVER_FOLDER,
                                                file_naming=window_file_name)
    )

    # ---------------------- GOLD LAYER ---------------------- 
    
    # ---------------------- METRIC 1 ----------------------
    
    aircrafts =(
        cleaned_data
        | "Filter desired fields (metric 1)" >> beam.Map(select_desired_fields, FIELDS_METRIC_1)
        | "Filter aircrafts in the air" >> beam.Filter(is_in_the_air)
        | "Filter aircrafts with geolocation data" >> beam.Filter(has_proper_geolocation_data)
        | "Greb the most recent update (Metric 1)" >> get_most_recent_update()
    )
    
    aircraft_per_country = (
        aircrafts
        | "Add country" >> beam.Map(add_curr_country)
        | "Group by country" >> beam.Map(lambda x: (x['curr_country'], x))
        | "Count aircraft per country" >> beam.combiners.Count().PerKey()
        | "Converts to a single dict" >> beam.combiners.ToDict().without_defaults()
        | "Add missing countries" >> beam.Map(add_missing_countries)
        | "Add window metadata" >> beam.ParDo(AddWindowStart())
    )
    
    (aircraft_per_country | "Write aircrafts_per_country to bq" >> beam.io.WriteToBigQuery(
        table=AIRCRAFTS_PER_COUNTRY_TABLE_ID,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )

    # ---------------------- METRIC 2 ----------------------

    realtime_aircraft_velocity = (
        cleaned_data
        | "Filter desired fields (metric 2)" >> beam.Map(select_desired_fields, FIELDS_METRIC_2)
        | "Filter records with velocity data" >> beam.Filter(has_velocity)
        | "Greb the most recent update (metric 2)" >> get_most_recent_update()
        | "Remove 'last_contact' field" >> beam.Map(lambda x: {"icao24" : x["icao24"],
                                                               "velocity" : x["velocity"]})
        | "Add window metadata (metric 2)" >>  beam.ParDo(AddWindowStart())
    )

    (realtime_aircraft_velocity | "Write realtime_aricraft_velocity to bq" >> beam.io.WriteToBigQuery(
        table=AIRCRAFTS_VELOCITY_TABLE_ID,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )


