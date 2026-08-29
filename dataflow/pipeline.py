import os
import json

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
                            add_missing_countries)


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

options = PipelineOptions(
    runner="DataflowRunner",
    # runner="DirectRunner",
    streaming=True,
    project=PROJECT_ID,
    region="us-east1",
    worker_zone="us-east1-d",
    staging_location=STAGING_FOLDER,
    temp_location=TEMP_LOCATION,
    max_num_workers=5,

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

    (bronze_layer
        | "Use single key" >> beam.Map(lambda record: ('window_data', record))
        | "Collapse window" >> beam.GroupByKey()
        | "Serialize window" >> beam.Map(lambda kv: json.dumps(list(kv[1])))
        | "Write Bronze to GCS" >> WriteToFiles(BRONZE_FOLDER,
                                                file_naming=window_file_name)
    )

    # ---------------------- SILVER LAYER ---------------------- 
    silver_layer = (
        bronze_layer
        | "filter undesired records" >> beam.ParDo(IsInvalidRecord()).with_outputs(
            "BAD_RECORD",
            main="GOOD_RECORD"
        )
    )    

    (silver_layer["GOOD_RECORD"]
        | "Use single key 2" >> beam.Map(lambda record: ('window_data', record))
        | "Collapse window 2" >> beam.GroupByKey()
        | "Serialize window 2" >> beam.Map(lambda kv: json.dumps(list(kv[1])))
        | "Write silver to GCS" >> WriteToFiles(SILVER_FOLDER,
                                                file_naming=window_file_name)
    )

    (silver_layer["BAD_RECORD"]
        | "Use single key 3" >> beam.Map(lambda record: ('window_data', record))
        | "Collapse window 3" >> beam.GroupByKey()
        | "Serialize window 3" >> beam.Map(lambda kv: json.dumps(list(kv[1])))
        | "Write bad records to GCS" >> WriteToFiles(BAD_RECORDS_FOLDER,
                                                     file_naming=window_file_name)
    )
    
    valid_records = silver_layer["GOOD_RECORD"]

    # ---------------------- GOLD LAYER ---------------------- 
    aircraft_per_country = (
        valid_records
        | "Filter desired fields" >> beam.Map(select_desired_fields)
        | "Filter aircrafts in the air" >> beam.Filter(is_in_the_air)
        | "Filter aircrafts with geolocation data" >> beam.Filter(has_proper_geolocation_data)
        | "Group by aircraft id (icao24)" >> beam.Map(lambda x: (x['icao24'], x))
        | "Greb the most recent update" >> beam.CombinePerKey(max, key=lambda x: int(x["last_contact"]))
        | "Drop keys" >> beam.Values() 

        | "Add country" >> beam.Map(add_curr_country)
        | "Group by country" >> beam.Map(lambda x: (x['curr_country'], x))
        | "Count aircraft per country" >> beam.combiners.Count().PerKey()
        | "Converts to a single dict" >> beam.combiners.ToDict().without_defaults()
        | "Add missing countries" >> beam.Map(add_missing_countries)
        | "Add window metadata" >> beam.ParDo(AddWindowStart())
    )
    
    (aircraft_per_country | "Write gold to bq" >> beam.io.WriteToBigQuery(
        table=AIRCRAFTS_PER_COUNTRY_TABLE_ID,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )

