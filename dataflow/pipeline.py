import os
import json

from dotenv import load_dotenv
import apache_beam as beam
from apache_beam.transforms.window import FixedWindows
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.fileio import WriteToFiles

from dataflow.utils import (parse_json,
                            window_file_name,
                            IsInvalidRecord,)

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
TEMP_LOCATION = os.getenv("TEMP_LOCATION")
PUB_SUB_TOPIC = os.getenv("PUB_SUB_TOPIC")
STAGING_FOLDER = os.getenv("STAGING_FOLDER")
BRONZE_FOLDER = os.getenv("BRONZE_FOLDER")
SILVER_FOLDER = os.getenv("SILVER_FOLDER")
BAD_RECORDS_FOLDER = os.getenv("BAD_RECORDS_FOLDER")

options = PipelineOptions(
    runner="DataflowRunner",
    streaming=True,
    project=PROJECT_ID,
    region="us-east1",
    # worker_zone="us-central1-b",
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
        | "Use single key" >> beam.Map(lambda record: ("window_data", record))
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
        | "Use single key 2" >> beam.Map(lambda record: ("window_data", record))
        | "Collapse window 2" >> beam.GroupByKey()
        | "Serialize window 2" >> beam.Map(lambda kv: json.dumps(list(kv[1])))
        | "Write silver to GCS" >> WriteToFiles(SILVER_FOLDER,
                                                file_naming=window_file_name)
    )

    (silver_layer["BAD_RECORD"]
        | "Use single key 3" >> beam.Map(lambda record: ("window_data", record))
        | "Collapse window 3" >> beam.GroupByKey()
        | "Serialize window 3" >> beam.Map(lambda kv: json.dumps(list(kv[1])))
        | "Write bad records to GCS" >> WriteToFiles(BAD_RECORDS_FOLDER,
                                                     file_naming=window_file_name)
    )
    
    valid_records = silver_layer["GOOD_RECORDS"]

    


