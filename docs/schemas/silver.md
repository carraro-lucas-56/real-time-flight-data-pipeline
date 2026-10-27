# Silver layer

Validated aircraft states. Every bronze record is checked and routed to exactly one of two outputs: valid records or rejected records. Both keep the bronze schema; what changes is the guarantees on the data.

| | |
|---|---|
| **Storage** | Google Cloud Storage: `SILVER_FOLDER` (valid) and `BAD_RECORDS_FOLDER` (rejected) |
| **Format** | One JSON file per 1-minute event-time window, containing a JSON array of records |
| **File name** | `<window_start UTC, %Y-%m-%dT%H:%M:%S>-<shard>.json` |
| **Grain** | One record per aircraft state report |
| **Validation** | `is_valid()` in `dataflow/utils.py` |

## Table: `silver_flight_states`

Records that passed every validation rule. Rows where a rule tightens the bronze contract are marked in the **Rule** column.

| Field | Type | Mode | Rule | Description |
|---|---|---|---|---|
| `icao24` | STRING | REQUIRED | Must be present | Unique ICAO 24-bit transponder address, as a hex string |
| `callsign` | STRING | NULLABLE | | Callsign, right-padded with spaces to 8 characters |
| `origin_country` | STRING | REQUIRED | | Country of registration |
| `time_position` | INT64 | NULLABLE | | Unix time (seconds) of the last position update |
| `last_contact` | INT64 | REQUIRED | Must be present and real (see `has_valid_last_contact`) | Unix time (seconds) of the last transponder message. Used as event time |
| `longitude` | FLOAT64 | NULLABLE | If present, between −180 and 180 | WGS-84 longitude in decimal degrees |
| `latitude` | FLOAT64 | NULLABLE | If present, between −90 and 90 | WGS-84 latitude in decimal degrees |
| `baro_altitude` | FLOAT64 | NULLABLE | If present, ≥ 0 | Barometric altitude in meters |
| `on_ground` | BOOL | REQUIRED | Must be present | `TRUE` if the position came from a surface position report |
| `velocity` | FLOAT64 | NULLABLE | If present, ≥ 0 | Ground speed in m/s |
| `true_track` | FLOAT64 | NULLABLE | | True track in decimal degrees, clockwise from north |
| `vertical_rate` | FLOAT64 | NULLABLE | | Vertical rate in m/s. Positive = climbing, negative = descending |
| `sensors` | INT64 | REPEATED | | IDs of the contributing receivers |
| `geo_altitude` | FLOAT64 | NULLABLE | | Geometric altitude in meters |
| `squawk` | STRING | NULLABLE | | Transponder code (squawk) |
| `spi` | BOOL | REQUIRED | | Special purpose indicator |
| `position_source` | INT64 | REQUIRED | | `0` ADS-B, `1` ASTERIX, `2` MLAT, `3` FLARM |
| `ingestion_timestamp` | INT64 | REQUIRED | | Unix time (seconds) when the producer received the API response |
| `has_valid_last_contact` | BOOL | REQUIRED | Always `TRUE` | Records with a producer-filled `last_contact` are rejected, so event time is never fabricated |

## Table: `silver_bad_records`

Records that failed at least one rule above. They're written to their own folder instead of being dropped, so data-quality problems in the source can be inspected.

The schema is identical to [`bronze_flight_states`](bronze.md#table-bronze_flight_states), with no guarantees beyond the bronze contract. Any field covered by a rule may hold the value that caused the rejection.
