# Bronze layer

Raw aircraft state vectors exactly as published by the producer (`extract/extract.py`), before any validation.

| | |
|---|---|
| **Storage** | Google Cloud Storage, `BRONZE_FOLDER` |
| **Format** | One JSON file per 1-minute event-time window, containing a JSON array of records |
| **File name** | `<window_start UTC, %Y-%m-%dT%H:%M:%S>-<shard>.json` |
| **Grain** | One record per aircraft state report from the OpenSky `/states/all` endpoint |
| **Event time** | `last_contact` (sent to Pub/Sub as a message attribute, in milliseconds) |

Types follow BigQuery standard SQL so the files can be loaded or queried as an external table. Modes reflect the OpenSky API contract: bronze is unvalidated, so the silver layer is where they're enforced.

## Table: `bronze_flight_states`

| Field | Type | Mode | Description |
|---|---|---|---|
| `icao24` | STRING | REQUIRED | Unique ICAO 24-bit transponder address, as a hex string (e.g. `e49fe1`) |
| `callsign` | STRING | NULLABLE | Callsign of the vehicle, right-padded with spaces to 8 characters |
| `origin_country` | STRING | REQUIRED | Country of registration, inferred by OpenSky from the ICAO address |
| `time_position` | INT64 | NULLABLE | Unix time (seconds) of the last position update |
| `last_contact` | INT64 | REQUIRED | Unix time (seconds) of the last message received from the transponder. If the API returns null, the producer fills it with the ingestion time and sets `has_valid_last_contact = FALSE` |
| `longitude` | FLOAT64 | NULLABLE | WGS-84 longitude in decimal degrees |
| `latitude` | FLOAT64 | NULLABLE | WGS-84 latitude in decimal degrees |
| `baro_altitude` | FLOAT64 | NULLABLE | Barometric altitude in meters |
| `on_ground` | BOOL | REQUIRED | `TRUE` if the position came from a surface position report |
| `velocity` | FLOAT64 | NULLABLE | Ground speed in m/s |
| `true_track` | FLOAT64 | NULLABLE | True track in decimal degrees, clockwise from north (0° = north) |
| `vertical_rate` | FLOAT64 | NULLABLE | Vertical rate in m/s. Positive = climbing, negative = descending |
| `sensors` | INT64 | REPEATED | IDs of the receivers that contributed to this state. Arrives as `null` from `/states/all` unless sensor serials are requested; a REPEATED field loads null as an empty array |
| `geo_altitude` | FLOAT64 | NULLABLE | Geometric altitude in meters |
| `squawk` | STRING | NULLABLE | Transponder code (squawk) |
| `spi` | BOOL | REQUIRED | Whether the flight status indicates a special purpose indicator |
| `position_source` | INT64 | REQUIRED | Origin of the position: `0` ADS-B, `1` ASTERIX, `2` MLAT, `3` FLARM |
| `ingestion_timestamp` | INT64 | REQUIRED | Unix time (seconds) when the producer received the API response. Added by the pipeline |
| `has_valid_last_contact` | BOOL | REQUIRED | `FALSE` if `last_contact` was missing from the API and filled in by the producer. Added by the pipeline |

## Example record

```json
{
  "icao24": "e49fe1",
  "callsign": "PSENG   ",
  "origin_country": "Brazil",
  "time_position": 1792251172,
  "last_contact": 1792251173,
  "longitude": -53.2389,
  "latitude": -20.6381,
  "baro_altitude": 12192,
  "on_ground": false,
  "velocity": 162.98,
  "true_track": 323.6,
  "vertical_rate": 0,
  "sensors": null,
  "geo_altitude": 12801.6,
  "squawk": null,
  "spi": false,
  "position_source": 0,
  "ingestion_timestamp": 1792251480,
  "has_valid_last_contact": true
}
```
