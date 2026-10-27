# Gold layer

Aggregated metrics computed from silver records and written to BigQuery by the Dataflow job with `WRITE_APPEND`. The dashboard reads them through views that return only recent data.

| | |
|---|---|
| **Storage** | BigQuery, dataset `aircraft_metrics` |
| **Write mode** | Streaming append from Dataflow |
| **Consumers** | Views `*_recent`, queried by Power BI in DirectQuery mode |

Table IDs are configured through environment variables (see `.env.example`).

## Table: `aircrafts_per_country`

Number of airborne aircraft over each South American country, per minute.

| | |
|---|---|
| **Env var** | `AIRCRAFTS_PER_COUNTRY_TABLE_ID` |
| **View** | `aircrafts_per_country_recent` |
| **Window** | 1-minute fixed |
| **Grain** | One row per country per window (14 rows per window, including countries with 0 aircraft) |

| Field | Type | Mode | Description |
|---|---|---|---|
| `window_start` | TIMESTAMP | REQUIRED | Start of the 1-minute event-time window (UTC) |
| `country` | STRING | REQUIRED | Country the aircraft is flying over, from reverse geocoding its latest position. Uses underscores for spaces (e.g. `Falkland_Islands`) |
| `aircraft_count` | INT64 | REQUIRED | Distinct airborne aircraft over the country in the window, counting each `icao24` once by its most recent report |

Aircraft that aren't over one of the 14 countries in `countries_dict` are excluded.

## Table: `realtime_aircraft_velocity`

Latest ground speed of each airborne aircraft, per minute.

| | |
|---|---|
| **Env var** | `AIRCRAFTS_VELOCITY_TABLE_ID` |
| **View** | `realtime_aircraft_velocity_recent` |
| **Window** | 1-minute fixed |
| **Grain** | One row per aircraft per window |

| Field | Type | Mode | Description |
|---|---|---|---|
| `icao24` | STRING | REQUIRED | Unique ICAO 24-bit transponder address |
| `velocity` | FLOAT64 | REQUIRED | Ground speed in m/s, from the aircraft's most recent report in the window |
| `window_start` | TIMESTAMP | REQUIRED | Start of the 1-minute event-time window (UTC) |

Only aircraft in the air with a non-null velocity are included.

## Table: `aircraft_vertical_anomalies`

Vertical acceleration of each aircraft between consecutive reports, flagged when it indicates a dangerous descent.

| | |
|---|---|
| **Env var** | `AIRCRAFT_VERTICAL_ANOMALIES_TABLE_ID` |
| **View** | `aircraft_vertical_anomalies_recent` |
| **Window** | 1-minute sliding, 30-second period |
| **Grain** | One row per pair of consecutive reports, per aircraft, per window |

| Field | Type | Mode | Description |
|---|---|---|---|
| `icao24` | STRING | REQUIRED | Unique ICAO 24-bit transponder address |
| `timestamp` | TIMESTAMP | REQUIRED | `last_contact` of the later report in the pair (UTC) |
| `vertical_rate` | FLOAT64 | REQUIRED | Vertical rate in m/s at `timestamp`. Positive = climbing, negative = descending |
| `vertical_acceleration` | FLOAT64 | REQUIRED | Change in vertical rate divided by the time between the two reports, in m/s² |
| `vertical_acceleration_danger` | BOOL | REQUIRED | `TRUE` when `vertical_acceleration` ≤ −1.0 m/s² (`VERTICAL_ACCELERATION_THRESHOLD`) |
| `window_start` | TIMESTAMP | REQUIRED | Start of the sliding window (UTC) |

Because sliding windows overlap, the same pair of reports can appear in two windows. This is intentional: it keeps every window's evaluation complete so the dashboard can follow how an aircraft's vertical behavior changes over time. Deduplicate on (`icao24`, `timestamp`) when counting distinct events.
