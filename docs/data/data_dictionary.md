# Data Dictionary

## nyc_tlc_yellow (raw / bronze source: TLC Yellow Taxi trip records)

| Field | Type (source) | Description |
|---|---|---|
| tpep_pickup_datetime | timestamp | Pickup date and time |
| tpep_dropoff_datetime | timestamp | Dropoff date and time |
| PULocationID | integer | TLC taxi zone ID where the trip started |
| DOLocationID | integer | TLC taxi zone ID where the trip ended |
| trip_distance | float | Trip distance in miles, as reported by the taximeter |
| passenger_count | integer | Number of passengers, driver-entered |
| fare_amount | float | Metered fare |
| total_amount | float | Total amount charged, including tolls, tips and fees |

Exact field names and types are confirmed against the downloaded file at ingestion time;
the schema contract in `src/contracts/tlc_trip_schema.yaml` is the source of truth for
required columns.

## nyc_tlc_taxi_zone (reference data)

| Field | Type | Description |
|---|---|---|
| LocationID | integer | Unique taxi zone identifier, join key for PU/DOLocationID |
| Borough | string | NYC borough name |
| Zone | string | Taxi zone name |
| service_zone | string | TLC service zone classification |

## noaa_ghcnh (planned, milestone 2)

| Field | Type | Description |
|---|---|---|
| timestamp | timestamp (UTC) | Observation time |
| station_id | string | GHCNh station identifier |
| latitude / longitude | float | Station coordinates |
| temperature | float | Air temperature |
| dew_point | float | Dew point temperature |
| wind_speed | float | Wind speed |
| precipitation | float | Precipitation amount |
| visibility | float | Visibility |
| pressure | float | Atmospheric pressure |
| weather_condition | string | Reported weather condition code |

## Derived fields (silver layer, planned)

| Field | Derivation |
|---|---|
| pickup_date | date(tpep_pickup_datetime) |
| pickup_hour | hour(tpep_pickup_datetime), used as the weather join bucket |
| trip_duration_seconds | tpep_dropoff_datetime - tpep_pickup_datetime |
