CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.hourly_trip_metrics (
    trip_hour TIMESTAMPTZ NOT NULL,
    trip_count BIGINT NOT NULL,
    total_distance NUMERIC NOT NULL,
    total_fare NUMERIC NOT NULL,
    avg_fare NUMERIC NOT NULL,
    avg_distance NUMERIC NOT NULL,
    avg_trip_duration NUMERIC NOT NULL,
    avg_passenger_count NUMERIC NOT NULL,
    source_period TEXT NOT NULL,
    PRIMARY KEY (trip_hour)
);

CREATE TABLE IF NOT EXISTS warehouse.zone_daily_metrics (
    trip_date DATE NOT NULL,
    location_id INTEGER NOT NULL,
    trip_count BIGINT NOT NULL,
    total_fare NUMERIC NOT NULL,
    avg_distance NUMERIC NOT NULL,
    avg_duration NUMERIC NOT NULL,
    source_period TEXT NOT NULL,
    PRIMARY KEY (trip_date, location_id)
);

CREATE TABLE IF NOT EXISTS warehouse.weather_impact_metrics (
    weather_hour TIMESTAMPTZ NOT NULL,
    trip_count BIGINT NOT NULL,
    avg_trip_distance NUMERIC NOT NULL,
    avg_trip_duration NUMERIC NOT NULL,
    avg_total_amount NUMERIC NOT NULL,
    precipitation NUMERIC,
    temperature NUMERIC,
    wind_speed NUMERIC,
    source_period TEXT NOT NULL,
    PRIMARY KEY (weather_hour)
);

CREATE TABLE IF NOT EXISTS warehouse.daily_system_metrics (
    run_date DATE NOT NULL,
    rows_ingested BIGINT NOT NULL,
    rows_valid BIGINT NOT NULL,
    rows_rejected BIGINT NOT NULL,
    files_processed INTEGER NOT NULL,
    processing_time_seconds NUMERIC NOT NULL,
    PRIMARY KEY (run_date)
);

CREATE INDEX IF NOT EXISTS idx_zone_daily_metrics_location
    ON warehouse.zone_daily_metrics (location_id);
