from datetime import datetime
from pathlib import Path

import polars as pl

from src.transformation.bronze import BronzeTransformer
from src.transformation.gold import GoldTransformer
from src.transformation.quarantine import QuarantineStore
from src.transformation.silver import SilverTransformer
from src.validation.quality_framework import DataQualityFramework
from src.warehouse.loader import WarehouseLoader


def test_full_pipeline_flow(tmp_path: Path) -> None:
    # 1. Setup local fixture files
    sample_dir = tmp_path / "samples"
    sample_dir.mkdir(parents=True)

    trips_parquet = sample_dir / "yellow_trip_2024_01.parquet"
    pl.DataFrame(
        {
            "tpep_pickup_datetime": [
                datetime(2024, 1, 1, 10, 0, 0),
                datetime(2024, 1, 1, 10, 30, 0),
            ],
            "tpep_dropoff_datetime": [
                datetime(2024, 1, 1, 10, 15, 0),
                datetime(2024, 1, 1, 10, 45, 0),
            ],
            "PULocationID": [1, 2],
            "DOLocationID": [2, 1],
            "trip_distance": [1.5, 2.8],
            "passenger_count": [1, 2],
            "fare_amount": [10.0, 15.0],
            "total_amount": [12.0, 18.0],
        }
    ).write_parquet(trips_parquet)

    zones_csv = sample_dir / "taxi_zone_lookup.csv"
    zones_csv.write_text(
        "LocationID,Borough,Zone,service_zone\n"
        "1,Manhattan,Central Park,Yellow Zone\n"
        "2,Queens,JFK Airport,Airports\n"
    )

    weather_parquet = sample_dir / "ghcnh_weather.parquet"
    pl.DataFrame(
        {
            "timestamp": [datetime(2024, 1, 1, 10, 0, 0)],
            "station_id": ["USW00094728"],
            "latitude": [40.78],
            "longitude": [-73.96],
            "temperature": [4.5],
            "dew_point": [-1.0],
            "wind_speed": [3.2],
            "precipitation": [0.0],
            "visibility": [10.0],
            "pressure": [1013.2],
            "weather_condition": ["Clear"],
        }
    ).write_parquet(weather_parquet)

    # 2. Bronze transformation
    bronze = BronzeTransformer()
    bronze.bronze_dir = tmp_path / "bronze"

    b_trips = bronze.transform_trips(trips_parquet, "2024-01")
    b_zones = bronze.transform_zones(zones_csv)
    b_weather = bronze.transform_weather(weather_parquet, "2024-01")

    assert b_trips.exists()
    assert b_zones.exists()
    assert b_weather.exists()

    # 3. Silver transformation
    quarantine = QuarantineStore(quarantine_dir=tmp_path / "quarantine")
    silver = SilverTransformer(quarantine_store=quarantine)
    silver.silver_dir = tmp_path / "silver"

    s_trips = silver.clean_and_transform_trips(b_trips, "2024-01")
    s_zones = silver.clean_and_transform_zones(b_zones)
    s_weather = silver.clean_and_transform_weather(b_weather, "2024-01")

    s_joined = silver.build_trip_weather_join(s_trips, s_weather, s_zones, "2024-01")
    assert s_joined.exists()

    df_joined = pl.read_parquet(s_joined)
    assert len(df_joined) == 2
    assert "pickup_borough" in df_joined.columns
    assert "temperature" in df_joined.columns

    # 4. Quality evaluation
    dq = DataQualityFramework()
    dq.reports_dir = tmp_path / "reports"
    report = dq.evaluate_trips(df_joined, run_id="test-run-e2e", period="2024-01")
    assert report.overall_status == "passed"

    # 5. Gold transformation
    gold = GoldTransformer()
    gold.gold_dir = tmp_path / "gold"

    g_hourly = gold.build_hourly_trip_metrics(s_joined, "2024-01")
    g_zone = gold.build_zone_daily_metrics(s_joined, "2024-01")
    g_weather = gold.build_weather_impact_metrics(s_joined, "2024-01")
    g_system = gold.build_daily_system_metrics("2024-01", 2, 2, 0, 3, 1.25)

    assert g_hourly.exists()
    assert g_zone.exists()
    assert g_weather.exists()
    assert g_system.exists()

    # 6. Warehouse load into DuckDB
    loader = WarehouseLoader()
    loader.local_db_path = tmp_path / "quantaflow.duckdb"

    assert loader.load_hourly_trip_metrics(g_hourly) > 0
    assert loader.load_zone_daily_metrics(g_zone) > 0
    assert loader.load_weather_impact_metrics(g_weather) > 0
    assert loader.load_daily_system_metrics(g_system) > 0
