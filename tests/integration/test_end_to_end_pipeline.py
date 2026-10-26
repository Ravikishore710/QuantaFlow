from datetime import datetime
from pathlib import Path

import duckdb
import polars as pl
import pytest

from src.monitoring.pipeline_monitor import PipelineMonitor
from src.sources.noaa_weather import NOAAWeatherSource
from src.sources.taxi_zone import TaxiZoneSource
from src.sources.tlc_taxi import TLCTaxiSource
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


def test_offline_e2e_pipeline_with_fixtures_and_network_block(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixtures_dir = Path(__file__).resolve().parent.parent / "fixtures"
    assert fixtures_dir.exists()

    monkeypatch.setenv("QUANTAFLOW_OFFLINE_FIXTURES_DIR", str(fixtures_dir))

    # Assert 100% offline: forbidden network calls
    def forbid_network(*args: object, **kwargs: object) -> None:
        raise RuntimeError("Prohibited live network call in offline test!")

    monkeypatch.setattr("requests.get", forbid_network)
    monkeypatch.setattr("urllib.request.urlopen", forbid_network)

    # Ingestion step from fixtures
    download_dir = tmp_path / "downloads"
    tlc_src = TLCTaxiSource()
    tlc_file = tlc_src.download(tlc_src.discover("2024-01"), download_dir)
    assert tlc_src.validate(tlc_file).passed is True

    zone_src = TaxiZoneSource()
    zone_file = zone_src.download(zone_src.discover("2024-01"), download_dir)
    assert zone_src.validate(zone_file).passed is True

    weather_src = NOAAWeatherSource()
    weather_file = weather_src.download(weather_src.discover("2024-01"), download_dir)
    assert weather_src.validate(weather_file).passed is True

    # Bronze
    bronze = BronzeTransformer()
    bronze.bronze_dir = tmp_path / "bronze"
    b_trips = bronze.transform_trips(tlc_file.local_path, "2024-01")
    b_zones = bronze.transform_zones(zone_file.local_path)
    b_weather = bronze.transform_weather(weather_file.local_path, "2024-01")

    # Silver
    quarantine = QuarantineStore(quarantine_dir=tmp_path / "quarantine")
    silver = SilverTransformer(quarantine_store=quarantine)
    silver.silver_dir = tmp_path / "silver"
    s_trips = silver.clean_and_transform_trips(b_trips, "2024-01")
    s_zones = silver.clean_and_transform_zones(b_zones)
    s_weather = silver.clean_and_transform_weather(b_weather, "2024-01")
    s_joined = silver.build_trip_weather_join(s_trips, s_weather, s_zones, "2024-01")

    # Data Quality & WAP Gate
    df_joined = pl.read_parquet(s_joined)
    dq = DataQualityFramework()
    dq.reports_dir = tmp_path / "reports"
    report = dq.evaluate_trips(df_joined, run_id="offline-run-e2e", period="2024-01")
    dq.enforce_wap_gate(report)
    assert report.overall_status == "passed"

    # Gold
    gold = GoldTransformer()
    gold.gold_dir = tmp_path / "gold"
    g_hourly = gold.build_hourly_trip_metrics(s_joined, "2024-01")
    g_zone = gold.build_zone_daily_metrics(s_joined, "2024-01")
    g_weather = gold.build_weather_impact_metrics(s_joined, "2024-01")
    g_system = gold.build_daily_system_metrics("2024-01", 50, len(df_joined), 0, 3, 2.5)

    # Warehouse
    db_file = tmp_path / "e2e_quantaflow.duckdb"
    loader = WarehouseLoader(engine_mode="duckdb", duckdb_path=db_file)
    loader.load_hourly_trip_metrics(g_hourly)
    loader.load_zone_daily_metrics(g_zone)
    loader.load_weather_impact_metrics(g_weather)
    loader.load_daily_system_metrics(g_system)

    # Monitor
    monitor = PipelineMonitor(engine_mode="duckdb", duckdb_path=db_file)
    monitor.start_pipeline_run("offline-run-e2e", "quantaflow_e2e", "2024-01")
    monitor.record_quality_results(report)
    monitor.finish_pipeline_run("offline-run-e2e", "success", 50, len(df_joined), 0)

    # Verify warehouse tables in DuckDB
    con = duckdb.connect(str(db_file))
    try:
        assert con.execute("SELECT COUNT(*) FROM warehouse.hourly_trip_metrics").fetchone()[0] > 0
        assert con.execute("SELECT COUNT(*) FROM warehouse.zone_daily_metrics").fetchone()[0] > 0
        res_weather = con.execute(
            "SELECT COUNT(*) FROM warehouse.weather_impact_metrics"
        ).fetchone()
        assert res_weather is not None and res_weather[0] > 0
        assert con.execute("SELECT COUNT(*) FROM warehouse.daily_system_metrics").fetchone()[0] > 0
        assert con.execute("SELECT COUNT(*) FROM monitoring.pipeline_runs").fetchone()[0] == 1
    finally:
        con.close()
