"""Modular execution callables corresponding to Airflow DAG stages."""

from pathlib import Path
from typing import Any

import polars as pl

from src.common.logging import get_logger
from src.config.settings import Settings, get_settings
from src.lineage.tracker import LineageGraph
from src.sources.noaa_weather import NOAAWeatherSource
from src.sources.taxi_zone import TaxiZoneSource
from src.sources.tlc_taxi import TLCTaxiSource
from src.transformation.bronze import BronzeTransformer
from src.transformation.gold import GoldTransformer
from src.transformation.quarantine import QuarantineStore
from src.transformation.silver import SilverTransformer
from src.validation.ingress import PipelineExecutionRequest
from src.validation.quality_framework import DataQualityFramework
from src.warehouse.loader import WarehouseLoader

logger = get_logger(__name__)


def get_bronze_trips_path(settings: Settings, period: str) -> Path:
    return settings.bronze_dir / "nyc_tlc_yellow" / f"period={period}" / "trips.parquet"


def get_bronze_zones_path(settings: Settings) -> Path:
    return settings.bronze_dir / "nyc_tlc_taxi_zone" / "zones.parquet"


def get_bronze_weather_path(settings: Settings, period: str) -> Path:
    return settings.bronze_dir / "noaa_ghcnh" / f"period={period}" / "weather.parquet"


def get_silver_trips_path(settings: Settings, period: str) -> Path:
    year, month = period.split("-")
    return settings.silver_dir / "trips" / f"year={year}" / f"month={month}" / "trips.parquet"


def get_silver_joined_path(settings: Settings, period: str) -> Path:
    year, month = period.split("-")
    base = settings.silver_dir / "trip_weather" / f"year={year}" / f"month={month}"
    return base / "trip_weather.parquet"


def get_gold_mart_path(settings: Settings, mart_name: str, period: str) -> Path:
    return settings.gold_dir / mart_name / f"period={period}" / f"{mart_name}.parquet"


def validate_ingress_task(period: str, skip_upload: bool = True) -> dict[str, Any]:
    """Stage 1: Validate execution boundaries via Pydantic ingress schema."""
    req = PipelineExecutionRequest(period=period, skip_upload=skip_upload)
    return {"status": "validated", "period": req.period, "skip_upload": req.skip_upload}


def ingest_bronze_task(period: str, settings: Settings | None = None) -> dict[str, Any]:
    """Stage 2: Download raw multi-source datasets and partition into Bronze layer."""
    settings = settings or get_settings()
    bronze = BronzeTransformer(settings)

    tlc_src = TLCTaxiSource()
    tlc_disc = tlc_src.discover(period)
    tlc_down = tlc_src.download(tlc_disc, settings.sample_dir)
    bronze_taxi_path = bronze.transform_trips(tlc_down.local_path, period=period)

    zone_src = TaxiZoneSource()
    zone_disc = zone_src.discover(period)
    zone_down = zone_src.download(zone_disc, settings.sample_dir)
    bronze_zones_path = bronze.transform_zones(zone_down.local_path)

    weather_src = NOAAWeatherSource(station_id=settings.weather_station_id)
    weather_disc = weather_src.discover(period)
    weather_down = weather_src.download(weather_disc, settings.sample_dir)
    bronze_weather_path = bronze.transform_weather(weather_down.local_path, period=period)

    t_rows = len(pl.read_parquet(bronze_taxi_path))
    z_rows = len(pl.read_parquet(bronze_zones_path))
    w_rows = len(pl.read_parquet(bronze_weather_path))

    return {
        "status": "ingested",
        "period": period,
        "bronze_taxi_path": str(bronze_taxi_path),
        "bronze_zones_path": str(bronze_zones_path),
        "bronze_weather_path": str(bronze_weather_path),
        "bronze_taxi_rows": t_rows,
        "bronze_zone_rows": z_rows,
        "bronze_weather_rows": w_rows,
    }


def transform_silver_task(period: str, settings: Settings | None = None) -> dict[str, Any]:
    """Stage 3: Apply physics cleaning rules, quarantine DLQ routing, and enrichment."""
    settings = settings or get_settings()
    quarantine = QuarantineStore(settings.quarantine_dir)
    silver = SilverTransformer(settings, quarantine_store=quarantine)

    bronze_taxi_path = get_bronze_trips_path(settings, period)
    bronze_zones_path = get_bronze_zones_path(settings)
    bronze_weather_path = get_bronze_weather_path(settings, period)

    silver_trips = silver.clean_and_transform_trips(bronze_taxi_path, period=period)
    silver_zones = silver.clean_and_transform_zones(bronze_zones_path)
    silver_weather = silver.clean_and_transform_weather(bronze_weather_path, period=period)

    silver_joined = silver.build_trip_weather_join(
        silver_trips_path=silver_trips,
        silver_weather_path=silver_weather,
        silver_zones_path=silver_zones,
        period=period,
    )

    clean_rows = len(pl.read_parquet(silver_trips))

    return {
        "status": "transformed",
        "period": period,
        "silver_trips_path": str(silver_trips),
        "silver_joined_path": str(silver_joined),
        "clean_rows": clean_rows,
    }


def evaluate_wap_quality_gate_task(
    period: str, run_id: str = "airflow-run", settings: Settings | None = None
) -> dict[str, Any]:
    """Stage 4: Write-Audit-Publish (WAP) circuit breaker gate."""
    settings = settings or get_settings()
    dq = DataQualityFramework(settings)

    silver_trips_path = get_silver_trips_path(settings, period)
    silver_df = pl.read_parquet(silver_trips_path)
    report = dq.evaluate_trips(silver_df, run_id=run_id, period=period)
    dq.enforce_wap_gate(report)

    return {
        "status": "passed",
        "period": period,
        "overall_status": report.overall_status,
        "passed_checks": report.passed_checks,
        "failed_checks": report.failed_checks,
    }


def build_gold_marts_task(period: str, settings: Settings | None = None) -> dict[str, Any]:
    """Stage 5: Aggregate analytical metrics and write Gold marts."""
    settings = settings or get_settings()
    gold = GoldTransformer(settings)

    joined_path = get_silver_joined_path(settings, period)
    hourly_path = gold.build_hourly_trip_metrics(joined_path, period)
    zone_path = gold.build_zone_daily_metrics(joined_path, period)
    weather_path = gold.build_weather_impact_metrics(joined_path, period)

    return {
        "status": "marts_built",
        "period": period,
        "hourly_path": str(hourly_path),
        "zone_path": str(zone_path),
        "weather_path": str(weather_path),
    }


def load_warehouse_task(period: str, settings: Settings | None = None) -> dict[str, Any]:
    """Stage 6: Upsert Gold analytical marts into warehouse database."""
    settings = settings or get_settings()
    loader = WarehouseLoader(settings)

    hourly_path = get_gold_mart_path(settings, "hourly_trip_metrics", period)
    zone_path = get_gold_mart_path(settings, "zone_daily_metrics", period)
    weather_path = get_gold_mart_path(settings, "weather_impact_metrics", period)

    h_rows = loader.load_hourly_trip_metrics(hourly_path)
    z_rows = loader.load_zone_daily_metrics(zone_path)
    w_rows = loader.load_weather_impact_metrics(weather_path)

    return {
        "status": "loaded",
        "period": period,
        "hourly_loaded": h_rows,
        "zone_loaded": z_rows,
        "weather_loaded": w_rows,
    }


def emit_lineage_task(
    period: str, run_id: str = "airflow-run", settings: Settings | None = None
) -> dict[str, Any]:
    """Stage 7: Generate and record OpenLineage JSON event."""
    settings = settings or get_settings()
    graph = LineageGraph()
    artifact = graph.export_openlineage_event(
        run_id=run_id,
        period=period,
        output_dir=settings.reports_dir,
    )
    return {"status": "lineage_emitted", "period": period, "artifact": str(artifact)}
