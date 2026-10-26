import argparse
import sys
import time
from pathlib import Path

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl  # noqa: E402

from src.common.error_tracking import capture_pipeline_exception, init_error_tracking
from src.common.errors import DataQualityGateError
from src.common.hashing import schema_hash, sha256_file
from src.common.logging import configure_logging, get_logger
from src.common.run_context import RunContext
from src.config.settings import get_settings
from src.ingestion.manifest import ManifestRecord, ManifestStore, utc_now_iso
from src.monitoring.pipeline_monitor import PipelineMonitor
from src.sources.noaa_weather import NOAAWeatherSource
from src.sources.taxi_zone import TaxiZoneSource
from src.sources.tlc_taxi import TLCTaxiSource
from src.storage.object_store import ObjectStore, raw_object_key
from src.transformation.bronze import BronzeTransformer
from src.transformation.gold import GoldTransformer
from src.transformation.quarantine import QuarantineStore
from src.transformation.silver import SilverTransformer
from src.validation.quality_framework import DataQualityFramework, send_quality_alert
from src.warehouse.loader import WarehouseLoader

logger = get_logger(__name__)


def run_pipeline(period: str, skip_upload: bool = False) -> int:
    configure_logging()
    settings = get_settings()
    init_error_tracking(settings.sentry_dsn, settings.environment, settings.pipeline_version)
    run_ctx = RunContext()
    start_time = time.time()

    monitor = PipelineMonitor(settings)
    monitor.start_pipeline_run(
        run_id=run_ctx.run_id,
        pipeline_name=run_ctx.pipeline_name,
        period=period,
    )

    logger.info("pipeline_started", run_id=run_ctx.run_id, period=period)

    manifest_store = ManifestStore(settings.manifest_dir, settings=settings)
    quarantine_store = QuarantineStore(settings.quarantine_dir, settings=settings)

    # -------------------------------------------------------------
    # 1. Ingestion: TLC Taxi
    # -------------------------------------------------------------
    tlc_source = TLCTaxiSource()
    tlc_disc = tlc_source.discover(period)
    tlc_down = tlc_source.download(tlc_disc, settings.sample_dir)
    tlc_hash = sha256_file(tlc_down.local_path)

    tlc_validation = tlc_source.validate(tlc_down)
    if not tlc_validation.passed:
        logger.error("tlc_validation_failed", errors=tlc_validation.errors)
        manifest_store.save(
            ManifestRecord(
                dataset=tlc_disc.dataset,
                period=period,
                source_url=tlc_disc.source_url,
                source_format=tlc_disc.source_format,
                downloaded_at=utc_now_iso(),
                sha256=tlc_hash,
                file_size_bytes=tlc_down.file_size_bytes,
                row_count=tlc_validation.row_count,
                schema_hash=schema_hash(tlc_validation.column_names, tlc_validation.column_types),
                pipeline_version=settings.pipeline_version,
                status="validation_failed",
            )
        )
        monitor.finish_pipeline_run(run_ctx.run_id, "failed", 0, 0, 0)
        return 1

    tlc_obj_path = "not_uploaded"
    if not skip_upload:
        try:
            store = ObjectStore(settings)
            store.ensure_bucket()
            key = raw_object_key(tlc_disc.dataset, period, tlc_down.local_path.name)
            tlc_obj_path = store.upload_file(tlc_down.local_path, key)
        except Exception as exc:
            logger.warn("minio_upload_skipped_or_failed", error=str(exc))

    manifest_store.save(
        ManifestRecord(
            dataset=tlc_disc.dataset,
            period=period,
            source_url=tlc_disc.source_url,
            source_format=tlc_disc.source_format,
            downloaded_at=utc_now_iso(),
            sha256=tlc_hash,
            file_size_bytes=tlc_down.file_size_bytes,
            row_count=tlc_validation.row_count,
            schema_hash=schema_hash(tlc_validation.column_names, tlc_validation.column_types),
            pipeline_version=settings.pipeline_version,
            status="validated",
            object_path=tlc_obj_path,
        )
    )

    # -------------------------------------------------------------
    # 2. Ingestion: Taxi Zones
    # -------------------------------------------------------------
    zone_source = TaxiZoneSource()
    zone_disc = zone_source.discover(period)
    zone_down = zone_source.download(zone_disc, settings.sample_dir)
    zone_hash = sha256_file(zone_down.local_path)
    zone_val = zone_source.validate(zone_down)

    manifest_store.save(
        ManifestRecord(
            dataset=zone_disc.dataset,
            period=period,
            source_url=zone_disc.source_url,
            source_format=zone_disc.source_format,
            downloaded_at=utc_now_iso(),
            sha256=zone_hash,
            file_size_bytes=zone_down.file_size_bytes,
            row_count=zone_val.row_count,
            schema_hash=schema_hash(zone_val.column_names, zone_val.column_types),
            pipeline_version=settings.pipeline_version,
            status="validated" if zone_val.passed else "validation_failed",
        )
    )

    # -------------------------------------------------------------
    # 3. Ingestion: NOAA Weather
    # -------------------------------------------------------------
    weather_source = NOAAWeatherSource(station_id=settings.weather_station_id)
    weather_disc = weather_source.discover(period)
    weather_down = weather_source.download(weather_disc, settings.sample_dir)
    weather_hash = sha256_file(weather_down.local_path)
    weather_val = weather_source.validate(weather_down)

    manifest_store.save(
        ManifestRecord(
            dataset=weather_disc.dataset,
            period=period,
            source_url=weather_disc.source_url,
            source_format=weather_disc.source_format,
            downloaded_at=utc_now_iso(),
            sha256=weather_hash,
            file_size_bytes=weather_down.file_size_bytes,
            row_count=weather_val.row_count,
            schema_hash=schema_hash(weather_val.column_names, weather_val.column_types),
            pipeline_version=settings.pipeline_version,
            status="validated" if weather_val.passed else "validation_failed",
        )
    )

    # -------------------------------------------------------------
    # 4. Bronze Transformation
    # -------------------------------------------------------------
    bronze = BronzeTransformer(settings)
    bronze_trips = bronze.transform_trips(tlc_down.local_path, period)
    bronze_zones = bronze.transform_zones(zone_down.local_path)
    bronze_weather = bronze.transform_weather(weather_down.local_path, period)

    # -------------------------------------------------------------
    # 5. Silver Transformation
    # -------------------------------------------------------------
    silver = SilverTransformer(settings, quarantine_store=quarantine_store)
    silver_trips = silver.clean_and_transform_trips(bronze_trips, period)
    silver_zones = silver.clean_and_transform_zones(bronze_zones)
    silver_weather = silver.clean_and_transform_weather(bronze_weather, period)

    silver_joined = silver.build_trip_weather_join(
        silver_trips_path=silver_trips,
        silver_weather_path=silver_weather,
        silver_zones_path=silver_zones,
        period=period,
    )

    # -------------------------------------------------------------
    # 6. Data Quality Framework Evaluation
    # -------------------------------------------------------------
    silver_trips_df = pl.read_parquet(silver_trips)
    dq = DataQualityFramework(settings)
    report = dq.evaluate_trips(silver_trips_df, run_id=run_ctx.run_id, period=period)
    monitor.record_quality_results(report)

    rejected_rows = quarantine_store.get_rejected_count("nyc_tlc_yellow", period)
    rows_valid = len(silver_trips_df)
    rows_ingested = tlc_validation.row_count

    try:
        dq.enforce_wap_gate(report)
    except DataQualityGateError as gate_err:
        capture_pipeline_exception(
            gate_err, {"run_id": run_ctx.run_id, "period": period, "layer": "wap_gate"}
        )
        logger.error(
            "pipeline_wap_gate_tripped",
            run_id=run_ctx.run_id,
            error=str(gate_err),
            failed_checks=gate_err.failed_checks,
        )
        send_quality_alert(report.to_dict(), settings.alert_webhook_url)
        monitor.finish_pipeline_run(
            run_id=run_ctx.run_id,
            status="failed",
            rows_in=rows_ingested,
            rows_out=rows_valid,
            rows_rejected=rejected_rows,
        )
        return 1

    # -------------------------------------------------------------
    # 7. Gold Transformation
    # -------------------------------------------------------------
    gold = GoldTransformer(settings)
    gold_hourly = gold.build_hourly_trip_metrics(silver_joined, period)
    gold_zone = gold.build_zone_daily_metrics(silver_joined, period)
    gold_weather = gold.build_weather_impact_metrics(silver_joined, period)

    duration = time.time() - start_time
    rejected_rows = quarantine_store.get_rejected_count("nyc_tlc_yellow", period)
    rows_valid = len(silver_trips_df)
    rows_ingested = tlc_validation.row_count

    gold_daily = gold.build_daily_system_metrics(
        period=period,
        rows_ingested=rows_ingested,
        rows_valid=rows_valid,
        rows_rejected=rejected_rows,
        files_processed=3,
        processing_time_seconds=round(duration, 2),
    )

    # -------------------------------------------------------------
    # 8. Warehouse Load
    # -------------------------------------------------------------
    loader = WarehouseLoader(settings)
    loader.load_hourly_trip_metrics(gold_hourly)
    loader.load_zone_daily_metrics(gold_zone)
    loader.load_weather_impact_metrics(gold_weather)
    loader.load_daily_system_metrics(gold_daily)

    # -------------------------------------------------------------
    # 9. Finalize Run Monitoring
    # -------------------------------------------------------------
    status = "success" if report.overall_status == "passed" else "warning"
    monitor.finish_pipeline_run(
        run_id=run_ctx.run_id,
        status=status,
        rows_in=rows_ingested,
        rows_out=rows_valid,
        rows_rejected=rejected_rows,
    )

    logger.info(
        "pipeline_completed_successfully",
        run_id=run_ctx.run_id,
        period=period,
        duration_seconds=round(duration, 2),
        rows_valid=rows_valid,
        rows_rejected=rejected_rows,
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Execute QuantaFlow end-to-end data pipeline")
    parser.add_argument("--period", default="2024-01", help="Period in YYYY-MM format")
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        default=False,
        help="Skip MinIO/S3 upload and run locally",
    )
    args = parser.parse_args()
    sys.exit(run_pipeline(args.period, skip_upload=args.skip_upload))


if __name__ == "__main__":
    main()
