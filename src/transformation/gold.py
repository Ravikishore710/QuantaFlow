from datetime import date
from pathlib import Path

import polars as pl

from src.common.logging import get_logger
from src.config.settings import Settings, get_settings

logger = get_logger(__name__)


class GoldTransformer:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.gold_dir = self.settings.gold_dir
        self.gold_dir.mkdir(parents=True, exist_ok=True)

    def build_hourly_trip_metrics(self, silver_trip_weather_path: Path, period: str) -> Path:
        target_dir = self.gold_dir / "hourly_trip_metrics" / f"period={period}"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / "hourly_trip_metrics.parquet"

        lf = pl.scan_parquet(silver_trip_weather_path)
        agg_df = (
            lf.group_by("pickup_hour")
            .agg(
                [
                    pl.len().alias("trip_count"),
                    pl.col("trip_distance").sum().alias("total_distance"),
                    pl.col("fare_amount").sum().alias("total_fare"),
                    pl.col("fare_amount").mean().alias("avg_fare"),
                    pl.col("trip_distance").mean().alias("avg_distance"),
                    pl.col("trip_duration_seconds").mean().alias("avg_trip_duration"),
                    pl.col("passenger_count").mean().alias("avg_passenger_count"),
                ]
            )
            .with_columns(
                [
                    pl.col("pickup_hour").alias("trip_hour"),
                    pl.lit(period).alias("source_period"),
                ]
            )
            .select(
                [
                    "trip_hour",
                    "trip_count",
                    "total_distance",
                    "total_fare",
                    "avg_fare",
                    "avg_distance",
                    "avg_trip_duration",
                    "avg_passenger_count",
                    "source_period",
                ]
            )
            .sort("trip_hour")
            .collect()
        )

        agg_df.write_parquet(target_path)
        logger.info("gold_hourly_trip_metrics_built", path=str(target_path), row_count=len(agg_df))
        return target_path

    def build_zone_daily_metrics(self, silver_trip_weather_path: Path, period: str) -> Path:
        target_dir = self.gold_dir / "zone_daily_metrics" / f"period={period}"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / "zone_daily_metrics.parquet"

        lf = pl.scan_parquet(silver_trip_weather_path)
        agg_df = (
            lf.group_by(["pickup_date", "PULocationID"])
            .agg(
                [
                    pl.len().alias("trip_count"),
                    pl.col("fare_amount").sum().alias("total_fare"),
                    pl.col("trip_distance").mean().alias("avg_distance"),
                    pl.col("trip_duration_seconds").mean().alias("avg_duration"),
                ]
            )
            .with_columns(
                [
                    pl.col("pickup_date").alias("trip_date"),
                    pl.col("PULocationID").alias("location_id"),
                    pl.lit(period).alias("source_period"),
                ]
            )
            .select(
                [
                    "trip_date",
                    "location_id",
                    "trip_count",
                    "total_fare",
                    "avg_distance",
                    "avg_duration",
                    "source_period",
                ]
            )
            .sort(["trip_date", "location_id"])
            .collect()
        )

        agg_df.write_parquet(target_path)
        logger.info("gold_zone_daily_metrics_built", path=str(target_path), row_count=len(agg_df))
        return target_path

    def build_weather_impact_metrics(self, silver_trip_weather_path: Path, period: str) -> Path:
        target_dir = self.gold_dir / "weather_impact_metrics" / f"period={period}"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / "weather_impact_metrics.parquet"

        lf = pl.scan_parquet(silver_trip_weather_path)
        agg_df = (
            lf.group_by("pickup_hour")
            .agg(
                [
                    pl.len().alias("trip_count"),
                    pl.col("trip_distance").mean().alias("avg_trip_distance"),
                    pl.col("trip_duration_seconds").mean().alias("avg_trip_duration"),
                    pl.col("total_amount").mean().alias("avg_total_amount"),
                    pl.col("precipitation").mean().alias("precipitation"),
                    pl.col("temperature").mean().alias("temperature"),
                    pl.col("wind_speed").mean().alias("wind_speed"),
                ]
            )
            .with_columns(
                [
                    pl.col("pickup_hour").alias("weather_hour"),
                    pl.lit(period).alias("source_period"),
                ]
            )
            .select(
                [
                    "weather_hour",
                    "trip_count",
                    "avg_trip_distance",
                    "avg_trip_duration",
                    "avg_total_amount",
                    "precipitation",
                    "temperature",
                    "wind_speed",
                    "source_period",
                ]
            )
            .sort("weather_hour")
            .collect()
        )

        agg_df.write_parquet(target_path)
        logger.info(
            "gold_weather_impact_metrics_built", path=str(target_path), row_count=len(agg_df)
        )
        return target_path

    def build_daily_system_metrics(
        self,
        period: str,
        rows_ingested: int,
        rows_valid: int,
        rows_rejected: int,
        files_processed: int,
        processing_time_seconds: float,
    ) -> Path:
        target_dir = self.gold_dir / "daily_system_metrics" / f"period={period}"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / "daily_system_metrics.parquet"

        today = date.today()
        df = pl.DataFrame(
            {
                "run_date": [today],
                "rows_ingested": [rows_ingested],
                "rows_valid": [rows_valid],
                "rows_rejected": [rows_rejected],
                "files_processed": [files_processed],
                "processing_time_seconds": [processing_time_seconds],
            }
        )

        df.write_parquet(target_path)
        logger.info("gold_daily_system_metrics_built", path=str(target_path), row_count=len(df))
        return target_path
