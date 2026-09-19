from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from src.common.logging import get_logger
from src.config.settings import Settings, get_settings

logger = get_logger(__name__)


class BronzeTransformer:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.bronze_dir = self.settings.bronze_dir
        self.bronze_dir.mkdir(parents=True, exist_ok=True)

    def transform_trips(self, raw_path: Path, period: str) -> Path:
        target_dir = self.bronze_dir / "nyc_tlc_yellow" / f"period={period}"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / "trips.parquet"

        now_iso = datetime.now(UTC).isoformat()
        lf = pl.scan_parquet(raw_path)

        schema = lf.collect_schema()
        exprs: list[pl.Expr] = []

        # Typed conversion for known columns
        if "tpep_pickup_datetime" in schema:
            exprs.append(pl.col("tpep_pickup_datetime").cast(pl.Datetime))
        if "tpep_dropoff_datetime" in schema:
            exprs.append(pl.col("tpep_dropoff_datetime").cast(pl.Datetime))
        if "PULocationID" in schema:
            exprs.append(pl.col("PULocationID").cast(pl.Int64))
        if "DOLocationID" in schema:
            exprs.append(pl.col("DOLocationID").cast(pl.Int64))
        if "trip_distance" in schema:
            exprs.append(pl.col("trip_distance").cast(pl.Float64))
        if "passenger_count" in schema:
            exprs.append(pl.col("passenger_count").fill_null(1).cast(pl.Int64))
        if "fare_amount" in schema:
            exprs.append(pl.col("fare_amount").cast(pl.Float64))
        if "total_amount" in schema:
            exprs.append(pl.col("total_amount").cast(pl.Float64))

        # Include other columns if present
        for col_name in schema.names():
            if col_name not in [
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "PULocationID",
                "DOLocationID",
                "trip_distance",
                "passenger_count",
                "fare_amount",
                "total_amount",
            ]:
                exprs.append(pl.col(col_name))

        # Add lineage audit columns
        exprs.extend(
            [
                pl.lit(now_iso).alias("_ingested_at"),
                pl.lit(raw_path.name).alias("_source_file"),
                pl.lit("nyc_tlc_yellow").alias("_source_dataset"),
                pl.lit(period).alias("_period"),
            ]
        )

        df = lf.select(exprs).collect()
        df.write_parquet(target_path)
        logger.info("bronze_trips_built", path=str(target_path), row_count=len(df))
        return target_path

    def transform_zones(self, raw_path: Path) -> Path:
        target_dir = self.bronze_dir / "nyc_tlc_taxi_zone"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / "zones.parquet"

        now_iso = datetime.now(UTC).isoformat()
        df = pl.read_csv(raw_path)

        df_typed = df.select(
            [
                pl.col("LocationID").cast(pl.Int64),
                pl.col("Borough").cast(pl.Utf8),
                pl.col("Zone").cast(pl.Utf8),
                pl.col("service_zone").cast(pl.Utf8),
                pl.lit(now_iso).alias("_ingested_at"),
                pl.lit(raw_path.name).alias("_source_file"),
                pl.lit("nyc_tlc_taxi_zone").alias("_source_dataset"),
            ]
        )

        df_typed.write_parquet(target_path)
        logger.info("bronze_zones_built", path=str(target_path), row_count=len(df_typed))
        return target_path

    def transform_weather(self, raw_path: Path, period: str) -> Path:
        target_dir = self.bronze_dir / "noaa_ghcnh" / f"period={period}"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / "weather.parquet"

        now_iso = datetime.now(UTC).isoformat()
        lf = pl.scan_parquet(raw_path)
        schema = lf.collect_schema()

        exprs: list[pl.Expr] = []
        if "timestamp" in schema:
            exprs.append(pl.col("timestamp").cast(pl.Datetime))
        if "station_id" in schema:
            exprs.append(pl.col("station_id").cast(pl.Utf8))
        if "latitude" in schema:
            exprs.append(pl.col("latitude").cast(pl.Float64))
        if "longitude" in schema:
            exprs.append(pl.col("longitude").cast(pl.Float64))
        if "temperature" in schema:
            exprs.append(pl.col("temperature").cast(pl.Float64))
        if "dew_point" in schema:
            exprs.append(pl.col("dew_point").cast(pl.Float64))
        if "wind_speed" in schema:
            exprs.append(pl.col("wind_speed").cast(pl.Float64))
        if "precipitation" in schema:
            exprs.append(pl.col("precipitation").cast(pl.Float64))
        if "visibility" in schema:
            exprs.append(pl.col("visibility").cast(pl.Float64))
        if "pressure" in schema:
            exprs.append(pl.col("pressure").cast(pl.Float64))
        if "weather_condition" in schema:
            exprs.append(pl.col("weather_condition").cast(pl.Utf8))

        exprs.extend(
            [
                pl.lit(now_iso).alias("_ingested_at"),
                pl.lit(raw_path.name).alias("_source_file"),
                pl.lit("noaa_ghcnh").alias("_source_dataset"),
                pl.lit(period).alias("_period"),
            ]
        )

        df = lf.select(exprs).collect()
        df.write_parquet(target_path)
        logger.info("bronze_weather_built", path=str(target_path), row_count=len(df))
        return target_path
