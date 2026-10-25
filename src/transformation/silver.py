from pathlib import Path

import polars as pl

from src.common.logging import get_logger
from src.config.settings import Settings, get_settings
from src.transformation.quarantine import QuarantineStore

logger = get_logger(__name__)


class SilverTransformer:
    def __init__(
        self,
        settings: Settings | None = None,
        quarantine_store: QuarantineStore | None = None,
        max_trip_distance: float = 500.0,
        max_fare_amount: float = 10000.0,
        max_trip_duration_minutes: float = 1440.0,
    ) -> None:
        self.settings = settings or get_settings()
        self.silver_dir = self.settings.silver_dir
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        self.quarantine_store = quarantine_store or QuarantineStore(settings=self.settings)
        self.max_trip_distance = max_trip_distance
        self.max_fare_amount = max_fare_amount
        self.max_trip_duration_minutes = max_trip_duration_minutes

    def clean_and_transform_trips(self, bronze_trips_path: Path, period: str) -> Path:
        """Applies cleaning rules to bronze trips:
        - Non-negative distance (trip_distance >= 0.0)
        - Non-negative fare (fare_amount >= 0.0)
        - Non-negative total (total_amount >= 0.0)
        - Consistency check (total_amount >= fare_amount)
        - Chronological timestamps (tpep_dropoff_datetime >= tpep_pickup_datetime)
        - Non-null location IDs (PULocationID, DOLocationID)
        - Configurable upper-bound physics checks:
          * trip_distance <= max_trip_distance (default: 500.0 miles)
          * fare_amount <= max_fare_amount (default: $10,000.0)
          * trip_duration_minutes <= max_trip_duration_minutes (default: 1440.0 mins / 24 hrs)

        Routes violating records to QuarantineStore (dead-letter queue).
        Deduplicates on compound business keys and generates derived fields.
        """
        year, month = period.split("-")
        target_dir = self.silver_dir / "trips" / f"year={year}" / f"month={month}"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / "trips.parquet"

        df = pl.read_parquet(bronze_trips_path)
        initial_count = len(df)

        duration_seconds_expr = (
            (
                pl.col("tpep_dropoff_datetime").cast(pl.Int64)
                - pl.col("tpep_pickup_datetime").cast(pl.Int64)
            )
            / 1_000_000
        )
        duration_minutes_expr = duration_seconds_expr / 60.0

        # 1. Base validation filter: identify rule violations (lower bounds, chronological, nulls)
        base_valid_condition = (
            (pl.col("trip_distance") >= 0.0)
            & (pl.col("fare_amount") >= 0.0)
            & (pl.col("total_amount") >= 0.0)
            & (pl.col("total_amount") >= pl.col("fare_amount"))
            & (pl.col("tpep_dropoff_datetime") >= pl.col("tpep_pickup_datetime"))
            & (pl.col("PULocationID").is_not_null())
            & (pl.col("DOLocationID").is_not_null())
        )

        invalid_base_df = df.filter(~base_valid_condition)
        if len(invalid_base_df) > 0:
            self.quarantine_store.route_records(
                dataset="nyc_tlc_yellow",
                period=period,
                stage="silver_trip_cleaning",
                rejected_rows=invalid_base_df.to_dicts(),
                reason="failed_validation_rules",
            )

        # 2. Physics outlier validation: upper bounds on distance, fare, and duration
        base_passed_df = df.filter(base_valid_condition)
        physics_valid_condition = (
            (pl.col("trip_distance") <= self.max_trip_distance)
            & (pl.col("fare_amount") <= self.max_fare_amount)
            & (duration_minutes_expr <= self.max_trip_duration_minutes)
        )

        physics_outliers_df = base_passed_df.filter(~physics_valid_condition)
        if len(physics_outliers_df) > 0:
            self.quarantine_store.route_records(
                dataset="nyc_tlc_yellow",
                period=period,
                stage="silver_trip_cleaning",
                rejected_rows=physics_outliers_df.to_dicts(),
                reason="physics_outlier_violation",
            )

        valid_df = base_passed_df.filter(physics_valid_condition)

        # 3. Deduplication
        dedup_keys = [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "trip_distance",
            "fare_amount",
        ]
        available_dedup_keys = [k for k in dedup_keys if k in valid_df.columns]
        cleaned_df = valid_df.unique(subset=available_dedup_keys)

        # 4. Derived fields
        cleaned_df = cleaned_df.with_columns(
            [
                pl.col("tpep_pickup_datetime").dt.date().alias("pickup_date"),
                pl.col("tpep_pickup_datetime").dt.truncate("1h").alias("pickup_hour"),
                duration_seconds_expr.alias("trip_duration_seconds"),
            ]
        )

        cleaned_df.write_parquet(target_path)
        logger.info(
            "silver_trips_built",
            path=str(target_path),
            in_rows=initial_count,
            out_rows=len(cleaned_df),
            rejected_rows=initial_count - len(cleaned_df),
        )
        return target_path

    def clean_and_transform_zones(self, bronze_zones_path: Path) -> Path:
        target_dir = self.silver_dir / "zones"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / "zones.parquet"

        df = pl.read_parquet(bronze_zones_path)
        cleaned_df = df.filter(pl.col("LocationID").is_not_null()).unique(subset=["LocationID"])

        cleaned_df.write_parquet(target_path)
        logger.info("silver_zones_built", path=str(target_path), row_count=len(cleaned_df))
        return target_path

    def clean_and_transform_weather(self, bronze_weather_path: Path, period: str) -> Path:
        year, month = period.split("-")
        target_dir = self.silver_dir / "weather" / f"year={year}" / f"month={month}"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / "weather.parquet"

        df = pl.read_parquet(bronze_weather_path)
        cleaned_df = (
            df.filter(pl.col("timestamp").is_not_null())
            .unique(subset=["timestamp", "station_id"])
            .with_columns(
                [
                    pl.col("timestamp").dt.truncate("1h").alias("weather_hour"),
                ]
            )
        )

        cleaned_df.write_parquet(target_path)
        logger.info("silver_weather_built", path=str(target_path), row_count=len(cleaned_df))
        return target_path

    def build_trip_weather_join(
        self,
        silver_trips_path: Path,
        silver_weather_path: Path,
        silver_zones_path: Path | None,
        period: str,
    ) -> Path:
        """Join silver trips with silver weather and reference zones on pickup_hour."""
        year, month = period.split("-")
        target_dir = self.silver_dir / "trip_weather" / f"year={year}" / f"month={month}"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / "trip_weather.parquet"

        trips_lf = pl.scan_parquet(silver_trips_path)
        weather_lf = pl.scan_parquet(silver_weather_path)

        # Average weather observations by hour in case multiple reports exist
        weather_agg = weather_lf.group_by("weather_hour").agg(
            [
                pl.col("temperature").mean().alias("temperature"),
                pl.col("dew_point").mean().alias("dew_point"),
                pl.col("wind_speed").mean().alias("wind_speed"),
                pl.col("precipitation").max().alias("precipitation"),
                pl.col("visibility").mean().alias("visibility"),
            ]
        )

        joined_lf = trips_lf.join(
            weather_agg,
            left_on="pickup_hour",
            right_on="weather_hour",
            how="left",
        )

        if silver_zones_path and silver_zones_path.exists():
            zones_lf = pl.scan_parquet(silver_zones_path)
            # Pickup zone
            joined_lf = joined_lf.join(
                zones_lf.select(
                    [
                        pl.col("LocationID").alias("PULocationID"),
                        pl.col("Borough").alias("pickup_borough"),
                        pl.col("Zone").alias("pickup_zone"),
                    ]
                ),
                on="PULocationID",
                how="left",
            )
            # Dropoff zone
            joined_lf = joined_lf.join(
                zones_lf.select(
                    [
                        pl.col("LocationID").alias("DOLocationID"),
                        pl.col("Borough").alias("dropoff_borough"),
                        pl.col("Zone").alias("dropoff_zone"),
                    ]
                ),
                on="DOLocationID",
                how="left",
            )

        df = joined_lf.collect()
        df.write_parquet(target_path)
        logger.info(
            "silver_trip_weather_built",
            path=str(target_path),
            row_count=len(df),
        )
        return target_path
