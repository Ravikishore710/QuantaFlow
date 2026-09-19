import io
from pathlib import Path

import polars as pl
import requests

from src.common.logging import get_logger
from src.sources.base import DiscoveredFile, DownloadedFile, SourceConnector, ValidationResult
from src.validation.schema_validator import load_contract, validate_columns

logger = get_logger(__name__)

NOAA_S3_BASE_URL = "https://noaa-ghcnh-pds.s3.amazonaws.com/hourly/access/by-year"
DEFAULT_STATION_ID = "USW00094728"  # Central Park, NYC


class NOAAWeatherSource(SourceConnector):
    dataset_name = "noaa_ghcnh"

    def __init__(self, station_id: str = DEFAULT_STATION_ID) -> None:
        self.station_id = station_id

    def discover(self, period: str) -> DiscoveredFile:
        year, _ = period.split("-")
        source_url = f"{NOAA_S3_BASE_URL}/{year}/parquet/GHCNh_{self.station_id}_{year}.parquet"
        return DiscoveredFile(
            dataset=self.dataset_name,
            period=period,
            source_url=source_url,
            source_format="parquet",
        )

    def download(self, discovered: DiscoveredFile, destination_dir: Path) -> DownloadedFile:
        destination_dir.mkdir(parents=True, exist_ok=True)
        year, month = discovered.period.split("-")
        local_path = destination_dir / f"ghcnh_{self.station_id}_{year}_{month}.parquet"

        if local_path.exists() and local_path.stat().st_size > 0:
            file_size_bytes = local_path.stat().st_size
            logger.info("file_already_present", target=str(local_path), size_bytes=file_size_bytes)
            return DownloadedFile(
                discovered=discovered, local_path=local_path, file_size_bytes=file_size_bytes
            )

        logger.info(
            "noaa_download_started",
            url=discovered.source_url,
            station=self.station_id,
            period=discovered.period,
            target=str(local_path),
        )

        response = requests.get(discovered.source_url, timeout=60)
        response.raise_for_status()

        # Filter the annual parquet file to the requested month to keep files modular
        annual_df = pl.read_parquet(io.BytesIO(response.content))

        # Determine column names for year/month/station
        filtered_df = annual_df
        if "Year" in annual_df.columns and "Month" in annual_df.columns:
            filtered_df = annual_df.filter(
                (pl.col("Year").cast(pl.Utf8) == year)
                & (pl.col("Month").cast(pl.Utf8).str.zfill(2) == month)
            )
        elif "DATE" in annual_df.columns:
            filtered_df = annual_df.filter(pl.col("DATE").str.starts_with(discovered.period))

        # Standardize columns to match weather_schema contract
        standardized_df = self._standardize_schema(filtered_df)
        standardized_df.write_parquet(local_path)

        file_size_bytes = local_path.stat().st_size
        logger.info(
            "noaa_download_completed",
            target=str(local_path),
            size_bytes=file_size_bytes,
            rows=len(standardized_df),
        )
        return DownloadedFile(
            discovered=discovered, local_path=local_path, file_size_bytes=file_size_bytes
        )

    def _standardize_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        """Map raw NOAA GHCNh parquet columns to the data contract schema."""
        exprs: list[pl.Expr] = []

        # timestamp
        if "DATE" in df.columns:
            exprs.append(
                pl.col("DATE")
                .str.to_datetime("%Y-%m-%dT%H:%M:%S")
                .dt.replace_time_zone("UTC")
                .alias("timestamp")
            )
        elif "timestamp" in df.columns:
            exprs.append(pl.col("timestamp"))

        # station_id
        if "STATION" in df.columns:
            exprs.append(pl.col("STATION").cast(pl.Utf8).alias("station_id"))
        elif "station_id" in df.columns:
            exprs.append(pl.col("station_id").cast(pl.Utf8))
        else:
            exprs.append(pl.lit(self.station_id).alias("station_id"))

        # latitude / longitude
        if "LATITUDE" in df.columns:
            exprs.append(pl.col("LATITUDE").cast(pl.Float64).alias("latitude"))
        elif "latitude" in df.columns:
            exprs.append(pl.col("latitude").cast(pl.Float64))

        if "LONGITUDE" in df.columns:
            exprs.append(pl.col("LONGITUDE").cast(pl.Float64).alias("longitude"))
        elif "longitude" in df.columns:
            exprs.append(pl.col("longitude").cast(pl.Float64))

        # temperature
        if "temperature" in df.columns:
            exprs.append(pl.col("temperature").cast(pl.Float64).alias("temperature"))

        # dew_point
        if "dew_point_temperature" in df.columns:
            exprs.append(pl.col("dew_point_temperature").cast(pl.Float64).alias("dew_point"))
        elif "dew_point" in df.columns:
            exprs.append(pl.col("dew_point").cast(pl.Float64))

        # wind_speed
        if "wind_speed" in df.columns:
            exprs.append(pl.col("wind_speed").cast(pl.Float64).alias("wind_speed"))

        # precipitation
        if "precipitation" in df.columns:
            exprs.append(pl.col("precipitation").cast(pl.Float64).alias("precipitation"))

        # visibility
        if "visibility" in df.columns:
            exprs.append(pl.col("visibility").cast(pl.Float64).alias("visibility"))

        # pressure
        if "station_level_pressure" in df.columns:
            exprs.append(pl.col("station_level_pressure").cast(pl.Float64).alias("pressure"))
        elif "pressure" in df.columns:
            exprs.append(pl.col("pressure").cast(pl.Float64))

        # weather_condition
        if "sky_condition" in df.columns:
            exprs.append(pl.col("sky_condition").cast(pl.Utf8).alias("weather_condition"))
        elif "weather_condition" in df.columns:
            exprs.append(pl.col("weather_condition").cast(pl.Utf8))

        return df.select(exprs)

    def validate(self, downloaded: DownloadedFile) -> ValidationResult:
        errors: list[str] = []
        try:
            lazy_frame = pl.scan_parquet(downloaded.local_path)
            schema = lazy_frame.collect_schema()
            column_names = list(schema.names())
            column_types = [str(dtype) for dtype in schema.dtypes()]
        except Exception as exc:
            return ValidationResult(
                passed=False, row_count=0, column_names=[], column_types=[], errors=[str(exc)]
            )

        contract = load_contract("src/contracts/weather_schema.yaml")
        errors.extend(validate_columns(column_names, contract, allow_unexpected=True))

        row_count = lazy_frame.select(pl.len()).collect().item()
        if row_count == 0:
            errors.append("weather file contains zero rows")

        passed = len(errors) == 0
        return ValidationResult(
            passed=passed,
            row_count=row_count,
            column_names=column_names,
            column_types=column_types,
            errors=errors,
        )
