from pathlib import Path

import polars as pl
import requests

from src.common.logging import get_logger
from src.sources.base import DiscoveredFile, DownloadedFile, SourceConnector, ValidationResult
from src.validation.schema_validator import load_contract, validate_columns

logger = get_logger(__name__)

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
REQUIRED_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "passenger_count",
    "fare_amount",
    "total_amount",
]


class TLCTaxiSource(SourceConnector):
    dataset_name = "nyc_tlc_yellow"

    def discover(self, period: str) -> DiscoveredFile:
        year, month = period.split("-")
        file_name = f"yellow_tripdata_{year}-{month}.parquet"
        source_url = f"{TLC_BASE_URL}/{file_name}"
        return DiscoveredFile(
            dataset=self.dataset_name,
            period=period,
            source_url=source_url,
            source_format="parquet",
        )

    def download(self, discovered: DiscoveredFile, destination_dir: Path) -> DownloadedFile:
        destination_dir.mkdir(parents=True, exist_ok=True)
        file_name = f"yellow_trip_{discovered.period.replace('-', '_')}.parquet"
        local_path = destination_dir / file_name

        if local_path.exists() and local_path.stat().st_size > 0:
            file_size_bytes = local_path.stat().st_size
            logger.info("file_already_present", target=str(local_path), size_bytes=file_size_bytes)
            return DownloadedFile(
                discovered=discovered, local_path=local_path, file_size_bytes=file_size_bytes
            )

        logger.info("download_started", url=discovered.source_url, target=str(local_path))
        with requests.get(discovered.source_url, stream=True, timeout=60) as response:
            response.raise_for_status()
            with local_path.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                    handle.write(chunk)

        file_size_bytes = local_path.stat().st_size
        logger.info("download_completed", target=str(local_path), size_bytes=file_size_bytes)
        return DownloadedFile(
            discovered=discovered, local_path=local_path, file_size_bytes=file_size_bytes
        )

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

        contract = load_contract("src/contracts/tlc_trip_schema.yaml")
        errors.extend(validate_columns(column_names, contract))

        row_count = lazy_frame.select(pl.len()).collect().item()
        if row_count == 0:
            errors.append("file contains zero rows")

        passed = len(errors) == 0
        return ValidationResult(
            passed=passed,
            row_count=row_count,
            column_names=column_names,
            column_types=column_types,
            errors=errors,
        )
