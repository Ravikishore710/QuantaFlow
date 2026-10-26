import os
import shutil
from pathlib import Path

import polars as pl
import requests

from src.common.logging import get_logger
from src.sources.base import DiscoveredFile, DownloadedFile, SourceConnector, ValidationResult
from src.validation.schema_validator import load_contract, validate_columns

logger = get_logger(__name__)

TAXI_ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
REQUIRED_COLUMNS = ["LocationID", "Borough", "Zone", "service_zone"]


class TaxiZoneSource(SourceConnector):
    dataset_name = "nyc_tlc_taxi_zone"

    def discover(self, period: str) -> DiscoveredFile:
        return DiscoveredFile(
            dataset=self.dataset_name,
            period=period,
            source_url=TAXI_ZONE_URL,
            source_format="csv",
        )

    def download(
        self,
        discovered: DiscoveredFile,
        destination_dir: Path,
        fixture_path: Path | None = None,
    ) -> DownloadedFile:
        destination_dir.mkdir(parents=True, exist_ok=True)
        local_path = destination_dir / "taxi_zone_lookup.csv"

        if local_path.exists() and local_path.stat().st_size > 0:
            file_size_bytes = local_path.stat().st_size
            logger.info("file_already_present", target=str(local_path), size_bytes=file_size_bytes)
            return DownloadedFile(
                discovered=discovered, local_path=local_path, file_size_bytes=file_size_bytes
            )

        resolved_fixture = fixture_path
        if not resolved_fixture:
            env_fixtures = os.environ.get("QUANTAFLOW_OFFLINE_FIXTURES_DIR")
            if env_fixtures:
                cand = Path(env_fixtures) / "taxi_zone_lookup.csv"
                if cand.exists():
                    resolved_fixture = cand

        if resolved_fixture and resolved_fixture.exists():
            shutil.copyfile(resolved_fixture, local_path)
            file_size_bytes = local_path.stat().st_size
            logger.info("fixture_copied", fixture=str(resolved_fixture), target=str(local_path))
            return DownloadedFile(
                discovered=discovered, local_path=local_path, file_size_bytes=file_size_bytes
            )

        logger.info("download_started", url=discovered.source_url, target=str(local_path))
        response = requests.get(discovered.source_url, timeout=30)
        response.raise_for_status()
        local_path.write_bytes(response.content)

        file_size_bytes = local_path.stat().st_size
        logger.info("download_completed", target=str(local_path), size_bytes=file_size_bytes)
        return DownloadedFile(
            discovered=discovered, local_path=local_path, file_size_bytes=file_size_bytes
        )

    def validate(self, downloaded: DownloadedFile) -> ValidationResult:
        errors: list[str] = []
        try:
            frame = pl.read_csv(downloaded.local_path)
        except Exception as exc:
            return ValidationResult(
                passed=False, row_count=0, column_names=[], column_types=[], errors=[str(exc)]
            )

        column_names = frame.columns
        column_types = [str(dtype) for dtype in frame.dtypes]

        contract = load_contract("src/contracts/taxi_zone_schema.yaml")
        errors.extend(validate_columns(column_names, contract))

        if "LocationID" in frame.columns and frame["LocationID"].n_unique() != len(frame):
            errors.append("LocationID values are not unique")

        row_count = len(frame)
        if row_count == 0:
            errors.append("zone lookup file contains zero rows")

        passed = len(errors) == 0
        return ValidationResult(
            passed=passed,
            row_count=row_count,
            column_names=column_names,
            column_types=column_types,
            errors=errors,
        )
