from pathlib import Path
from unittest.mock import patch

import polars as pl

from src.common.hashing import sha256_file
from src.ingestion.manifest import ManifestRecord, ManifestStore, utc_now_iso
from src.sources.base import DownloadedFile
from src.sources.tlc_taxi import TLCTaxiSource


def build_sample_parquet(path: Path) -> None:
    frame = pl.DataFrame(
        {
            "tpep_pickup_datetime": ["2024-01-01T00:00:00"] * 5,
            "tpep_dropoff_datetime": ["2024-01-01T00:15:00"] * 5,
            "PULocationID": [1, 2, 3, 4, 5],
            "DOLocationID": [1, 2, 3, 4, 5],
            "trip_distance": [1.5] * 5,
            "passenger_count": [1] * 5,
            "fare_amount": [10.0] * 5,
            "total_amount": [12.5] * 5,
        }
    )
    frame.write_parquet(path)


def test_second_run_is_skipped_when_manifest_already_validated(tmp_path: Path) -> None:
    sample_path = tmp_path / "yellow_trip_2024_01.parquet"
    build_sample_parquet(sample_path)
    checksum = sha256_file(sample_path)

    manifest_store = ManifestStore(tmp_path / "manifests")
    manifest_store.save(
        ManifestRecord(
            dataset="nyc_tlc_yellow",
            period="2024-01",
            source_url="https://example.com/yellow_tripdata_2024-01.parquet",
            source_format="parquet",
            downloaded_at=utc_now_iso(),
            sha256=checksum,
            file_size_bytes=sample_path.stat().st_size,
            row_count=5,
            schema_hash="irrelevant-for-this-test",
            pipeline_version="0.1.0",
            status="validated",
        )
    )

    source = TLCTaxiSource()
    discovered = source.discover("2024-01")

    with patch.object(TLCTaxiSource, "download") as mocked_download:
        mocked_download.return_value = DownloadedFile(
            discovered=discovered,
            local_path=sample_path,
            file_size_bytes=sample_path.stat().st_size,
        )
        downloaded = source.download(discovered, tmp_path)
        recomputed_checksum = sha256_file(downloaded.local_path)

        already_processed = manifest_store.exists("nyc_tlc_yellow", "2024-01", recomputed_checksum)

    assert already_processed is True
