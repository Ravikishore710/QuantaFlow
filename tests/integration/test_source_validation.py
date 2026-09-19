from pathlib import Path

import polars as pl

from src.sources.base import DiscoveredFile, DownloadedFile
from src.sources.tlc_taxi import TLCTaxiSource


def build_sample_parquet(path: Path, row_count: int = 10) -> None:
    frame = pl.DataFrame(
        {
            "tpep_pickup_datetime": ["2024-01-01T00:00:00"] * row_count,
            "tpep_dropoff_datetime": ["2024-01-01T00:15:00"] * row_count,
            "PULocationID": list(range(1, row_count + 1)),
            "DOLocationID": list(range(1, row_count + 1)),
            "trip_distance": [1.5] * row_count,
            "passenger_count": [1] * row_count,
            "fare_amount": [10.0] * row_count,
            "total_amount": [12.5] * row_count,
        }
    )
    frame.write_parquet(path)


def test_tlc_validate_passes_for_conformant_sample(tmp_path: Path) -> None:
    sample_path = tmp_path / "yellow_trip_2024_01.parquet"
    build_sample_parquet(sample_path)

    source = TLCTaxiSource()
    discovered = DiscoveredFile(
        dataset="nyc_tlc_yellow", period="2024-01", source_url="local", source_format="parquet"
    )
    downloaded = DownloadedFile(
        discovered=discovered, local_path=sample_path, file_size_bytes=sample_path.stat().st_size
    )

    result = source.validate(downloaded)

    assert result.passed is True
    assert result.row_count == 10
    assert "trip_distance" in result.column_names


def test_tlc_validate_fails_when_required_column_missing(tmp_path: Path) -> None:
    sample_path = tmp_path / "yellow_trip_2024_02.parquet"
    frame = pl.DataFrame({"tpep_pickup_datetime": ["2024-02-01T00:00:00"]})
    frame.write_parquet(sample_path)

    source = TLCTaxiSource()
    discovered = DiscoveredFile(
        dataset="nyc_tlc_yellow", period="2024-02", source_url="local", source_format="parquet"
    )
    downloaded = DownloadedFile(
        discovered=discovered, local_path=sample_path, file_size_bytes=sample_path.stat().st_size
    )

    result = source.validate(downloaded)

    assert result.passed is False
    assert any("missing required columns" in error for error in result.errors)
