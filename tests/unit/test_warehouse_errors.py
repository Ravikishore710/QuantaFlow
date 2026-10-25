from pathlib import Path
from unittest.mock import patch

import psycopg2
import pytest

from src.common.errors import (
    ManifestPersistenceError,
    QuantaFlowError,
    WarehouseConnectionError,
)
from src.ingestion.manifest import ManifestRecord, ManifestStore, utc_now_iso
from src.warehouse.loader import WarehouseLoader


def test_quantaflow_error_inheritance() -> None:
    assert issubclass(WarehouseConnectionError, QuantaFlowError)
    assert issubclass(WarehouseConnectionError, ConnectionError)
    assert issubclass(ManifestPersistenceError, QuantaFlowError)
    assert issubclass(ManifestPersistenceError, OSError)


def test_postgres_strict_mode_raises_warehouse_connection_error(tmp_path: Path) -> None:
    parquet_path = tmp_path / "dummy.parquet"
    import polars as pl
    pl.DataFrame({"trip_hour": [1]}).write_parquet(parquet_path)

    loader = WarehouseLoader(engine_mode="postgres")
    with (
        patch("psycopg2.connect", side_effect=psycopg2.OperationalError("Connection refused")),
        pytest.raises(WarehouseConnectionError, match="PostgreSQL production warehouse connection failed"),
    ):
        loader.load_hourly_trip_metrics(parquet_path)


def test_duckdb_connect_failure_raises_warehouse_connection_error(tmp_path: Path) -> None:
    parquet_path = tmp_path / "dummy.parquet"
    import polars as pl
    pl.DataFrame({"trip_hour": [1]}).write_parquet(parquet_path)

    # Directory where file expected causes duckdb connect failure
    invalid_db_path = tmp_path / "nonexistent_dir" / "subdir" / "db.duckdb"
    loader = WarehouseLoader(engine_mode="duckdb", duckdb_path=invalid_db_path)
    # Remove directory to induce duckdb IO/connection error if needed
    with patch("duckdb.connect", side_effect=RuntimeError("DuckDB lock collision")):
        with pytest.raises(WarehouseConnectionError, match="DuckDB warehouse connection failed"):
            loader.load_hourly_trip_metrics(parquet_path)


def test_manifest_store_save_raises_manifest_persistence_error(tmp_path: Path) -> None:
    store = ManifestStore(manifest_dir=tmp_path / "manifests")
    record = ManifestRecord(
        dataset="nyc_tlc_yellow",
        period="2024-01",
        source_url="https://example.com/test.parquet",
        source_format="parquet",
        downloaded_at=utc_now_iso(),
        sha256="dummy_sha",
        file_size_bytes=100,
        row_count=10,
        schema_hash="dummy_hash",
        pipeline_version="0.1.0",
        status="validated",
    )
    with patch("pathlib.Path.write_text", side_effect=OSError("Read-only file system")):
        with pytest.raises(ManifestPersistenceError, match="Failed to persist manifest record"):
            store.save(record)


def test_manifest_store_load_corrupt_file_raises_manifest_persistence_error(tmp_path: Path) -> None:
    manifest_dir = tmp_path / "manifests"
    dataset_dir = manifest_dir / "nyc_tlc_yellow"
    dataset_dir.mkdir(parents=True)
    bad_file = dataset_dir / "2024-01.json"
    bad_file.write_text("NOT_VALID_JSON{{{", encoding="utf-8")

    store = ManifestStore(manifest_dir=manifest_dir)
    with pytest.raises(ManifestPersistenceError, match="Failed to read manifest record"):
        store.load("nyc_tlc_yellow", "2024-01")
