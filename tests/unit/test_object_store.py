from pathlib import Path
from unittest.mock import MagicMock

from src.config.settings import Settings
from src.storage.object_store import ObjectStore, raw_object_key


def test_raw_object_key() -> None:
    key = raw_object_key("nyc_tlc_yellow", "2024-01", "trips.parquet")
    assert key == "raw/nyc_tlc_yellow/period=2024-01/trips.parquet"


def test_object_store_local_fallback(tmp_path: Path) -> None:
    settings = Settings(
        minio_endpoint="http://invalid.host:9000",
        minio_bucket="test-bucket",
    )
    store = ObjectStore(settings=settings)
    store.client = None
    store.local_raw_dir = tmp_path / "raw"

    sample_file = tmp_path / "sample.txt"
    sample_file.write_text("sample content", encoding="utf-8")

    key = raw_object_key("nyc_tlc_yellow", "2024-01", "sample.txt")
    uri = store.upload_file(sample_file, key)

    assert uri.startswith("file://")
    assert store.object_exists(key) is True
    assert store.object_exists("nonexistent/key.txt") is False


def test_object_store_s3_mocked(tmp_path: Path) -> None:
    settings = Settings(
        minio_endpoint="http://localhost:9000",
        minio_bucket="test-bucket",
    )
    store = ObjectStore(settings=settings)

    mock_client = MagicMock()
    mock_client.list_buckets.return_value = {"Buckets": []}
    mock_client.create_bucket.return_value = {}
    mock_client.upload_file.return_value = None
    mock_client.head_object.return_value = {"ContentLength": 100}

    store.client = mock_client

    store.ensure_bucket()
    assert mock_client.create_bucket.called

    sample_file = tmp_path / "sample.txt"
    sample_file.write_text("sample content", encoding="utf-8")

    key = "raw/test.txt"
    uri = store.upload_file(sample_file, key)
    assert uri == "s3://test-bucket/raw/test.txt"
    assert store.object_exists(key) is True
