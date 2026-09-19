from pathlib import Path

from src.ingestion.manifest import ManifestRecord, ManifestStore, utc_now_iso


def make_record(sha256: str = "abc123", status: str = "validated") -> ManifestRecord:
    return ManifestRecord(
        dataset="nyc_tlc_yellow",
        period="2024-01",
        source_url="https://example.com/yellow_tripdata_2024-01.parquet",
        source_format="parquet",
        downloaded_at=utc_now_iso(),
        sha256=sha256,
        file_size_bytes=1024,
        row_count=100,
        schema_hash="schema-hash-value",
        pipeline_version="0.1.0",
        status=status,
    )


def test_manifest_round_trip(tmp_path: Path) -> None:
    store = ManifestStore(tmp_path)
    record = make_record()

    store.save(record)
    loaded = store.load("nyc_tlc_yellow", "2024-01")

    assert loaded == record


def test_manifest_exists_is_idempotent(tmp_path: Path) -> None:
    store = ManifestStore(tmp_path)
    record = make_record()
    store.save(record)

    assert store.exists("nyc_tlc_yellow", "2024-01", "abc123") is True
    assert store.exists("nyc_tlc_yellow", "2024-01", "different-hash") is False
    assert store.exists("nyc_tlc_yellow", "2024-02", "abc123") is False


def test_manifest_failed_validation_is_not_treated_as_processed(tmp_path: Path) -> None:
    store = ManifestStore(tmp_path)
    record = make_record(status="validation_failed")
    store.save(record)

    assert store.exists("nyc_tlc_yellow", "2024-01", "abc123") is False
