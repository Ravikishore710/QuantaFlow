"""Unit tests verifying StorageLifecycleManager partition compaction and retention policies."""

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from src.config.settings import Settings
from src.storage.lifecycle import StorageLifecycleManager


@pytest.fixture
def test_settings(tmp_path: Path) -> Settings:
    return Settings(
        bronze_dir=tmp_path / "bronze",
        silver_dir=tmp_path / "silver",
        gold_dir=tmp_path / "gold",
        quarantine_dir=tmp_path / "quarantine",
        manifest_dir=tmp_path / "manifests",
        reports_dir=tmp_path / "reports",
    )


def test_compact_partition_merges_chunks_and_verifies_row_count(
    test_settings: Settings,
) -> None:
    """Verify compaction consolidates multi-part parquet files and preserves total records."""
    partition_dir = test_settings.silver_dir / "trips" / "year=2024" / "month=01"
    partition_dir.mkdir(parents=True, exist_ok=True)

    part1 = partition_dir / "part-001.parquet"
    part2 = partition_dir / "part-002.parquet"
    part3 = partition_dir / "part-003.parquet"

    pl.DataFrame({"id": [1, 2], "val": ["a", "b"]}).write_parquet(part1)
    pl.DataFrame({"id": [3, 4, 5], "val": ["c", "d", "e"]}).write_parquet(part2)
    pl.DataFrame({"id": [6], "val": ["f"]}).write_parquet(part3)

    mgr = StorageLifecycleManager(test_settings)
    compacted_file = mgr.compact_partition(partition_dir, output_filename="trips.parquet")

    assert compacted_file.is_file()
    assert compacted_file.name == "trips.parquet"

    df = pl.read_parquet(compacted_file)
    assert len(df) == 6
    assert df["id"].to_list() == [1, 2, 3, 4, 5, 6]

    # Chunk files must be removed
    assert not part1.exists()
    assert not part2.exists()
    assert not part3.exists()


def test_compact_partition_error_handling(test_settings: Settings) -> None:
    """Verify proper FileNotFoundError when directory or parquet files are missing."""
    mgr = StorageLifecycleManager(test_settings)
    missing_dir = test_settings.silver_dir / "nonexistent"

    with pytest.raises(FileNotFoundError):
        mgr.compact_partition(missing_dir)

    empty_dir = test_settings.silver_dir / "empty"
    empty_dir.mkdir(parents=True, exist_ok=True)

    with pytest.raises(FileNotFoundError):
        mgr.compact_partition(empty_dir)


def test_prune_expired_partitions_retention_and_dry_run(test_settings: Settings) -> None:
    """Verify retention pruning cleans directories beyond TTL and respects dry-run mode."""
    quarantine_dir = test_settings.quarantine_dir
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    old_folder = quarantine_dir / "date=2023-01-01"
    old_folder.mkdir()
    (old_folder / "bad.parquet").write_bytes(b"bad data")

    new_folder = quarantine_dir / "date=2024-01-01"
    new_folder.mkdir()
    (new_folder / "recent.parquet").write_bytes(b"recent data")

    # Set old_folder modification time to 40 days ago
    past_ts = (datetime.now(UTC) - timedelta(days=40)).timestamp()
    os.utime(old_folder, (past_ts, past_ts))

    mgr = StorageLifecycleManager(test_settings)

    # 1. Dry run should list old_folder without deleting it
    dry_pruned = mgr.prune_expired_partitions(quarantine_dir, retention_days=30, dry_run=True)
    assert len(dry_pruned) == 1
    assert dry_pruned[0] == old_folder
    assert old_folder.exists()

    # 2. Live execution should prune old_folder and preserve new_folder
    live_pruned = mgr.prune_expired_partitions(quarantine_dir, retention_days=30, dry_run=False)
    assert len(live_pruned) == 1
    assert not old_folder.exists()
    assert new_folder.exists()


def test_get_storage_stats_telemetry(test_settings: Settings) -> None:
    """Verify storage disk usage and file counts are computed across Medallion layers."""
    bronze_part = test_settings.bronze_dir / "test"
    bronze_part.mkdir(parents=True, exist_ok=True)
    (bronze_part / "file1.parquet").write_bytes(b"12345")
    (bronze_part / "file2.parquet").write_bytes(b"67890")

    mgr = StorageLifecycleManager(test_settings)
    stats = mgr.get_storage_stats()

    assert "bronze" in stats
    assert stats["bronze"]["file_count"] == 2
    assert stats["bronze"]["size_bytes"] == 10
    assert stats["silver"]["file_count"] == 0
