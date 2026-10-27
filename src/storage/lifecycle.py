"""Storage partition compaction, retention pruning, and disk telemetry."""

import contextlib
import os
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

from src.common.logging import get_logger
from src.config.settings import Settings, get_settings

logger = get_logger(__name__)


class StorageLifecycleManager:
    """Manages partition compaction and retention lifecycle policies for the Medallion lakehouse."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    def compact_partition(
        self,
        partition_dir: Path,
        output_filename: str = "compacted.parquet",
    ) -> Path:
        """Merges multiple parquet chunks into a single consolidated parquet file."""
        if not partition_dir.is_dir():
            raise FileNotFoundError(f"Partition directory does not exist: {partition_dir}")

        parquet_files = sorted(
            [p for p in partition_dir.glob("*.parquet") if p.name != output_filename]
        )
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files to compact in {partition_dir}")

        dfs = [pl.read_parquet(f) for f in parquet_files]
        total_rows = sum(len(df) for df in dfs)
        merged_df = pl.concat(dfs, how="vertical")

        if len(merged_df) != total_rows:
            raise ValueError(
                f"Compacted row count mismatch: expected {total_rows}, got {len(merged_df)}"
            )

        temp_target = partition_dir / f".tmp_{output_filename}"
        final_target = partition_dir / output_filename
        merged_df.write_parquet(temp_target, compression="zstd")
        temp_target.replace(final_target)

        # Remove source chunk files if distinct from output
        for f in parquet_files:
            if f != final_target:
                f.unlink(missing_ok=True)

        logger.info(
            "partition_compacted",
            partition=str(partition_dir),
            source_files=len(parquet_files),
            total_rows=total_rows,
            output_file=str(final_target),
        )
        return final_target

    def prune_expired_partitions(
        self,
        layer_dir: Path,
        retention_days: int,
        dry_run: bool = False,
    ) -> list[Path]:
        """Prunes subdirectories or files in layer_dir older than retention_days."""
        if not layer_dir.exists():
            return []

        cutoff = datetime.now(UTC) - timedelta(days=retention_days)
        pruned: list[Path] = []

        for entry in layer_dir.iterdir():
            mtime = datetime.fromtimestamp(entry.stat().st_mtime, tz=UTC)
            if mtime < cutoff:
                pruned.append(entry)
                if not dry_run:
                    if entry.is_dir():
                        shutil.rmtree(entry, ignore_errors=True)
                    else:
                        entry.unlink(missing_ok=True)

        logger.info(
            "expired_partitions_pruned",
            layer=str(layer_dir),
            retention_days=retention_days,
            pruned_count=len(pruned),
            dry_run=dry_run,
        )
        return pruned

    def get_storage_stats(self) -> dict[str, dict[str, int]]:
        """Calculates total file count and size in bytes across all Medallion layers."""
        layers = {
            "bronze": self.settings.bronze_dir,
            "silver": self.settings.silver_dir,
            "gold": self.settings.gold_dir,
            "quarantine": self.settings.quarantine_dir,
            "manifests": self.settings.manifest_dir,
            "reports": self.settings.reports_dir,
        }

        stats: dict[str, dict[str, int]] = {}
        for name, path in layers.items():
            file_count = 0
            total_bytes = 0
            if path.exists():
                for root, _, files in os.walk(path):
                    for f in files:
                        file_count += 1
                        file_path = Path(root) / f
                        with contextlib.suppress(OSError):
                            total_bytes += file_path.stat().st_size
            stats[name] = {"file_count": file_count, "size_bytes": total_bytes}

        return stats
