import time
from pathlib import Path
from typing import Any

import polars as pl


def run_benchmark_partitioning(
    num_partitions: int = 6, rows_per_part: int = 50_000
) -> dict[str, Any]:
    """Benchmark partitioned scan vs unpartitioned full scan."""
    tmp_dir = Path("evaluation/results/tmp_benchmarks")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    single_file = tmp_dir / "unpartitioned.parquet"
    part_dir = tmp_dir / "partitioned"
    part_dir.mkdir(parents=True, exist_ok=True)

    all_dfs = []
    for month in range(1, num_partitions + 1):
        month_str = f"{month:02d}"
        part_path = part_dir / f"month={month_str}"
        part_path.mkdir(parents=True, exist_ok=True)

        m_df = pl.DataFrame(
            {
                "month": [month_str] * rows_per_part,
                "fare_amount": [10.0 + (i % 50) for i in range(rows_per_part)],
                "trip_distance": [1.0 + (i % 20) for i in range(rows_per_part)],
                "PULocationID": [1 + (i % 200) for i in range(rows_per_part)],
            }
        )
        m_df.write_parquet(part_path / "data.parquet")
        all_dfs.append(m_df)

    # Combined single file
    pl.concat(all_dfs).write_parquet(single_file)

    # 1. Unpartitioned scan with filter
    t0 = time.perf_counter()
    _ = (
        pl.scan_parquet(single_file)
        .filter(pl.col("month") == "03")
        .select(pl.col("fare_amount").sum())
        .collect()
    )
    unpart_seconds = time.perf_counter() - t0

    # 2. Partitioned scan targeting specific partition directly
    t0 = time.perf_counter()
    target_part_file = part_dir / "month=03" / "data.parquet"
    _ = pl.scan_parquet(target_part_file).select(pl.col("fare_amount").sum()).collect()
    part_seconds = time.perf_counter() - t0

    speedup = round(unpart_seconds / max(part_seconds, 0.0001), 2)

    return {
        "benchmark": "partition_pruning",
        "total_rows": num_partitions * rows_per_part,
        "target_partition_rows": rows_per_part,
        "unpartitioned_scan_seconds": round(unpart_seconds, 4),
        "partitioned_scan_seconds": round(part_seconds, 4),
        "speedup_factor": speedup,
    }
