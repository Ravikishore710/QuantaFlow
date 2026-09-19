import time
from pathlib import Path
from typing import Any

import polars as pl


def run_benchmark_execution(sample_size: int = 150_000) -> dict[str, Any]:
    """Benchmark execution engine: Eager Polars vs Lazy Polars pipeline optimization."""
    tmp_dir = Path("evaluation/results/tmp_benchmarks")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = tmp_dir / "bench_exec.parquet"

    df = pl.DataFrame(
        {
            "tpep_pickup_datetime": ["2024-01-01T00:00:00"] * sample_size,
            "tpep_dropoff_datetime": ["2024-01-01T00:15:00"] * sample_size,
            "PULocationID": [1 + (i % 263) for i in range(sample_size)],
            "DOLocationID": [1 + ((i * 2) % 263) for i in range(sample_size)],
            "trip_distance": [0.5 + ((i % 50) / 10.0) for i in range(sample_size)],
            "fare_amount": [5.0 + (i % 80) for i in range(sample_size)],
            "passenger_count": [1 + (i % 5) for i in range(sample_size)],
            "total_amount": [7.0 + (i % 90) for i in range(sample_size)],
        }
    )
    df.write_parquet(parquet_path)

    # 1. Eager execution
    t0 = time.perf_counter()
    eager_df = pl.read_parquet(parquet_path)
    eager_filtered = eager_df.filter(pl.col("trip_distance") > 2.0)
    _ = eager_filtered.group_by("PULocationID").agg(
        [
            pl.col("fare_amount").mean().alias("avg_fare"),
            pl.col("trip_distance").sum().alias("sum_dist"),
        ]
    )
    eager_seconds = time.perf_counter() - t0

    # 2. Lazy execution (predicate & projection pushdown)
    t0 = time.perf_counter()
    lazy_agg = (
        pl.scan_parquet(parquet_path)
        .filter(pl.col("trip_distance") > 2.0)
        .group_by("PULocationID")
        .agg(
            [
                pl.col("fare_amount").mean().alias("avg_fare"),
                pl.col("trip_distance").sum().alias("sum_dist"),
            ]
        )
        .collect()
    )
    lazy_seconds = time.perf_counter() - t0

    speedup = round(eager_seconds / max(lazy_seconds, 0.0001), 2)

    return {
        "benchmark": "execution_eager_vs_lazy",
        "sample_size": sample_size,
        "eager_seconds": round(eager_seconds, 4),
        "lazy_seconds": round(lazy_seconds, 4),
        "speedup_factor": speedup,
        "rows_output": len(lazy_agg),
    }
