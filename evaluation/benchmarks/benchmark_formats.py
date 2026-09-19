import time
from pathlib import Path
from typing import Any

import polars as pl


def run_benchmark_formats(sample_size: int = 100_000) -> dict[str, Any]:
    """Benchmark format efficiency: Parquet vs CSV (scan time, file size)."""
    tmp_dir = Path("evaluation/results/tmp_benchmarks")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = tmp_dir / "bench_sample.parquet"
    csv_path = tmp_dir / "bench_sample.csv"

    # Generate synthetic dataset
    df = pl.DataFrame(
        {
            "id": list(range(sample_size)),
            "fare_amount": [12.5 + (i % 50) for i in range(sample_size)],
            "trip_distance": [1.5 + ((i % 100) / 10.0) for i in range(sample_size)],
            "passenger_count": [1 + (i % 4) for i in range(sample_size)],
            "PULocationID": [1 + (i % 263) for i in range(sample_size)],
            "DOLocationID": [1 + ((i * 3) % 263) for i in range(sample_size)],
            "pickup_datetime": ["2024-01-01T12:00:00"] * sample_size,
        }
    )

    # Write formats
    df.write_parquet(parquet_path)
    df.write_csv(csv_path)

    parquet_size_bytes = parquet_path.stat().st_size
    csv_size_bytes = csv_path.stat().st_size

    # Measure Parquet scan & aggregation time
    t0 = time.perf_counter()
    _ = (
        pl.scan_parquet(parquet_path)
        .group_by("PULocationID")
        .agg(pl.col("fare_amount").mean())
        .collect()
    )
    parquet_scan_seconds = time.perf_counter() - t0

    # Measure CSV scan & aggregation time
    t0 = time.perf_counter()
    _ = (
        pl.scan_csv(csv_path).group_by("PULocationID").agg(pl.col("fare_amount").mean()).collect()
    )
    csv_scan_seconds = time.perf_counter() - t0

    compression_ratio = round(csv_size_bytes / max(parquet_size_bytes, 1), 2)
    speedup = round(csv_scan_seconds / max(parquet_scan_seconds, 0.0001), 2)

    return {
        "benchmark": "formats_parquet_vs_csv",
        "sample_size": sample_size,
        "parquet_size_bytes": parquet_size_bytes,
        "csv_size_bytes": csv_size_bytes,
        "compression_ratio": compression_ratio,
        "parquet_scan_seconds": round(parquet_scan_seconds, 4),
        "csv_scan_seconds": round(csv_scan_seconds, 4),
        "speedup_factor": speedup,
    }
