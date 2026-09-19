import csv
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from evaluation.benchmarks.benchmark_execution import run_benchmark_execution
from evaluation.benchmarks.benchmark_formats import run_benchmark_formats
from evaluation.benchmarks.benchmark_partitioning import run_benchmark_partitioning


def main() -> None:
    print("Running QuantaFlow performance benchmarks...")

    results_dir = Path("evaluation/results")
    results_dir.mkdir(parents=True, exist_ok=True)
    csv_file = results_dir / "pipeline_benchmarks.csv"

    r1 = run_benchmark_formats()
    print("Format benchmark:", r1)

    r2 = run_benchmark_execution()
    print("Execution benchmark:", r2)

    r3 = run_benchmark_partitioning()
    print("Partitioning benchmark:", r3)

    fieldnames = [
        "benchmark_name",
        "description",
        "baseline_seconds",
        "optimized_seconds",
        "speedup_factor",
        "notes",
    ]

    rows = [
        {
            "benchmark_name": "Parquet vs CSV Scan",
            "description": f"Scan & aggregate {r1['sample_size']} rows",
            "baseline_seconds": r1["csv_scan_seconds"],
            "optimized_seconds": r1["parquet_scan_seconds"],
            "speedup_factor": r1["speedup_factor"],
            "notes": f"Compression ratio {r1['compression_ratio']}x",
        },
        {
            "benchmark_name": "Eager vs Lazy Polars",
            "description": f"Filter & group-by {r2['sample_size']} rows",
            "baseline_seconds": r2["eager_seconds"],
            "optimized_seconds": r2["lazy_seconds"],
            "speedup_factor": r2["speedup_factor"],
            "notes": "Predicate & projection pushdown",
        },
        {
            "benchmark_name": "Partition Pushdown Scan",
            "description": (
                f"Scan 1 of 6 partitions ({r3['target_partition_rows']}/{r3['total_rows']} rows)"
            ),
            "baseline_seconds": r3["unpartitioned_scan_seconds"],
            "optimized_seconds": r3["partitioned_scan_seconds"],
            "speedup_factor": r3["speedup_factor"],
            "notes": "Partition directory pruning",
        },
    ]

    with csv_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nBenchmarks written to {csv_file}")


if __name__ == "__main__":
    main()
