# QuantaFlow

[![CI](https://github.com/quantaflow/quantaflow/actions/workflows/ci.yml/badge.svg)](https://github.com/quantaflow/quantaflow/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

**QuantaFlow** is a production-grade incremental data engineering pipeline designed to ingest, validate, clean, enrich, aggregate, and monitor multi-source urban mobility and meteorological data.

---

## 1. Architecture Overview

QuantaFlow processes raw data through an idempotent, contract-enforced **Medallion Architecture**:

```
[Raw Sources]
  ├── NYC TLC Yellow Taxi (Monthly Parquet)
  ├── Taxi Zone Lookup (CSV)
  └── NOAA GHCNh Weather (Public S3 Parquet, Central Park USW00094728)
       │
       ▼
[Data Contracts & Manifest]
  ├── Strict YAML Schemas & Fail-Closed Validation
  └── SHA-256 Checksums & PostgreSQL/JSON File Manifest (Idempotent Deduplication)
       │
       ▼
[Bronze Layer: data/bronze/]
  └── Raw records partitioned with lineage metadata (_ingested_at, _source_file, _period)
       │
       ▼
[Silver Layer: data/silver/]
  ├── Cleaning rules: non-negative fare, non-negative distance, chronological timestamps
  ├── Non-conformant rows routed to Quarantine DLQ (data/quarantine/*.jsonl)
  ├── Deduplication on compound business keys
  └── Multi-source enrichment: Taxi Trips + Zone Metadata + Hourly Weather
       │
       ▼
[Data Quality Framework: data/reports/]
  └── Automated evaluation of 6 dimensions: Completeness, Validity, Uniqueness,
      Consistency, Freshness, Volume (Saved as JSON audit reports)
       │
       ▼
[Gold Layer: data/gold/]
  ├── hourly_trip_metrics (volume, total/average fare, duration, distance)
  ├── zone_daily_metrics (daily volume and revenue by pickup zone and borough)
  ├── weather_impact_metrics (precipitation/temperature impact on ride volume and fares)
  └── daily_system_metrics (throughput, latency, valid vs quarantined counts)
       │
       ▼
[Warehouse & Monitoring]
  ├── Production Target: PostgreSQL 16 (Idempotent ON CONFLICT DO UPDATE)
  ├── Developer/Offline Engine: Embedded DuckDB (data/quantaflow.duckdb)
  └── Monitoring Tables: monitoring.pipeline_runs, monitoring.quality_results
```

---

## 2. Storage & Warehouse Architecture

To enable reliable continuous integration and local developer velocity alongside containerized production deployments, QuantaFlow implements a strict dual-target architecture:

| Component | Production Target | Developer / Offline Target |
| :--- | :--- | :--- |
| **Warehouse** | **PostgreSQL 16** (`warehouse.*` schema, idempotent upserts) | **DuckDB** (`data/quantaflow.duckdb`, embedded zero-ops) |
| **Object Store** | **MinIO / AWS S3** (`s3://quanta-flow/raw/...`) | **Local Filesystem** (`data/raw/` or `data/samples/`) |
| **Manifest** | PostgreSQL `monitoring.file_manifest` | JSON File Store (`data/manifests/`) |
| **Orchestration** | **Apache Airflow** (LocalExecutor via Docker Compose) | **Python CLI Runner** (`scripts/run_pipeline.py`) |

> **Architecture Boundary Note:**
> PostgreSQL is the formal production warehouse target. When running locally without Docker or PostgreSQL services active, `WarehouseLoader` operates in `engine_mode="auto"` or `"duckdb"`, executing the exact same analytical DDL and upsert semantics inside the local DuckDB database.

---

## 3. Data Cleaning & Business Rules

All transformations enforce clear mathematical definitions across contracts, transformation code, and quality checks:
- **Non-negative trip distance:** $\text{trip\_distance} \ge 0.0$
- **Non-negative fare amount:** $\text{fare\_amount} \ge 0.0$
- **Non-negative total amount:** $\text{total\_amount} \ge 0.0$
- **Chronological ordering:** $\text{tpep\_dropoff\_datetime} \ge \text{tpep\_pickup\_datetime}$
- **Consistency:** $\text{total\_amount} \ge \text{fare\_amount}$
- **Quarantine Policy:** Records violating any of these constraints are not deleted; they are preserved in the **Quarantine Dead-Letter Queue** (`data/quarantine/`) with audit timestamps and violation metadata.

---

## 4. Measured Performance Benchmarks

Benchmarks were measured using `evaluation/benchmarks/run_benchmarks.py` and saved to `evaluation/results/pipeline_benchmarks.csv`:

| Benchmark | Access Pattern | Baseline | Optimized | Speedup / Efficiency | Technical Rationale |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Parquet vs CSV** | Scan & aggregate 100,000 rows | 0.0135s | 0.0127s | **1.06× speedup** | **29.85× storage compression** via Snappy column encoding |
| **Polars Lazy vs Eager** | Filter & group-by 150,000 rows | 0.0172s | 0.0055s | **3.14× speedup** | Predicate and projection pushdown directly in query planner |
| **Partition Pruning** | Scan 1 of 6 partitions (50k/300k rows) | 0.0107s | 0.0085s | **1.25× speedup** | Prunes unneeded partition directories on disk before scanning |

---

## 5. Multi-Month Execution & Verification

The pipeline has been verified on both single-month live production data and a multi-month backfill:
- **Live Ingestion (2024-01):**
  - NYC TLC Yellow Taxi: **2,964,624 raw records** (49.9 MB)
  - Cleaned & valid: **2,927,000 records**
  - Quarantined: **37,624 records**
  - Taxi Zones: **265 rows**
  - NOAA Weather: **1,076 hourly observations**
  - Warehouse Marts Loaded: 749 hourly rows, 6,965 zone-daily rows, 749 weather-impact rows.
- **6-Month Historical Backfill (2024-01 through 2024-06):**
  - Incremental execution verified for all 6 months via `scripts/run_backfill.py`.
  - Accumulated Warehouse Records: **4,083 hourly metric rows**, **43,782 zone daily rows**.
  - All 6 pipeline runs and 36 data quality evaluations recorded in `monitoring.pipeline_runs` and `monitoring.quality_results`.

---

## 6. Quickstart & Usage

### Local Developer Setup (Windows / Mac / Linux)

```bash
# 1. Create and activate virtual environment
python -m venv .venv
# On Windows:
.\.venv\Scripts\Activate.ps1
# On Linux/macOS:
source .venv/bin/activate

# 2. Install dependencies
pip install -e ".[dev]"

# 3. Run automated tests (28 tests across unit, contract, warehouse, DAG)
pytest -v

# 4. Run linter
ruff check src scripts evaluation tests

# 5. Run single month pipeline (e.g. 2024-01)
python scripts/run_pipeline.py --period 2024-01 --skip-upload

# 6. Run 6-month historical backfill (2024-01 to 2024-06)
python scripts/run_backfill.py --start 2024-01 --end 2024-06 --use-synthetic
```

### Production Docker Deployment

```bash
# Spin up PostgreSQL 16, MinIO, and Apache Airflow
docker compose up -d

# Open Airflow UI at http://localhost:8080 (credentials: airflow / airflow)
# Trigger DAG: quanta_flow_monthly_pipeline
```

---

## 7. Interactive Warehouse Verification

Query the local DuckDB warehouse directly using Python:

```python
import duckdb

con = duckdb.connect("data/quantaflow.duckdb")

# Inspect daily system operational throughput
print(con.execute("SELECT * FROM warehouse.daily_system_metrics").df())

# View top revenue taxi pickup zones
print(
    con.execute("""
    SELECT pickup_borough, pickup_zone, SUM(total_fare_revenue) AS revenue
    FROM warehouse.zone_daily_metrics
    GROUP BY pickup_borough, pickup_zone
    ORDER BY revenue DESC LIMIT 5
""").df()
)

# Check pipeline monitoring execution log
print(
    con.execute("""
    SELECT data_interval_start, status, rows_in, rows_out, rows_rejected
    FROM monitoring.pipeline_runs
""").df()
)
```

---

## 8. Test Suite Summary

The test suite contains **28 automated tests** passing with 100% green status:
- `tests/contracts/`: YAML schema parsing, required column checks, evolution policies.
- `tests/unit/test_bronze.py`: Lineage audit injection and schema typing.
- `tests/unit/test_silver.py`: Non-negative rule filters, quarantine routing, deduplication.
- `tests/unit/test_gold.py`: Exact column mappings and dimensional aggregations.
- `tests/unit/test_quality_framework.py`: Completeness, validity, freshness, volume checks.
- `tests/unit/test_warehouse.py`: DuckDB table loads, PostgreSQL upsert query generation, connection error handling.
- `tests/unit/test_airflow_dag.py`: DAG bytecode compilation, AST structure, task callable execution.
- `tests/e2e/test_milestone1_idempotency.py`: Re-run skipping via cryptographic file manifest matching.
- `tests/integration/test_end_to_end_pipeline.py`: Full raw $\rightarrow$ bronze $\rightarrow$ silver $\rightarrow$ gold integration flow.

---

## License

MIT License. See [LICENSE](LICENSE) for details.
