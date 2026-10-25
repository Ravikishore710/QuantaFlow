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
  ├── Physics bounds: trip_distance <= 500 mi, fare_amount <= $10,000, duration <= 24 hr
  ├── Non-conformant rows routed to Quarantine DLQ (data/quarantine/*.jsonl)
  ├── Deduplication on compound business keys
  └── Multi-source enrichment: Taxi Trips + Zone Metadata + Hourly Weather
       │
       ▼
[Write-Audit-Publish (WAP) Circuit Breaker: data/reports/]
  ├── Automated evaluation of 6 dimensions: Completeness, Validity, Uniqueness,
  │   Consistency, Freshness, Volume (Saved as JSON audit reports)
  └── WAP Gate: Halts publish to Gold/Warehouse if quality score < 85% or error checks fail;
      dispatches webhooks to ALERT_WEBHOOK_URL
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
  ├── Production Target: PostgreSQL 16 (High-throughput COPY staging upsert)
  ├── Developer/Offline Engine: Embedded DuckDB (data/quantaflow.duckdb)
  └── Monitoring Tables: monitoring.pipeline_runs, monitoring.quality_results
```

---

## 2. Storage & Warehouse Architecture

To enable reliable continuous integration and local developer velocity alongside containerized production deployments, QuantaFlow implements a strict dual-target architecture:

| Component | Production Target | Developer / Offline Target |
| :--- | :--- | :--- |
| **Warehouse** | **PostgreSQL 16** (`warehouse.*` schema, high-throughput `COPY` staging upserts) | **DuckDB** (`data/quantaflow.duckdb`, embedded zero-ops) |
| **Object Store** | **MinIO / AWS S3** (`s3://quanta-flow/raw/...`) | **Local Filesystem** (`data/raw/` or `data/samples/`) |
| **Manifest** | PostgreSQL `monitoring.file_manifest` | JSON File Store (`data/manifests/`) |
| **Orchestration** | **Apache Airflow** (LocalExecutor via Docker Compose) | **Python CLI Runner** (`scripts/run_pipeline.py`) |

> **Architecture Boundary Note:**
> PostgreSQL is the formal production warehouse target. When running locally without Docker or PostgreSQL services active, `WarehouseLoader` operates in `engine_mode="auto"` or `"duckdb"`, executing the exact same analytical DDL and upsert semantics inside the local DuckDB database.

---

## 3. Data Cleaning & Business Rules

All transformations enforce clear mathematical definitions across contracts, transformation code, and quality checks:
- **Non-negative trip distance:** $\mathtt{trip\_distance} \ge 0.0$ (and physics upper-bound $\le 500.0\text{ mi}$)
- **Non-negative fare amount:** $\mathtt{fare\_amount} \ge 0.0$ (and physics upper-bound $\le \$10,000.0$)
- **Non-negative total amount:** $\mathtt{total\_amount} \ge 0.0$
- **Chronological ordering:** $\mathtt{tpep\_dropoff\_datetime} \ge \mathtt{tpep\_pickup\_datetime}$ (and trip duration $\le 1440.0\text{ min} / 24\text{ hr}$)
- **Consistency:** $\mathtt{total\_amount} \ge \mathtt{fare\_amount}$
- **Quarantine Policy & Replay CLI:** Records violating any constraint are routed to the **Quarantine Dead-Letter Queue** (`data/quarantine/*.jsonl`) with explicit violation reason metadata. Operators can re-validate and replay quarantined records using `python scripts/replay_quarantine.py`.
- **Write-Audit-Publish (WAP) Circuit Breaker:** Automated quality gate halts publication to Gold and Warehouse if error-level checks fail or pass rate falls below 85%, alerting external webhooks via `ALERT_WEBHOOK_URL`.

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
pip install -e ".[dev,airflow]"

# 3. Run automated tests (64 tests across unit, contracts, integration, e2e)
pytest tests/unit tests/contracts tests/integration tests/e2e --cov=src --cov-fail-under=85

# 4. Run linter and typechecker
ruff check src scripts tests
mypy src scripts

# 5. Run single month pipeline (e.g. 2024-01)
python scripts/run_pipeline.py --period 2024-01 --skip-upload

# 6. Replay quarantined records (Dead-Letter Queue audit & recovery)
python scripts/replay_quarantine.py --period 2024-01 --dry-run
python scripts/replay_quarantine.py --period 2024-01

# 7. Run 6-month historical backfill (2024-01 to 2024-06)
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

The test suite contains **64 automated tests** passing with 100% green status and **$\ge 88\%$ test coverage** (`--cov-fail-under=85`):
- `tests/unit/test_db_manager.py`: Centralized PostgreSQL and DuckDB connection handling, auto-fallback.
- `tests/unit/test_silver.py`: Physics bounds ($\le 500\text{ mi}$, $\le \$10,000$), lower bounds, quarantine routing.
- `tests/unit/test_bronze.py`: Lineage audit injection and schema typing.
- `tests/unit/test_gold.py`: Exact column mappings and dimensional aggregations.
- `tests/unit/test_quality_framework.py`: Completeness, validity, freshness, volume checks, WAP gate circuit breaker, webhook alerting.
- `tests/unit/test_warehouse.py`: DuckDB table loads, PostgreSQL COPY staging upsert, fallback handling.
- `tests/unit/test_replay_quarantine.py`: Quarantine DLQ inspection, dry run, and live silver recovery.
- `tests/unit/test_airflow_dag.py`: DAG bytecode compilation, AST structure, task callable execution.
- `tests/contracts/`: YAML schema parsing, required column checks, evolution policies.
- `tests/e2e/test_milestone1_idempotency.py`: Re-run skipping via cryptographic file manifest matching.
- `tests/integration/test_end_to_end_pipeline.py`: Full raw $\rightarrow$ bronze $\rightarrow$ silver $\rightarrow$ gold integration flow.

---

## Governance & Contributing

- [CHANGELOG.md](CHANGELOG.md): Complete release history and version upgrade log.
- [CONTRIBUTING.md](CONTRIBUTING.md): Developer onboarding, coding standards, and PR requirements.
- [SECURITY.md](SECURITY.md): Vulnerability reporting policy and security architecture overview.

---

## License

MIT License. See [LICENSE](LICENSE) for details.
