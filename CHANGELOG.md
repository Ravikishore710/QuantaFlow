# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-10-26

### Added
- **Centralized Database Connection Manager (`src/common/db.py`):** Consolidates PostgreSQL and DuckDB connection handling and auto-fallback logic across `WarehouseLoader`, `FileManifest`, and `PipelineMonitor`.
- **Upper-Bound Physics Outlier Validation (`src/transformation/silver.py`):** Enforces sensory upper-bound physics checks ($\le 500.0\text{ mi}$, $\le \$10,000.0$, $\le 24\text{ hr}$) and routes outliers to Dead-Letter Queue (DLQ) with violation metadata.
- **Write-Audit-Publish (WAP) Circuit Breaker (`src/validation/quality_framework.py`):** Halts publishing to Gold and Warehouse if error-level checks fail or pass rate drops below 85%; triggers outbound HTTP POST alerts via `ALERT_WEBHOOK_URL`.
- **High-Throughput PostgreSQL COPY Staging Upsert (`src/warehouse/loader.py`):** Implements set-based COPY staging via temporary tables and `copy_expert`, eliminating row-by-row iteration bottlenecks.
- **Quarantine DLQ Replay CLI (`scripts/replay_quarantine.py`):** Provides a CLI utility with `--dry-run` and period filtering to inspect and recover valid records back into Silver.
- **DevSecOps Hardening:** Added `pip-audit` automated vulnerability scanning and pip caching in GitHub Actions CI, non-root user execution (`quantaflow`, UID 10001) in `Dockerfile`, `.dockerignore`, `Makefile`, and `.pre-commit-config.yaml`.
- **Governance Documentation:** Added `CHANGELOG.md`, `CONTRIBUTING.md`, and `SECURITY.md`.

### Changed
- Elevated test coverage floor from 80% to 85% (`88.43%` achieved across 64 automated tests).
- Fixed KaTeX/LaTeX math syntax in `README.md` by properly escaping formula underscores.
- Narrowed database exception handling to typed domain errors (`WarehouseConnectionError`, `ManifestPersistenceError`, `DataQualityGateError`).

---

## [1.0.0] - 2026-10-25

### Added
- Complete end-to-end Medallion Architecture (Bronze, Silver, Gold).
- Multi-source ingestion: NYC TLC Yellow Taxi, Taxi Zone Lookup, and NOAA GHCNh Weather.
- Data contracts with fail-closed YAML schema validation.
- Cryptographic file manifest with SHA-256 deduplication and pipeline idempotency.
- Apache Airflow orchestration DAG (`quanta_flow_monthly_pipeline`).
- Dual-target warehouse architecture supporting PostgreSQL 16 and embedded DuckDB.
- Structured JSON logging with `structlog`.
