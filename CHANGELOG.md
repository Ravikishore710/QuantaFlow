# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2026-10-26

### Added
- **Modular Terraform Infrastructure-as-Code (`terraform/`):** Enterprise-grade AWS deployment with modules for S3 Lakehouse (AES-256 encryption, versioning, lifecycle transitions), RDS PostgreSQL 16 (storage encryption, Secrets Manager automated credentials), and ECS Fargate cluster (KMS-encrypted CloudWatch logs, container insights).
- **Offline Integration Testing & Ephemeral Test Stack:** Local offline fixtures (`tests/fixtures/`) and isolated ephemeral Docker Compose environment (`docker-compose.test.yml`) allowing full E2E pipeline testing with zero live external network calls.
- **Container Health Check Subsystem (`src/monitoring/health.py`, `scripts/health_check.py`):** Multi-probe health checker serving `/health`, `/healthz`, and `/readyz` over HTTP and CLI probe supporting engine status, storage readiness, and disk space monitoring.
- **Observability & Error Tracking:** Integrated Sentry error tracking (`src/common/error_tracking.py`) and correlation ID tracing across all `structlog` events and run context metadata.
- **Secret Management Layer (`src/common/secrets.py`):** Multi-backend secret resolution supporting mounted files (`/run/secrets/`), AWS Secrets Manager, and environment variables, with automated logging redaction.
- **Strict Ingress Schema Gates (`src/validation/ingress.py`):** Fail-closed Pydantic v2 validation protecting pipeline execution, backfills, quarantine replay, and Airflow DAGs.
- **Fresh-Clone Verification Script (`scripts/verify_fresh_clone.sh`):** Strict bash script validating environment installation, linting, typing, test suite, and health checks on fresh clones.
- **Hardened CI/CD Pipeline:** 4 structured GitHub Actions jobs (`quality-and-tests`, `fresh-clone-check`, `iac-validation`, `deploy`) enforcing `uv lock --check`, Terraform validation, and Docker container deployment probes.

### Changed
- Elevated test coverage floor from 85% to 90% (`92.72%` achieved across 108 automated tests).
- Exported pinned `requirements.txt` and refreshed `uv.lock` with explicit version bounds across all runtime dependencies.

---

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
